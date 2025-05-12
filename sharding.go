package sharding

import (
	"errors"
	"fmt"
	"hash/crc32"
	"log"
	"math"
	"math/big"
	"reflect"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/bwmarrin/snowflake"
	pg_query "github.com/pganalyze/pg_query_go/v6"
	"gorm.io/gorm"
)

// PartitionType defines the type of partitioning strategy
type PartitionType string

const (
	// PartitionTypeHash represents hash-based partitioning
	PartitionTypeHash PartitionType = "hash"
	// PartitionTypeList represents list-based partitioning
	PartitionTypeList PartitionType = "list"
)

var (
	ErrMissingShardingKey = errors.New("sharding key or id required, and use operator =")
	ErrInvalidID          = errors.New("invalid id format")
	ErrInsertDiffSuffix   = errors.New("can not insert different suffix table in one query ")
)

var (
	ShardingIgnoreStoreKey = "sharding_ignore"
)

type Sharding struct {
	*gorm.DB
	ConnPool       *ConnPool
	configs        map[string]Config
	querys         sync.Map
	snowflakeNodes []*snowflake.Node
	globalIndices  *GlobalIndexRegistry
	queryRewriter  *QueryRewriter
	txRegistry     *TransactionRegistry     // Add transaction registry
	healthChecker  *ConnectionHealthChecker // Add health checker

	_config Config
	_tables []any

	mutex sync.RWMutex
}

// Config specifies the configuration for sharding.
type Config struct {
	// When DoubleWrite enabled, data will double write to both main table and sharding table.
	DoubleWrite bool

	// ShardingKey specifies the table column you want to used for sharding the table rows.
	// For example, for a product order table, you may want to split the rows by `user_id`.
	ShardingKey string

	// PartitionType specifies which partitioning strategy to use
	PartitionType PartitionType

	// NumberOfShards specifies how many tables you want to sharding.
	NumberOfShards uint

	// tableFormat specifies the sharding table suffix format.
	tableFormat string

	// ShardingAlgorithm specifies a function to generate the sharding
	// table's suffix by the column value.
	// For example, this function implements a mod sharding algorithm.
	//
	// 	func(value any) (suffix string, err error) {
	//		if uid, ok := value.(int64);ok {
	//			return fmt.Sprintf("_%02d", user_id % 64), nil
	//		}
	//		return "", errors.New("invalid user_id")
	// 	}
	ShardingAlgorithm func(columnValue any) (suffix string, err error)

	// ShardingSuffixs specifies a function to generate all table's suffix.
	// Used to support Migrator and generate PrimaryKey.
	// For example, this function get a mod all sharding suffixs.
	//
	// func () (suffixs []string) {
	// 	numberOfShards := 5
	// 	for i := 0; i < numberOfShards; i++ {
	// 		suffixs = append(suffixs, fmt.Sprintf("_%02d", i%numberOfShards))
	// 	}
	// 	return
	// }
	ShardingSuffixs func() (suffixs []string)

	// ShardingAlgorithmByPrimaryKey specifies a function to generate the sharding
	// table's suffix by the primary key. Used when no sharding key specified.
	// For example, this function use the Snowflake library to generate the suffix.
	//
	// 	func(id int64) (suffix string) {
	//		return fmt.Sprintf("_%02d", snowflake.ParseInt64(id).Node())
	//	}
	ShardingAlgorithmByPrimaryKey func(id int64) (suffix string)

	// PrimaryKeyGenerator specifies the primary key generate algorithm.
	// Used only when insert and the record does not contains an id field.
	// Options are PKSnowflake, PKPGSequence and PKCustom.
	// When use PKCustom, you should also specify PrimaryKeyGeneratorFn.
	PrimaryKeyGenerator int

	// PrimaryKeyGeneratorFn specifies a function to generate the primary key.
	// When use auto-increment like generator, the tableIdx argument could ignored.
	// For example, this function use the Snowflake library to generate the primary key.
	// If you don't want to auto-fill the `id` or use a primary key that isn't called `id`, just return 0.
	//
	// 	func(tableIdx int64) int64 {
	//		return nodes[tableIdx].Generate().Int64()
	//	}
	PrimaryKeyGeneratorFn func(tableIdx int64) int64

	// ValueConverter converts values before they are used in SQL queries
	// This is especially useful for handling custom types like UInt256
	ValueConverter func(value interface{}) (interface{}, error)

	// ListValues maps category values to partition numbers (for list partitioning)
	// For example: {"ERC20": 0, "ERC721": 1, "ERC1155": 2}
	ListValues map[string]int

	// DefaultPartition specifies which partition to use for values not in ListValues
	// Set to -1 to throw an error when a value doesn't match
	DefaultPartition int

	engine DatabaseEngine
}

func Register(config interface{}, tables ...interface{}) *Sharding {
	// Load configuration from file
	if err := LoadConfigFromFile(""); err != nil {
		GetLogger().Info("Failed to load configuration: %v", err)
	}

	s := &Sharding{
		_tables: tables,
	}
	switch c := config.(type) {
	case Config:
		s._config = c
	case map[string]Config:
		s.configs = c
	default:
		panic("Invalid config type")
	}

	// Create an empty GlobalIndexRegistry
	s.globalIndices = &GlobalIndexRegistry{
		indices: make(map[string]map[string]*GlobalIndex),
	}

	return s
}

func (s *Sharding) compile() error {
	if s.configs == nil {
		s.configs = make(map[string]Config)
	}

	// Process all tables and ensure they have a config
	for _, table := range s._tables {
		var tableName string
		if t, ok := table.(string); ok {
			tableName = t
		} else {
			stmt := &gorm.Statement{DB: s.DB}
			if err := stmt.Parse(table); err != nil {
				return err
			}
			tableName = stmt.Table
		}

		// Only set the default config if a specific config is not already set
		if _, exists := s.configs[tableName]; !exists {
			s.configs[tableName] = s._config
		}
	}

	// Process configuration for each table
	for t, c := range s.configs {
		// Set the default partition type if not specified
		if c.PartitionType == "" {
			c.PartitionType = PartitionTypeHash
		}

		// Validate NumberOfShards for Snowflake
		if c.NumberOfShards > 1024 && c.PrimaryKeyGenerator == PKSnowflake {
			return errors.New("Snowflake NumberOfShards should be less than 1024")
		}

		// Set up PrimaryKeyGeneratorFn based on PrimaryKeyGenerator
		switch c.PrimaryKeyGenerator {
		case PKSnowflake:
			c.PrimaryKeyGeneratorFn = s.genSnowflakeKey
		case PKPGSequence:
			// Execute SQL to CREATE SEQUENCE for this table if not exist
			if err := s.createPostgreSQLSequenceKeyIfNotExist(t); err != nil {
				return err
			}
			c.PrimaryKeyGeneratorFn = func(index int64) int64 {
				return s.genPostgreSQLSequenceKey(t, index)
			}
		case PKMySQLSequence:
			if err := s.createMySQLSequenceKeyIfNotExist(t); err != nil {
				return err
			}
			c.PrimaryKeyGeneratorFn = func(index int64) int64 {
				return s.genMySQLSequenceKey(t, index)
			}
		case PKCustom:
			if c.PrimaryKeyGeneratorFn == nil {
				return errors.New("PrimaryKeyGeneratorFn is required when using PKCustom")
			}
		default:
			return errors.New("PrimaryKeyGenerator must be one of PKSnowflake, PKPGSequence, PKMySQLSequence, or PKCustom")
		}

		// Set up table format based on NumberOfShards
		if c.tableFormat == "" {
			switch {
			case c.NumberOfShards < 10:
				c.tableFormat = "_%01d"
			case c.NumberOfShards < 100:
				c.tableFormat = "_%02d"
			case c.NumberOfShards < 1000:
				c.tableFormat = "_%03d"
			case c.NumberOfShards < 10000:
				c.tableFormat = "_%04d"
			default:
				return errors.New("NumberOfShards exceeds maximum allowed shards")
			}
		}

		// Set up ShardingAlgorithm if not provided, based on partition type
		if c.ShardingAlgorithm == nil {
			switch c.PartitionType {
			case PartitionTypeHash:
				c.ShardingAlgorithm = defaultHashAlgorithm(&c)
			case PartitionTypeList:
				if len(c.ListValues) == 0 {
					return errors.New("ListValues must be provided for list partitioning")
				}
				c.ShardingAlgorithm = defaultListAlgorithm(&c)
			default:
				return fmt.Errorf("unsupported partition type: %s", c.PartitionType)
			}
		}

		// Set up ShardingSuffixs if not provided
		if c.ShardingSuffixs == nil {
			switch c.PartitionType {
			case PartitionTypeHash:
				c.ShardingSuffixs = func() []string {
					var suffixes []string
					for i := 0; i < int(c.NumberOfShards); i++ {
						suffix, err := c.ShardingAlgorithm(i)
						if err != nil {
							return nil
						}
						suffixes = append(suffixes, suffix)
					}
					return suffixes
				}
			case PartitionTypeList:
				c.ShardingSuffixs = defaultListSuffixes(&c)
			}
		}

		// Set up ShardingAlgorithmByPrimaryKey if not provided
		if c.ShardingAlgorithmByPrimaryKey == nil {
			switch c.PrimaryKeyGenerator {
			case PKSnowflake:
				c.ShardingAlgorithmByPrimaryKey = func(id int64) string {
					return fmt.Sprintf(c.tableFormat, snowflake.ParseInt64(id).Node())
				}
			case PKPGSequence, PKMySQLSequence:
				c.ShardingAlgorithmByPrimaryKey = func(id int64) string {
					return fmt.Sprintf(c.tableFormat, id%int64(c.NumberOfShards))
				}
			}
		}

		// Assign the updated config back to the map
		s.configs[t] = c
	}

	return nil
}

// Name plugin name for Gorm plugin interface
func (s *Sharding) Name() string {
	return "gorm:sharding"
}

// LastQuery get last SQL query
func (s *Sharding) LastQuery() string {
	if query, ok := s.querys.Load("last_query"); ok {
		return query.(string)
	}

	return ""
}

// Initialize implement for Gorm plugin interface
func (s *Sharding) Initialize(db *gorm.DB) error {
	db.Dialector = NewShardingDialector(db.Dialector, s)
	s.DB = db
	s.setDatabaseEngine()
	s.registerCallbacks(db)

	// Initialize transaction registry
	s.txRegistry = NewTransactionRegistry()

	// Initialize and start health checker if enabled
	if GetConfig().Connection.EnableAutoCleanup {
		interval := time.Duration(GetConfig().Connection.HealthCheckInterval) * time.Second
		s.healthChecker = NewConnectionHealthChecker(s, interval)
		s.healthChecker.Start()
	}

	// Extract the underlying *sql.DB for connection stats logging
	if sqlDB, err := db.DB(); err == nil {
		// Configure logger with database connection stats
		SetLogger(TryWithDBStats(GetLogger(), sqlDB))

		// Configure database timeouts
		if err := s.ConfigureDatabaseTimeouts(sqlDB); err != nil {
			// Log the error but don't fail initialization
			errorLog("Warning: Failed to configure database timeouts: %v", err)
		}
	}

	for t, c := range s.configs {
		if c.PrimaryKeyGenerator == PKPGSequence {
			err := s.DB.Exec("CREATE SEQUENCE IF NOT EXISTS " + pgSeqName(t)).Error
			if err != nil {
				return fmt.Errorf("init postgresql sequence error, %w", err)
			}
		}
		if c.PrimaryKeyGenerator == PKMySQLSequence {
			err := s.DB.Exec("CREATE TABLE IF NOT EXISTS " + mySQLSeqName(t) + " (id INT NOT NULL)").Error
			if err != nil {
				return fmt.Errorf("init mysql create sequence error, %w", err)
			}
			err = s.DB.Exec("INSERT INTO " + mySQLSeqName(t) + " VALUES (0)").Error
			if err != nil {
				return fmt.Errorf("init mysql insert sequence error, %w", err)
			}
		}
	}

	s.snowflakeNodes = make([]*snowflake.Node, 1024)
	for i := int64(0); i < 1024; i++ {
		n, err := snowflake.NewNode(i)
		if err != nil {
			return fmt.Errorf("init snowflake node error, %w", err)
		}
		s.snowflakeNodes[i] = n
	}

	err := s.compile()
	if err != nil {
		return err
	}

	// Initialize the query rewriter with default options
	// todo for indices
	//s.queryRewriter = NewQueryRewriter(s, DefaultQueryRewriteOptions())

	return nil
}

func (s *Sharding) registerCallbacks(db *gorm.DB) {
	s.Callback().Create().Before("*").Register("gorm:sharding", s.switchConn)
	s.Callback().Query().Before("*").Register("gorm:sharding", s.switchConn)
	s.Callback().Update().Before("*").Register("gorm:sharding", s.switchConn)
	s.Callback().Delete().Before("*").Register("gorm:sharding", s.switchConn)
	s.Callback().Row().Before("*").Register("gorm:sharding", s.switchConn)
	s.Callback().Raw().Before("*").Register("gorm:sharding", s.switchConn)

	// Add a new callback before gorm:create to handle multi-shard inserts
	db.Callback().Create().Before("gorm:create").Register("sharding:handle_multi_shard", func(db *gorm.DB) {
		if db.Error == nil {
			err := s.handleMultiShardInsert(db)
			if err != nil {
				db.Error = err
			}
		}
	})
}

func (s *Sharding) switchConn(db *gorm.DB) {
	// Support ignore sharding in some case, like:
	// When DoubleWrite is enabled, we need to query database schema
	// information by table name during the migration.
	if _, ok := db.Get(ShardingIgnoreStoreKey); !ok {
		if isSystemQuery(db.Statement.SQL.String()) {
			return
		}

		//stmt := db.Statement
		//if stmt.Schema != nil && stmt.Schema.Table != "" {
		//	s.mutex.RLock()
		//	_, tableRegistered := s.configs[stmt.Schema.Table]
		//	s.mutex.RUnlock()
		//
		//	// If table is not registered for sharding, skip applying sharding logic
		//	if !tableRegistered {
		//		return
		//	}
		//}

		// Don't hold the lock while creating the ConnPool
		var connPool gorm.ConnPool

		s.mutex.RLock()
		needGlobalIndex := s.globalIndices != nil && len(s.globalIndices.indices) > 0
		s.mutex.RUnlock()

		if db.Statement.ConnPool != nil {
			if needGlobalIndex {
				connPool = NewConnPoolWithGlobalIndex(db.Statement.ConnPool, s)
			} else {
				pool := &ConnPool{ConnPool: db.Statement.ConnPool, sharding: s}
				s.mutex.Lock()
				s.ConnPool = pool
				s.mutex.Unlock()
				connPool = pool
			}

			db.Statement.ConnPool = connPool
		}
	}
}

// resolve splits the old query into full table query and sharding table query
func (s *Sharding) resolve(query string, args ...interface{}) (ftQuery, stQuery, tableName string, err error) {
	// Initialize return values to avoid nil pointers
	ftQuery = query
	stQuery = query

	// Create a map with mutex to safely store tableMap (fixes the race condition)
	var tableMapMutex sync.Mutex
	tableMap := make(map[string]string) // originalTableName -> shardedTableName

	// Check if s.configs is nil or empty
	if s == nil || s.configs == nil {
		return query, query, tableName, nil
	}

	s.mutex.RLock()
	configsCount := len(s.configs)
	for baseTable, config := range s.configs {
		// Safely get sharding suffixes, handling nil cases
		var suffixes []string
		if config.ShardingSuffixs != nil {
			suffixes = config.ShardingSuffixs()
		}

		for _, suffix := range suffixes {
			// Check if query contains table with this suffix
			shardedTable := baseTable + suffix
			if strings.Contains(query, shardedTable) {
				// We found a direct query to a sharded table
				// Set the base table name and return the query as-is
				tableName = baseTable
				ftQuery = query
				stQuery = query
				s.mutex.RUnlock()
				return
			}
		}
	}
	s.mutex.RUnlock()

	// If configs is empty, return the query as-is
	if configsCount == 0 {
		return ftQuery, stQuery, tableName, nil
	}

	// Skip processing for system queries or explicit nosharding comments
	if isSystemQuery(query) || strings.Contains(query, "/*+ nosharding */") || strings.Contains(query, "/* nosharding */") {
		return ftQuery, stQuery, tableName, nil
	}

	// Parse the SQL query using pg_query_go
	parsed, err := pg_query.Parse(query)
	if err != nil {
		return ftQuery, stQuery, tableName, fmt.Errorf("error parsing query: %v", err)
	}

	if len(parsed.Stmts) == 0 {
		return ftQuery, stQuery, tableName, fmt.Errorf("no statements found in query")
	}

	// Assume single-statement queries
	stmt := parsed.Stmts[0]

	// Initialize variables
	var tables []string
	var isInsert, isSelect bool
	var insertStmt *pg_query.InsertStmt
	var selectStmt *pg_query.SelectStmt
	var conditions []*pg_query.Node

	// Process the parsed statement to extract tables and conditions
	switch stmtNode := stmt.Stmt.Node.(type) {
	case *pg_query.Node_SelectStmt:
		isSelect = true
		selectStmt = stmtNode.SelectStmt

		// Check if Op is SETOP_UNION. The 'All' field distinguishes UNION from UNION ALL.
		if selectStmt.Op == pg_query.SetOperation_SETOP_UNION {
			// This is a UNION query. We need to process each part to find all required shards.
			allSuffixes := make(map[string]bool)           // Collect unique suffixes determined by keys
			anyPartHasKeyForTable := make(map[string]bool) // Track if any part provided a key for a table

			// Helper function to process a select statement (part of the UNION)
			processUnionPart := func(partStmt *pg_query.SelectStmt) error {
				partTables := collectTablesFromSelect(partStmt)
				partConditions := collectConditionsFromSelect(partStmt) // Collect conditions including JOINs

				for _, tbl := range partTables {
					s.mutex.RLock()
					cfg, ok := s.configs[tbl]
					s.mutex.RUnlock()
					if !ok {
						continue // Skip non-sharded tables in this part
					}

					// Extract sharding key/ID for this part
					// Need alias map for this specific part if it uses aliases
					partAliasMap := make(map[string]string)
					for _, fromItem := range partStmt.FromClause {
						if joinExpr, ok := fromItem.Node.(*pg_query.Node_JoinExpr); ok {
							mergeMaps(partAliasMap, collectAliasesFromJoin(joinExpr.JoinExpr))
						} else if rangeVar, ok := fromItem.Node.(*pg_query.Node_RangeVar); ok {
							if rangeVar.RangeVar.Alias != nil {
								partAliasMap[rangeVar.RangeVar.Alias.Aliasname] = rangeVar.RangeVar.Relname
							} else {
								partAliasMap[rangeVar.RangeVar.Relname] = rangeVar.RangeVar.Relname // Map table to itself if no alias
							}
						}
					}

					value, id, keyFound, err := s.extractShardingKeyFromConditions(cfg.ShardingKey, partConditions, args, partAliasMap, tbl)
					if err != nil && !errors.Is(err, ErrMissingShardingKey) { // Ignore missing key error for now, handle later
						GetLogger().Info("Error extracting sharding key for table %s in UNION part: %v", tbl, err) // Changed Warn to Info
						// Decide how to handle errors - maybe skip this part or return error?
						// For now, let's try to continue if possible, but log it.
						// If DoubleWrite is enabled, we might proceed with base table.
						if !(cfg.DoubleWrite && errors.Is(err, ErrMissingShardingKey)) {
							return fmt.Errorf("error extracting sharding key for table %s in UNION part: %w", tbl, err)
						}
						// If DoubleWrite and missing key, immediately add all suffixes for this table
						if cfg.ShardingSuffixs != nil {
							GetLogger().Debug("DoubleWrite enabled for table %s and key missing in this part. Adding all suffixes.", tbl)
							for _, sfx := range cfg.ShardingSuffixs() {
								allSuffixes[sfx] = true
							}
						}
						continue // Continue to next table in this part
					}

					if keyFound || id != 0 {
						// Key was found, determine the specific suffix
						anyPartHasKeyForTable[tbl] = true // Mark that a key was found for this table
						suffix, suffixErr := getSuffix(value, id, keyFound, cfg)
						if suffixErr != nil {
							// Handle error getting suffix (e.g., invalid value for list partitioning)
							return fmt.Errorf("error determining suffix for table %s in UNION part: %w", tbl, suffixErr)
						}
						// Add only the specific suffix determined by the key
						allSuffixes[suffix] = true
					} else if cfg.DoubleWrite {
						// No key found, but DoubleWrite is enabled. Add all suffixes.
						GetLogger().Debug("DoubleWrite enabled for table %s in UNION part, no key found. Adding all suffixes.", tbl)
						if cfg.ShardingSuffixs != nil {
							for _, sfx := range cfg.ShardingSuffixs() {
								allSuffixes[sfx] = true
							}
						}
					} else {
						// No key found and DoubleWrite is not enabled - this is an error
						return fmt.Errorf("missing sharding key for table %s in UNION part and DoubleWrite not enabled: %w", tbl, ErrMissingShardingKey)
					}
				}
				return nil
			}

			// Recursively process UNION parts
			var processNode func(*pg_query.Node) error
			processNode = func(node *pg_query.Node) error {
				if node == nil {
					return nil
				}
				if selStmtNode, ok := node.Node.(*pg_query.Node_SelectStmt); ok {
					subSelectStmt := selStmtNode.SelectStmt
					if subSelectStmt.Op == pg_query.SetOperation_SETOP_UNION { // Check for UNION op
						// Nested UNION - Wrap Larg/Rarg back into Node for recursion
						if subSelectStmt.Larg != nil {
							largNode := &pg_query.Node{Node: &pg_query.Node_SelectStmt{SelectStmt: subSelectStmt.Larg}}
							if err := processNode(largNode); err != nil {
								return err
							}
						}
						if subSelectStmt.Rarg != nil {
							rargNode := &pg_query.Node{Node: &pg_query.Node_SelectStmt{SelectStmt: subSelectStmt.Rarg}}
							if err := processNode(rargNode); err != nil {
								return err
							}
						}
					} else {
						// Base SELECT statement
						if err := processUnionPart(subSelectStmt); err != nil {
							return err
						}
					}
					return nil
				}
				// Handle other node types if necessary, e.g., RangeSubselect
				return fmt.Errorf("unexpected node type in UNION structure: %T", node.Node)
			}

			// Start processing from the top-level UNION statement
			if err := processNode(stmt.Stmt); err != nil {
				return ftQuery, stQuery, tableName, fmt.Errorf("error processing UNION query: %w", err)
			}

			// Get all base table names involved in the original UNION
			unionTables := collectAllTablesFromUnion(selectStmt)
			if len(unionTables) > 0 {
				// Get the first table name encountered from the map
				for t := range unionTables {
					tableName = t
					break // Only need one for potential config lookup
				}
			}

			useBaseNameForTable := make(map[string]bool)
			allTablesNeedBaseName := true // Assume all need base name initially
			hasShardedTables := false
			for tbl := range unionTables {
				s.mutex.RLock()
				cfg, ok := s.configs[tbl]
				s.mutex.RUnlock()

				if ok { // Only consider sharded tables
					hasShardedTables = true
					if cfg.DoubleWrite && !anyPartHasKeyForTable[tbl] {
						// DoubleWrite is true for this table, AND no key was found in any part.
						// Mark this table to use its base name instead of sharded names.
						GetLogger().Debug("DoubleWrite enabled for table %s and no key found in any UNION part. Marking to use base table.", tbl)
						useBaseNameForTable[tbl] = true
						// Since we are using base name, we don't need specific suffixes for this table.
						// Remove any suffixes that might have been added by other parts (though unlikely with this logic).
					} else {
						// If key was found OR DoubleWrite is false, this table doesn't need base name
						allTablesNeedBaseName = false
					}
				} else {
					// Non-sharded table involved, so not all tables need base name
					allTablesNeedBaseName = false
				}
			}

			// If all sharded tables involved need the base name, return the original query
			if hasShardedTables && allTablesNeedBaseName {
				GetLogger().Debug("All sharded tables in UNION require base name due to DoubleWrite and missing keys. Returning original query.")
				return query, query, tableName, nil
			}

			// If no specific suffixes were determined (e.g., only non-sharded tables or error), return original
			if len(allSuffixes) == 0 {
				GetLogger().Debug("UNION query does not involve any determinable shards. Proceeding without rewrite.")
				return query, query, tableName, nil
			}

			// Create a UNION ALL query targeting each required shard.
			rewrittenQueries := []string{}
			originalUnionQuery := query // Keep the original structure

			// If we need to query specific shards (not just base tables)
			if len(allSuffixes) > 0 {
				for suffix := range allSuffixes {
					// Create a temporary map for this specific suffix/base name combination
					suffixTableMap := make(map[string]string)
					s.mutex.RLock()
					for baseTbl := range s.configs { // Iterate over configured tables
						// Only add tables that were actually present in the original UNION query
						if _, exists := unionTables[baseTbl]; exists {
							if useBaseNameForTable[baseTbl] {
								suffixTableMap[baseTbl] = baseTbl // Use base name
							} else {
								suffixTableMap[baseTbl] = baseTbl + suffix // Use sharded name
							}
						}
					}
					s.mutex.RUnlock()

					// Parse the original UNION query again to get a fresh AST
					parsedOriginal, parseErr := pg_query.Parse(originalUnionQuery)
					if parseErr != nil {
						return ftQuery, stQuery, tableName, fmt.Errorf("error re-parsing original UNION query: %v", parseErr)
					}
					if len(parsedOriginal.Stmts) == 0 {
						return ftQuery, stQuery, tableName, fmt.Errorf("no statements found in re-parsed UNION query")
					}
					unionStmtNode := parsedOriginal.Stmts[0]

					// Replace table names in this AST copy with the current suffix/base name
					replaceTableNames(unionStmtNode.Stmt, suffixTableMap)

					// Deparse this modified AST back to SQL
					deparsedSQL, deparseErr := pg_query.Deparse(&pg_query.ParseResult{Stmts: []*pg_query.RawStmt{unionStmtNode}})
					if deparseErr != nil {
						return ftQuery, stQuery, tableName, fmt.Errorf("error deparsing modified UNION query for suffix %s: %v", suffix, deparseErr)
					}
					// Wrap in parentheses for clarity in the final UNION ALL
					rewrittenQueries = append(rewrittenQueries, "("+deparsedSQL+")")
				}
				// Combine the rewritten queries with UNION ALL
				stQuery = strings.Join(rewrittenQueries, " UNION ALL ")
			} else {
				// This case should ideally not be reached if allTablesNeedBaseName was handled correctly
				// But as a fallback, return the original query if no suffixes were generated.
				stQuery = originalUnionQuery
				GetLogger().Debug("No suffixes generated for UNION rewrite, returning original query. This might indicate an issue.")

			}

			ftQuery = query // Keep original query for potential double write? Or should ftQuery also be rewritten? Let's keep original for now.

			GetLogger().Debug("Rewritten UNION query for shards %v: %s", mapsKeys(allSuffixes), stQuery)
			return ftQuery, stQuery, tableName, nil // Return the rewritten query
		}

		// Original SELECT logic (if not a UNION)
		tables = collectTablesFromSelect(selectStmt)
		if selectStmt.WhereClause != nil {
			conditions = append(conditions, selectStmt.WhereClause)
		}
		// Collect conditions from JOINs
		joinConditions := collectJoinConditions(selectStmt)
		conditions = append(conditions, joinConditions...)

		// If this is a SELECT without sharding key conditions
		if isSelect && len(conditions) > 0 {
			hasShardingKey := false
			for _, table := range tables {
				s.mutex.RLock()
				cfg, ok := s.configs[table]
				s.mutex.RUnlock()

				if ok {
					shardingKey := cfg.ShardingKey
					_, _, keyFound, _ := s.extractShardingKeyFromConditions(shardingKey, conditions, args, nil, table)
					if keyFound {
						hasShardingKey = true
						break
					}
				} else {
					return query, query, tableName, nil
				}
			}

			// If no sharding key found
			if !hasShardingKey {
				// Check if we're using LOWER function on a column that might be a sharding key
				for _, condition := range conditions {
					if containsLowerFunction(condition) {
						// If we're using LOWER, we'll try to extract the sharding key value
						// This is a special case for queries like "WHERE LOWER(name) = LOWER('value')"
						if tableName == "" && len(tables) > 0 {
							tableName = tables[0]
						}

						// Create a map to store table aliases
						var aliasMap map[string]string
						if isSelect {
							aliasMap = make(map[string]string)
							// Collect aliases from the FROM clause
							for _, fromItem := range selectStmt.FromClause {
								if joinExpr, ok := fromItem.Node.(*pg_query.Node_JoinExpr); ok {
									mergeMaps(aliasMap, collectAliasesFromJoin(joinExpr.JoinExpr))
								} else if rangeVar, ok := fromItem.Node.(*pg_query.Node_RangeVar); ok {
									if rangeVar.RangeVar.Alias != nil {
										aliasMap[rangeVar.RangeVar.Alias.Aliasname] = rangeVar.RangeVar.Relname
									}
								}
							}
						}

						// For tables with DoubleWrite enabled, try to determine the appropriate sharded table
						for _, table := range tables {
							s.mutex.RLock()
							cfg, ok := s.configs[table]
							s.mutex.RUnlock()
							if ok && cfg.DoubleWrite {
								// Create a map to store known keys for this extraction
								localKnownKeys := make(map[string]interface{})

								// Extract sharding key value from LOWER function
								for _, condition := range conditions {
									if containsLowerFunction(condition) {
										shardingKey := cfg.ShardingKey
										keyFound, value, err := extractShardingKeyFromLowerFunction(shardingKey, condition, args, localKnownKeys, aliasMap)
										if keyFound && err == nil && value != nil {
											// If we found a sharding key in a LOWER function, use it to determine the suffix
											suffix, err := getSuffix(value, 0, true, cfg)
											if err == nil {
												// Add the sharded table to the tableMap
												shardedTableName := table + suffix
												tableMapMutex.Lock()
												tableMap[table] = shardedTableName
												tableMapMutex.Unlock()
												GetLogger().Debug("Using sharded table %s for LOWER function on sharding key %s with value %v",
													shardedTableName, shardingKey, value)
											}
										}
									}
								}

								// If we've added entries to tableMap, we can proceed
								if len(tableMap) > 0 {
									return ftQuery, stQuery, tableName, nil
								}

								// Otherwise, allow the query to proceed with double write and add nosharding hint
								stQuery = fmt.Sprintf("/* nosharding */ %s", query)
								return ftQuery, stQuery, tableName, nil
							}
						}
						// If no table has DoubleWrite enabled, return the original error
						return ftQuery, stQuery, tableName, ErrMissingShardingKey
					}
				}

				if tableName == "" && len(tables) > 0 {
					tableName = tables[0] // Assign a table name if possible
				}

				// Check DoubleWrite before returning ErrMissingShardingKey for SELECT
				canUseBaseTable := false
				if tableName != "" { // Ensure we have a table name to check config
					s.mutex.RLock()
					cfg, ok := s.configs[tableName]
					s.mutex.RUnlock()
					if ok && cfg.DoubleWrite {
						canUseBaseTable = true
					}
				}

				if canUseBaseTable {
					// DoubleWrite enabled, signal to use the base table query (ftQuery)
					GetLogger().Debug("SELECT without sharding key for table '%s', DoubleWrite enabled. Using base table query.", tableName)
					// Return the original query (ftQuery) and nil error.
					// Both ftQuery and stQuery should be the original query in this case.
					return query, query, tableName, nil
				} else {
					// DoubleWrite is false or config not found, return the error
					GetLogger().Error("SELECT without sharding key for table '%s', DoubleWrite disabled or config missing. Returning error.", tableName)
					return ftQuery, stQuery, tableName, ErrMissingShardingKey
				}
			}
		}

	case *pg_query.Node_InsertStmt:
		isInsert = true
		insertStmt = stmtNode.InsertStmt
		if insertStmt.Relation != nil {
			// Get base table name
			baseTable := insertStmt.Relation.Relname
			tables = []string{baseTable}
			tableName = baseTable
		} else {
			return ftQuery, stQuery, tableName, fmt.Errorf("unexpected node type in InsertStmt.Relation")
		}
	case *pg_query.Node_UpdateStmt:
		updateStmt := stmtNode.UpdateStmt
		if updateStmt.Relation != nil {
			tables = []string{updateStmt.Relation.Relname}
			tableName = updateStmt.Relation.Relname
			if updateStmt.WhereClause != nil {
				conditions = append(conditions, updateStmt.WhereClause)
			}
		} else {
			return ftQuery, stQuery, tableName, fmt.Errorf("unexpected node type in UpdateStmt.Relation")
		}
	case *pg_query.Node_DeleteStmt:
		deleteStmt := stmtNode.DeleteStmt
		if deleteStmt.Relation != nil {
			tables = []string{deleteStmt.Relation.Relname}
			tableName = deleteStmt.Relation.Relname
			if deleteStmt.WhereClause != nil {
				conditions = append(conditions, deleteStmt.WhereClause)
			}
		} else {
			return ftQuery, stQuery, tableName, fmt.Errorf("unexpected node type in DeleteStmt.Relation")
		}
	// Handle DDL statements by returning the original query
	case *pg_query.Node_CreateStmt,
		*pg_query.Node_DropStmt,
		*pg_query.Node_AlterTableStmt,
		*pg_query.Node_TruncateStmt,
		*pg_query.Node_RenameStmt,
		*pg_query.Node_IndexStmt,
		*pg_query.Node_CommentStmt,
		*pg_query.Node_GrantStmt:
		// DDL statements. Bypass sharding.
		return query, query, tableName, nil
	case *pg_query.Node_ViewStmt:
		// Handle CREATE OR REPLACE VIEW statements
		viewStmt := stmtNode.ViewStmt

		// Extract the query part of the view definition
		if viewStmt.Query != nil {
			// Check if the query references any sharded tables
			if selectStmt, ok := viewStmt.Query.Node.(*pg_query.Node_SelectStmt); ok {
				// Collect tables from the SELECT statement
				tables := collectTablesFromSelect(selectStmt.SelectStmt)

				// Check if any of these tables are sharded
				for _, table := range tables {
					s.mutex.RLock()
					cfg, ok := s.configs[table]
					s.mutex.RUnlock()

					if ok {
						// This is a sharded table, extract sharding key from conditions
						if selectStmt.SelectStmt.WhereClause != nil {
							conditions := []*pg_query.Node{selectStmt.SelectStmt.WhereClause}

							// Extract sharding key value
							shardingKey := cfg.ShardingKey
							value, id, keyFound, _ := s.extractShardingKeyFromConditions(shardingKey, conditions, args, nil, table)

							if keyFound || id != 0 {
								// Determine the suffix
								suffix, err := getSuffix(value, id, keyFound, cfg)
								if err != nil {
									return ftQuery, stQuery, tableName, err
								}

								// Replace the table name in the query with the sharded table name
								shardedTable := table + suffix
								tableMap := map[string]string{table: shardedTable}

								// Replace table names in the query part
								replaceTableNames(viewStmt.Query, tableMap)

								// Deparse the modified AST back to SQL
								stmts := []*pg_query.RawStmt{stmt}
								stQuery, err = pg_query.Deparse(&pg_query.ParseResult{Stmts: stmts})
								if err != nil {
									return ftQuery, stQuery, tableName, fmt.Errorf("error deparsing modified view query: %v", err)
								}

								return ftQuery, stQuery, tableName, nil
							}
						}
					}
				}
			}
		}

		// If no sharding key found or no sharded tables, return the original query
		return query, query, tableName, nil
	default:
		return ftQuery, stQuery, tableName, fmt.Errorf("unsupported statement type")
	}

	// Iterate through each table to determine its sharded name
	for _, originalTableName := range tables {
		schemaName := ""
		localTableName := originalTableName

		// Check for schema-qualified table names
		if strings.Contains(originalTableName, ".") {
			parts := strings.SplitN(originalTableName, ".", 2)
			schemaName = parts[0]
			localTableName = parts[1]
		}

		fullTableName := localTableName
		if schemaName != "" {
			fullTableName = fmt.Sprintf("%s.%s", schemaName, localTableName)
		}

		s.mutex.RLock()
		r, ok := s.configs[fullTableName]
		s.mutex.RUnlock()

		if !ok {
			continue // Skip tables not configured for sharding
		}

		var suffix string
		if isInsert {
			// Handle insert statements
			var consistentSuffix string
			suffixes := make(map[string]bool)

			if insertStmt.SelectStmt != nil {
				nestedSelect, ok := insertStmt.SelectStmt.Node.(*pg_query.Node_SelectStmt)
				if !ok {
					return ftQuery, stQuery, tableName, fmt.Errorf("insert statement select is not a SelectStmt")
				}

				valuesSelect := nestedSelect.SelectStmt
				if len(valuesSelect.ValuesLists) == 0 {
					return ftQuery, stQuery, tableName, fmt.Errorf("insert statement has no VALUES list")
				}
				// Iterate through each values list to extract suffixes
				for _, valuesList := range valuesSelect.ValuesLists {
					value, id, keyFound, err := s.extractInsertShardingKeyFromValues(r, insertStmt, valuesList, args...)
					if err != nil {
						return ftQuery, stQuery, tableName, err
					}

					currentSuffix, err := getSuffix(value, id, keyFound, r)
					if err != nil {
						// Check if DoubleWrite is enabled for this table
						return ftQuery, stQuery, tableName, err
					}

					suffixes[currentSuffix] = true

					// If more than one unique suffix is found, return an error
					if len(suffixes) > 1 {
						// Return ErrInsertDiffSuffix to signal different sharding keys detected
						tableName = originalTableName
						return query, query, tableName, ErrInsertDiffSuffix
					}

					// Capture the consistent suffix
					consistentSuffix = currentSuffix
				}
			} else {
				if len(insertStmt.GetReturningList()) == 0 {
					return ftQuery, stQuery, tableName, fmt.Errorf("insert statement has no VALUES list")
				}

				// Iterate through each values list to extract suffixes
				for _, valuesList := range insertStmt.ReturningList {
					value, id, keyFound, err := s.extractInsertShardingKeyFromValues(r, insertStmt, valuesList, args...)
					if err != nil {
						return ftQuery, stQuery, tableName, err
					}

					currentSuffix, err := getSuffix(value, id, keyFound, r)
					if err != nil {
						// Check if DoubleWrite is enabled
						return ftQuery, stQuery, tableName, err
					}

					suffixes[currentSuffix] = true

					// If more than one unique suffix is found, return an error
					if len(suffixes) > 1 {
						// Return ErrInsertDiffSuffix to signal different sharding keys detected
						tableName = originalTableName
						return query, query, tableName, ErrInsertDiffSuffix
					}

					// Capture the consistent suffix
					consistentSuffix = currentSuffix
				}
			}

			// Ensure all suffixes are consistent
			if len(suffixes) == 1 {
				suffix = consistentSuffix
			} else {
				// Return ErrInsertDiffSuffix to signal different sharding keys detected
				tableName = originalTableName
				return query, query, tableName, ErrInsertDiffSuffix
			}

			shardedTableName := originalTableName + suffix

			// Thread-safely update the tableMap
			tableMapMutex.Lock()
			tableMap[originalTableName] = shardedTableName
			tableMapMutex.Unlock()

			// Update the table name in the insert statement
			if insertStmt.Relation != nil {
				insertStmt.Relation.Relname = shardedTableName

				// Now handle ID generation with args
				err := s.assignIDToInsert(insertStmt, r, &args)
				if err != nil {
					return ftQuery, stQuery, tableName, err
				}
			}
		} else {
			// Handle non-insert statements (SELECT, UPDATE, DELETE)
			shardingKey := r.ShardingKey
			if strings.Contains(shardingKey, ".") {
				parts := strings.Split(shardingKey, ".")
				shardingKey = parts[len(parts)-1]
			}
			var aliasMap map[string]string
			if isSelect {
				tables = collectTablesFromSelect(selectStmt)

				// Add logging before and after extracting sharding keys
				for _, table := range tables {
					s.mutex.RLock()
					cfg, ok := s.configs[table]
					s.mutex.RUnlock()

					if ok {
						shardingKey := cfg.ShardingKey
						value, id, keyFound, _ := s.extractShardingKeyFromConditions(shardingKey, conditions, args, aliasMap, table)
						//GetLogger().Debug("For table %s: keyFound=%v, value=%v, id=%v, err=%v", table, keyFound, value, id, err)

						// Log when suffix is determined
						if keyFound || (id != 0) {
							suffix, err := getSuffix(value, id, keyFound, cfg)
							GetLogger().Debug("For table %s: suffix=%s, err=%v", table, suffix, err)

							// Log the updating of tableMap
							shardedTableName := table + suffix
							GetLogger().Debug("Adding to tableMap: %s -> %s", table, shardedTableName)

							// Thread-safely update the tableMap
							tableMapMutex.Lock()
							tableMap[table] = shardedTableName
							tableMapMutex.Unlock()
						}
					} else {
						GetLogger().Debug("No config found for table %s", table)
					}
				}

				// After building tableMap
				GetLogger().Debug("Final tableMap for SELECT: %v", tableMap)
			}

			// Extract sharding key for the current table
			value, id, keyFound, _ := s.extractShardingKeyFromConditions(shardingKey, conditions, args, aliasMap, fullTableName)

			// Get the config again for DoubleWrite check
			s.mutex.RLock()
			cfg, configOk := s.configs[fullTableName]
			s.mutex.RUnlock()

			// Determine suffix and update tableMap
			if keyFound || (id != 0) { // Sharding key or ID found (even if err != nil, keyFound might be true from LOWER)
				suffix, suffixErr := getSuffix(value, id, keyFound, cfg)
				if suffixErr != nil {
					// If DoubleWrite is enabled, maybe we can proceed with the base table?
					// Or should it always error if a key was expected but suffix failed?
					// Let's stick to erroring for now unless DoubleWrite logic dictates otherwise.
					GetLogger().Error("Error getting suffix for %s: %v. Value: %v, ID: %d, KeyFound: %t", fullTableName, suffixErr, value, id, keyFound)
					return ftQuery, stQuery, tableName, suffixErr
				}
				shardedTableName := originalTableName + suffix
				tableMapMutex.Lock()
				tableMap[fullTableName] = shardedTableName // Use fullTableName as key
				tableMapMutex.Unlock()
				GetLogger().Debug("Sharding key/ID found for %s. Using sharded table: %s", fullTableName, shardedTableName)
			} else if configOk && cfg.DoubleWrite {
				// No sharding key/ID found, but DoubleWrite is enabled.
				// Check if this is a JOIN and if *other* tables have sharding keys.
				isJoinQuery := isSelect && len(tables) > 1
				otherTableHasKey := false
				if isJoinQuery {
					for _, joinedTable := range tables {
						// Ensure joinedTable is not the same as fullTableName before proceeding
						if joinedTable == fullTableName {
							continue // Skip self
						}

						s.mutex.RLock()
						joinedCfg, joinedOk := s.configs[joinedTable]
						s.mutex.RUnlock()

						if joinedOk {
							joinedShardingKey := joinedCfg.ShardingKey
							// Pass the correct aliasMap (which might be nil) to the recursive call
							_, _, joinedKeyFound, _ := s.extractShardingKeyFromConditions(
								joinedShardingKey, conditions, args, aliasMap, joinedTable) // Pass joinedTable and aliasMap here
							if joinedKeyFound {
								otherTableHasKey = true
								GetLogger().Debug("Other table %s in JOIN has sharding key.", joinedTable)
								break
							}
						}
					}
				}

				if isJoinQuery && otherTableHasKey {
					// JOIN query, DoubleWrite enabled, *no key for this table*, but *other tables have keys*.
					// -> Use the BASE table for this table. Don't add to tableMap.
					GetLogger().Debug("DoubleWrite enabled for %s, no key found, but other tables in JOIN have keys. Using base table.", fullTableName)
				} else {
					// Not a JOIN, or no other tables have keys.
					// With DoubleWrite, default to the base table if no key is found.
					// So, don't add to tableMap.
					GetLogger().Debug("DoubleWrite enabled for %s, no key found. Using base table.", fullTableName)
					// If strict sharding is needed even with DoubleWrite, uncomment the error:
					// return ftQuery, stQuery, tableName, ErrMissingShardingKey
				}
			} else {
				// No sharding key/ID found, and DoubleWrite is NOT enabled (or config not found).
				// This is an error condition unless it's a system query or explicitly ignored.
				// The initial checks for system queries should handle those cases.
				GetLogger().Error("Missing sharding key for table %s and DoubleWrite is not enabled or config not found. Query: %s", fullTableName, query)
				return ftQuery, stQuery, tableName, ErrMissingShardingKey
			}
		}
	}

	// Traverse the AST and replace original table names with sharded table names based on tableMap
	replaceTableNames(stmt.Stmt, tableMap)

	// Deparse the modified AST back to SQL
	stmts := []*pg_query.RawStmt{stmt}
	stQuery, err = pg_query.Deparse(&pg_query.ParseResult{Stmts: stmts})
	if err != nil {
		return ftQuery, stQuery, tableName, fmt.Errorf("error deparsing modified query: %v", err)
	}

	return
}

// extractShardingKeyFromConditions extracts the sharding key for a specific table based on its conditions
func (s *Sharding) extractShardingKeyFromConditions(shardingKey string, conditions []*pg_query.Node, args []interface{}, aliasMap map[string]string, currentTable string) (value interface{}, id int64, keyFound bool, err error) {
	// Initialize a separate knownKeys map for the current table
	knownKeys := make(map[string]interface{})

	// Get the config for this table
	config, found := s.configs[currentTable]
	if !found {
		return nil, 0, false, fmt.Errorf("no sharding config found for table %s", currentTable)
	}

	// First, check if any condition contains a LOWER function on the sharding key
	var lowerKeyFound bool // Declare keyFound and value outside the loop
	var lowerValue interface{}
	var lowerErr error // Declare error variable outside the loop
	for _, condition := range conditions {
		if containsLowerFunction(condition) {
			// If we find a LOWER function, try to extract the sharding key from it
			// Assign results to the variables declared outside the loop
			lowerKeyFound, lowerValue, lowerErr = extractShardingKeyFromLowerFunction(shardingKey, condition, args, knownKeys, aliasMap)
			if lowerKeyFound || lowerErr != nil { // Check if key found OR if an error occurred
				// Break the loop to handle the result after the loop finishes
				break
			}
		}
	}
	// Check the result after the first loop completes or breaks
	if lowerKeyFound || lowerErr != nil {
		GetLogger().Debug("Found sharding key %s in LOWER function: value=%v, err=%v", shardingKey, lowerValue, lowerErr)
		err = lowerErr // Assign the error before returning
		return lowerValue, id, lowerKeyFound, err
	}

	// If not found via LOWER(), iterate through conditions normally
	for _, condition := range conditions {
		keyFound, value, err = traverseConditionForKey(shardingKey, condition, args, knownKeys, aliasMap)
		if keyFound || err != nil { // If found or error in normal traversal
			break
		}

		// If no direct equality found, look for the sharding key in composite IN conditions
		// This handles ((col1, col2, ...)) IN ((val1, val2, ...)) pattern
		keyFound, value, err = extractShardingKeyFromCompositeIn(shardingKey, condition, args, knownKeys)
		if keyFound || err != nil {
			GetLogger().Debug("Composite IN condition found for sharding key %s: value=%v, err=%v", shardingKey, value, err)
			return value, id, keyFound, err
		}

		// check for "is_" boolean fields
		if config.PartitionType == PartitionTypeList {
			// Check for boolean fields like "is_erc20", "is_erc721", etc.
			for typeName := range config.ListValues {
				boolField := "is_" + strings.ToLower(typeName)

				// Check all conditions for this boolean field
				for _, condition := range conditions {
					foundBool, boolValue, err := traverseConditionForKey(boolField, condition, args, knownKeys, aliasMap)
					if err != nil {
						continue
					}

					if foundBool {
						// Check if it's true
						if bVal, ok := boolValue.(bool); ok && bVal {
							// Found "is_xxx = true" condition, use the corresponding type
							return typeName, 0, true, nil
						}
					}
				}
			}
		}
	}

	// If sharding key is not found, attempt to find 'id' for primary key sharding
	if !keyFound {
		var idFound bool
		var idValue interface{}

		// For hash partitioning, we can use the ID, but for list partitioning,
		// we must have the list key to determine the correct partition
		if config.PartitionType == PartitionTypeList {

			// todo  use global index instead
			return nil, 0, false, ErrMissingShardingKey
		}

		for _, condition := range conditions {
			//GetLogger().Trace("Traversing condition for 'id'")
			idFound, idValue, err = traverseConditionForKey("id", condition, args, knownKeys, aliasMap)
			if idFound || err != nil {
				break
			}
		}
		if idFound {
			idInt64, err := toInt64(idValue)
			if err != nil {
				return nil, 0, false, ErrInvalidID
			}
			id = idInt64
			return nil, id, true, nil
		} else {
			// Neither sharding key nor 'id' found; return error
			err = ErrMissingShardingKey
			return nil, 0, false, err
		}
	}

	return value, id, keyFound, err
}

// extractShardingKeyFromLowerFunction specifically extracts the sharding key from LOWER function calls
func extractShardingKeyFromLowerFunction(shardingKey string, node *pg_query.Node, args []interface{}, knownKeys map[string]interface{}, aliasMap map[string]string) (keyFound bool, value interface{}, err error) {
	if node == nil {
		return false, nil, nil
	}

	switch n := node.Node.(type) {
	case *pg_query.Node_AExpr:
		if n.AExpr.Kind == pg_query.A_Expr_Kind_AEXPR_OP && len(n.AExpr.Name) > 0 {
			opName := n.AExpr.Name[0].Node.(*pg_query.Node_String_).String_.Sval
			if opName == "=" {
				// Check left side for LOWER function
				if funcCall, ok := n.AExpr.Lexpr.Node.(*pg_query.Node_FuncCall); ok {
					if len(funcCall.FuncCall.Funcname) > 0 {
						funcName := funcCall.FuncCall.Funcname[0].Node.(*pg_query.Node_String_).String_.Sval
						if strings.EqualFold(funcName, "lower") && len(funcCall.FuncCall.Args) > 0 {
							// Extract the column from LOWER(column)
							if argColRef, ok := funcCall.FuncCall.Args[0].Node.(*pg_query.Node_ColumnRef); ok {
								colName := extractColumnName(argColRef.ColumnRef, aliasMap)
								GetLogger().Debug("Checking LEFT for sharding key in LOWER function: %s %s", colName, getColumnNameWithoutTable(colName))
								if getColumnNameWithoutTable(colName) == shardingKey {
									GetLogger().Debug("Found LEFT sharding key in LOWER function: %s", colName)

									// Check if right side is also a LOWER function
									if rightFuncCall, ok := n.AExpr.Rexpr.Node.(*pg_query.Node_FuncCall); ok {
										if len(rightFuncCall.FuncCall.Funcname) > 0 {
											rightFuncName := rightFuncCall.FuncCall.Funcname[0].Node.(*pg_query.Node_String_).String_.Sval
											if strings.EqualFold(rightFuncName, "lower") && len(rightFuncCall.FuncCall.Args) > 0 {
												// Extract value from right LOWER function argument
												rightVal, err := extractValueFromExpr(rightFuncCall.FuncCall.Args[0], args)
												if err == nil && rightVal != nil {
													GetLogger().Debug("Found sharding key %s in LOWER function with LOWER on both sides: value=%v", shardingKey, rightVal)
													return true, rightVal, nil
												}
											}
										}
									}

									// If right side is not a LOWER function, extract value directly
									rightVal, err := extractValueFromExpr(n.AExpr.Rexpr, args)
									if err == nil && rightVal != nil {
										GetLogger().Debug("Found sharding key %s in LOWER function: value=%v", shardingKey, rightVal)
										return true, rightVal, nil
									}
								}
							}
						}
					}
				}

				// Check right side for LOWER function
				if funcCall, ok := n.AExpr.Rexpr.Node.(*pg_query.Node_FuncCall); ok {
					if len(funcCall.FuncCall.Funcname) > 0 {
						funcName := funcCall.FuncCall.Funcname[0].Node.(*pg_query.Node_String_).String_.Sval
						if strings.EqualFold(funcName, "lower") && len(funcCall.FuncCall.Args) > 0 {
							// Extract the column from LOWER(column)
							if argColRef, ok := funcCall.FuncCall.Args[0].Node.(*pg_query.Node_ColumnRef); ok {
								colName := extractColumnName(argColRef.ColumnRef, aliasMap)
								GetLogger().Debug("Checking RIGHT for sharding key in LOWER function: %s", colName)
								if getColumnNameWithoutTable(colName) == shardingKey {
									GetLogger().Debug("Found RIGHT sharding key in LOWER function: %s", colName)

									// Check if left side is also a LOWER function
									if leftFuncCall, ok := n.AExpr.Lexpr.Node.(*pg_query.Node_FuncCall); ok {
										if len(leftFuncCall.FuncCall.Funcname) > 0 {
											leftFuncName := leftFuncCall.FuncCall.Funcname[0].Node.(*pg_query.Node_String_).String_.Sval
											if strings.EqualFold(leftFuncName, "lower") && len(leftFuncCall.FuncCall.Args) > 0 {
												// Extract value from left LOWER function argument
												leftVal, err := extractValueFromExpr(leftFuncCall.FuncCall.Args[0], args)
												if err == nil && leftVal != nil {
													GetLogger().Debug("Found sharding key %s in LOWER function with LOWER on both sides: value=%v", shardingKey, leftVal)
													return true, leftVal, nil
												}
											}
										}
									}

									// If left side is not a LOWER function, extract value directly
									leftVal, err := extractValueFromExpr(n.AExpr.Lexpr, args)
									if err == nil && leftVal != nil {
										GetLogger().Debug("Found sharding key %s in LOWER function: value=%v", shardingKey, leftVal)
										return true, leftVal, nil
									}
								}
							}
						}
					}
				}

				// Check for LOWER on both sides (e.g., LOWER(col) = LOWER(?))
				if leftFuncCall, ok := n.AExpr.Lexpr.Node.(*pg_query.Node_FuncCall); ok {
					if rightFuncCall, ok := n.AExpr.Rexpr.Node.(*pg_query.Node_FuncCall); ok {
						// Both sides are functions, check if both are LOWER
						if len(leftFuncCall.FuncCall.Funcname) > 0 && len(rightFuncCall.FuncCall.Funcname) > 0 {
							leftFuncName := leftFuncCall.FuncCall.Funcname[0].Node.(*pg_query.Node_String_).String_.Sval
							rightFuncName := rightFuncCall.FuncCall.Funcname[0].Node.(*pg_query.Node_String_).String_.Sval

							if strings.EqualFold(leftFuncName, "lower") && strings.EqualFold(rightFuncName, "lower") {
								// Both are LOWER functions
								if len(leftFuncCall.FuncCall.Args) > 0 && len(rightFuncCall.FuncCall.Args) > 0 {
									// Check if left arg is our sharding key column
									if leftArgColRef, ok := leftFuncCall.FuncCall.Args[0].Node.(*pg_query.Node_ColumnRef); ok {
										leftColName := extractColumnName(leftArgColRef.ColumnRef, aliasMap)
										if getColumnNameWithoutTable(leftColName) == shardingKey {
											// Extract value from right LOWER function argument
											rightVal, err := extractValueFromExpr(rightFuncCall.FuncCall.Args[0], args)
											if err == nil && rightVal != nil {
												GetLogger().Debug("Found sharding key %s in LOWER function (both sides): value=%v", shardingKey, rightVal)
												return true, rightVal, nil
											}
										}
									}

									// Check if right arg is our sharding key column
									if rightArgColRef, ok := rightFuncCall.FuncCall.Args[0].Node.(*pg_query.Node_ColumnRef); ok {
										rightColName := extractColumnName(rightArgColRef.ColumnRef, aliasMap)
										if getColumnNameWithoutTable(rightColName) == shardingKey {
											// Extract value from left LOWER function argument
											leftVal, err := extractValueFromExpr(leftFuncCall.FuncCall.Args[0], args)
											if err == nil && leftVal != nil {
												GetLogger().Debug("Found sharding key %s in LOWER function (both sides): value=%v", shardingKey, leftVal)
												return true, leftVal, nil
											}
										}
									}
								}
							}
						}
					}
				}
			}
		}
	case *pg_query.Node_BoolExpr:
		// Recursively check all arguments in the boolean expression
		for _, arg := range n.BoolExpr.Args {
			keyFound, value, err = extractShardingKeyFromLowerFunction(shardingKey, arg, args, knownKeys, aliasMap)
			if keyFound || err != nil {
				return keyFound, value, err
			}
		}
	}

	return false, nil, nil
}

func extractShardingKeyFromCompositeIn(shardingKey string, node *pg_query.Node, args []interface{}, knownKeys map[string]interface{}) (keyFound bool, value interface{}, err error) {
	if node == nil {
		return false, nil, nil
	}

	switch n := node.Node.(type) {
	case *pg_query.Node_AExpr:
		if n.AExpr.Kind == pg_query.A_Expr_Kind_AEXPR_IN {
			// This is an IN expression
			GetLogger().Debug("Found IN expression")

			// Check the left side (columns) of the IN expression
			if row, ok := n.AExpr.Lexpr.Node.(*pg_query.Node_RowExpr); ok {
				GetLogger().Debug("Found row expression with %d args", len(row.RowExpr.Args))

				// Check each column in the composite key to see if it matches our sharding key
				for colIndex, colNode := range row.RowExpr.Args {
					if colRef, ok := colNode.Node.(*pg_query.Node_ColumnRef); ok {
						colName := extractColumnName(colRef.ColumnRef, nil)
						GetLogger().Debug("Column at position %d: %s", colIndex, colName)

						// If this column matches our sharding key, extract the value from the right side
						if getColumnNameWithoutTable(colName) == shardingKey {
							GetLogger().Debug("Found sharding key %s in composite IN at position %d", shardingKey, colIndex)

							// Handle the right side based on its type
							switch rexpr := n.AExpr.Rexpr.Node.(type) {
							case *pg_query.Node_List:
								// List of RowExprs or other values
								GetLogger().Debug("Right side is a list with %d items", len(rexpr.List.Items))
								if len(rexpr.List.Items) > 0 {
									if firstRow, ok := rexpr.List.Items[0].Node.(*pg_query.Node_RowExpr); ok {
										// Extract the value at the same position as our sharding key
										if colIndex < len(firstRow.RowExpr.Args) {
											valueNode := firstRow.RowExpr.Args[colIndex]
											GetLogger().Debug("Extracting value from position %d", colIndex)
											value, err := extractValueFromExpr(valueNode, args)
											return true, value, err
										}
									}
								}
							case *pg_query.Node_RowExpr:
								// Single RowExpr
								GetLogger().Debug("Right side is a row expression with %d items", len(rexpr.RowExpr.Args))
								if colIndex < len(rexpr.RowExpr.Args) {
									valueNode := rexpr.RowExpr.Args[colIndex]
									value, err := extractValueFromExpr(valueNode, args)
									return true, value, err
								}
							case *pg_query.Node_SubLink:
								// Subquery - currently not supported
								GetLogger().Debug("Right side is a subquery (not supported)")
								return false, nil, fmt.Errorf("subquery in composite IN not supported")
							}
						}
					}
				}
			}
		}
	}

	return false, nil, nil
}

func collectJoinConditions(selectStmt *pg_query.SelectStmt) []*pg_query.Node {
	var conditions []*pg_query.Node
	for _, fromItem := range selectStmt.FromClause {
		conditions = append(conditions, extractSpecialConditionsFromNode(fromItem)...)
	}
	return conditions
}

func extractSpecialConditionsFromNode(node *pg_query.Node) []*pg_query.Node {
	var conditions []*pg_query.Node
	switch n := node.Node.(type) {
	case *pg_query.Node_JoinExpr:
		if n.JoinExpr.Quals != nil {
			conditions = append(conditions, n.JoinExpr.Quals)
		}
		// Recursively extract from left and right arguments
		conditions = append(conditions, extractSpecialConditionsFromNode(n.JoinExpr.Larg)...)
		conditions = append(conditions, extractSpecialConditionsFromNode(n.JoinExpr.Rarg)...)
	case *pg_query.Node_RangeSubselect:
		if subselect, ok := n.RangeSubselect.Subquery.Node.(*pg_query.Node_SelectStmt); ok {
			conditions = append(conditions, collectConditionsFromSelect(subselect.SelectStmt)...)
		}
	}
	return conditions
}

func collectConditionsFromSelect(selectStmt *pg_query.SelectStmt) []*pg_query.Node {
	var conditions []*pg_query.Node
	if selectStmt.WhereClause != nil {
		conditions = append(conditions, selectStmt.WhereClause)
	}
	conditions = append(conditions, collectJoinConditions(selectStmt)...)
	return conditions
}

// assignIDToInsert generates a new ID and assigns it to the insert statement's VALUES list
func (s *Sharding) assignIDToInsert(insertStmt *pg_query.InsertStmt, r Config, args *[]interface{}) error {
	// Check if 'id' column is present
	hasID := false
	idIndex := -1
	for i, colItem := range insertStmt.Cols {
		resTarget, ok := colItem.Node.(*pg_query.Node_ResTarget)
		if !ok {
			return fmt.Errorf("unsupported column item type: %T", colItem.Node)
		}
		colName := resTarget.ResTarget.Name
		if strings.ToLower(colName) == "id" {
			hasID = true
			idIndex = i
			break
		}
	}

	// Extract shard index from table name
	shardedTableName := insertStmt.Relation.Relname
	shardIndex, err := extractShardIndex(shardedTableName, r)
	if err != nil {
		return err
	}

	if hasID {
		// Handle existing 'id' column
		if insertStmt.SelectStmt == nil {
			return fmt.Errorf("insert statement has no SelectStmt")
		}

		selectNode, ok := insertStmt.SelectStmt.Node.(*pg_query.Node_SelectStmt)
		if !ok {
			return fmt.Errorf("insert statement SelectStmt is not of type SelectStmt")
		}

		valuesSelect := selectNode.SelectStmt
		if len(valuesSelect.ValuesLists) == 0 {
			return fmt.Errorf("insert statement has no VALUES list")
		}

		// Iterate through each VALUES list to assign 'id's
		for idx, valuesList := range valuesSelect.ValuesLists {
			listNode, ok := valuesList.Node.(*pg_query.Node_List)
			if !ok {
				return fmt.Errorf("unsupported values list type when assigning id")
			}

			// Ensure the VALUES list has enough items
			if len(listNode.List.Items) <= idIndex {
				return fmt.Errorf("values list does not have enough items for 'id' in VALUES list index %d", idx)
			}

			// Extract the current 'id' value
			expr := listNode.List.Items[idIndex]
			exprValue, err := extractValueFromExpr(expr, *args)
			if err != nil {
				return err
			}

			// Convert the 'id' value to int64
			idInt64, err := toInt64(exprValue)
			if err != nil {
				return err
			}

			if idInt64 == 0 || idInt64 < 0 {
				// Generate a new ID if 'id' is zero or negative (not explicitly set)
				if r.PrimaryKeyGeneratorFn != nil {
					generatedID := r.PrimaryKeyGeneratorFn(int64(shardIndex))
					if generatedID != 0 {
						// Replace the 'id' value with the generated ID
						listNode.List.Items[idIndex] = &pg_query.Node{
							Node: &pg_query.Node_AConst{
								AConst: &pg_query.A_Const{
									Val: &pg_query.A_Const_Ival{
										Ival: &pg_query.Integer{Ival: int32(generatedID)},
									},
								},
							},
						}
					}
					// Else, leave 'id' as zero
				}
			}
			// Else, leave 'id' as is
		}
	} else {
		// 'id' is not present in insert columns
		// Only add 'id' if the generator is NOT PKCustom and a generator function exists
		if r.PrimaryKeyGenerator != PKCustom && r.PrimaryKeyGeneratorFn != nil {
			// Add the 'id' column definition if it wasn't present
			log.Println("'id' column not present in insert columns; adding it.")
			insertStmt.Cols = append(insertStmt.Cols, &pg_query.Node{
				Node: &pg_query.Node_ResTarget{
					ResTarget: &pg_query.ResTarget{
						Name: "id",
					},
				},
			})

			if insertStmt.SelectStmt == nil {
				return fmt.Errorf("insert statement has no SelectStmt")
			}

			selectNode, ok := insertStmt.SelectStmt.Node.(*pg_query.Node_SelectStmt)
			if !ok {
				return fmt.Errorf("insert statement SelectStmt is not of type SelectStmt")
			}

			valuesSelect := selectNode.SelectStmt
			if len(valuesSelect.ValuesLists) == 0 {
				return fmt.Errorf("insert statement has no VALUES list")
			}

			// Iterate through each VALUES list to assign a UNIQUE ID
			for _, valuesList := range valuesSelect.ValuesLists { // Loop starts here
				listNode, ok := valuesList.Node.(*pg_query.Node_List)
				if !ok {
					return fmt.Errorf("unsupported values list type when assigning id")
				}

				// Generate a NEW unique ID for EACH row in the batch
				uniqueGeneratedID := r.PrimaryKeyGeneratorFn(int64(shardIndex))

				// Append the unique generated ID to this specific VALUES list
				// Ensure a distinct A_Const node is created for each row's ID
				listNode.List.Items = append(listNode.List.Items, &pg_query.Node{
					Node: &pg_query.Node_AConst{
						AConst: &pg_query.A_Const{
							Val: &pg_query.A_Const_Ival{
								// Create a new Integer struct for each ID
								Ival: &pg_query.Integer{Ival: int32(uniqueGeneratedID)},
							},
							Location: -1, // Force quoting/handling as constant
						},
					},
				})
			}
		}
		// Else, PrimaryKeyGenerator is PKCustom or PrimaryKeyGeneratorFn is nil, so we skip adding 'id' column
	}

	return nil
}

// Helper function to extract shard index from the sharded table name
func extractShardIndex(tableName string, r Config) (int64, error) {
	// Assuming tableName has suffix like "_0", "_1", etc.
	parts := strings.Split(tableName, "_")
	if len(parts) < 2 {
		return 0, fmt.Errorf("table name '%s' does not have a suffix", tableName)
	}
	suffix := parts[len(parts)-1]
	shardIndex, err := strconv.Atoi(suffix)
	if err != nil {
		return 0, fmt.Errorf("invalid shard index in table name '%s': %v", tableName, err)
	}
	return int64(shardIndex), nil
}

func (s *Sharding) extractInsertShardingKeyFromValues(r Config, insertStmt *pg_query.InsertStmt, valuesList *pg_query.Node, args ...interface{}) (value interface{}, id int64, keyFound bool, err error) {
	// Ensure that columns are specified
	if len(insertStmt.Cols) == 0 {
		return nil, 0, false, errors.New("invalid insert statement structure: no columns specified")
	}

	// Type assert the valuesList to *pg_query.Node_List
	listNode, ok := valuesList.Node.(*pg_query.Node_List)
	if !ok {
		return nil, 0, false, errors.New("unsupported values list type")
	}
	list := listNode.List

	// Ensure the number of values matches the number of columns
	if len(list.Items) != len(insertStmt.Cols) {
		return nil, 0, false, errors.New("values list has fewer items than columns")
	}
	// Iterate through columns to find the sharding key and id
	for i, colItem := range insertStmt.Cols {
		resTarget, ok := colItem.Node.(*pg_query.Node_ResTarget)
		if !ok {
			return nil, 0, false, fmt.Errorf("unsupported column item type: %T", colItem.Node)
		}
		colName := resTarget.ResTarget.Name

		expr := list.Items[i]
		exprValue, err := s.extractValueFromExpr(expr, args, r) // Use updated method
		if err != nil {
			return nil, 0, false, err
		}
		if strings.EqualFold(colName, r.ShardingKey) {
			GetLogger().Debug("Found sharding key '%s' with value: %v", colName, exprValue)
			value = exprValue
			keyFound = true
		}

		if strings.ToLower(colName) == "id" {
			// Corrected: Convert exprValue, not idValue
			idValue, err := toInt64(exprValue) // Use exprValue here
			if err == nil {
				id = idValue
			}
		}

	}

	// For list partitioning, we must have the list key
	if r.PartitionType == PartitionTypeList && !keyFound {
		// todo use a global index instead
		return nil, 0, false, ErrMissingShardingKey
	}

	if r.PartitionType == PartitionTypeHash && !keyFound {
		return nil, 0, false, ErrMissingShardingKey
	}

	return value, id, keyFound, nil
}

func toInt64(value interface{}) (int64, error) {

	if value == nil {
		return 0, fmt.Errorf("cannot convert nil to int64")
	}

	// Handle pointer types first
	valueType := reflect.TypeOf(value)
	if valueType.Kind() == reflect.Ptr {
		if reflect.ValueOf(value).IsNil() {
			return 0, fmt.Errorf("cannot convert nil pointer to int64")
		}
		// Dereference the pointer and recursively call toInt64
		return toInt64(reflect.ValueOf(value).Elem().Interface())
	}

	switch v := value.(type) {
	case int:
		return int64(v), nil
	case int8:
		return int64(v), nil
	case int16:
		return int64(v), nil
	case int32:
		return int64(v), nil
	case int64:
		return v, nil
	case uint:
		return int64(v), nil
	case uint8:
		return int64(v), nil
	case uint16:
		return int64(v), nil
	case uint32:
		return int64(v), nil
	case uint64:
		if v > math.MaxInt64 {
			GetLogger().Error("uint64 value %d overflows int64", v)
			return 0, fmt.Errorf("uint64 value %d overflows int64", v)
		}
		return int64(v), nil
	case float32:
		return int64(v), nil
	case float64:
		return int64(v), nil
	case string:
		i, err := strconv.ParseInt(v, 10, 64)
		if err != nil {
			return 0, fmt.Errorf("error converting string to int64: %v", err)
		}
		return i, nil
	default:
		val := reflect.ValueOf(value)
		if val.Kind() == reflect.Struct {
			// Try to find a field named "I" that is a *big.Int
			field := val.FieldByName("I")
			if field.IsValid() && field.Type() == reflect.TypeOf(&big.Int{}) {
				bigIntPtr := field.Interface().(*big.Int)
				if bigIntPtr != nil {
					return bigIntPtr.Int64(), nil
				}
			}
		}
		GetLogger().Error("Unsupported type for conversion to int64: %T", v)
		return 0, fmt.Errorf("unsupported type for conversion to int64: %T", v)
	}
}

func collectTablesFromSelect(selectStmt *pg_query.SelectStmt) []string {
	var tables []string
	for _, fromItem := range selectStmt.FromClause {

		switch node := fromItem.Node.(type) {
		case *pg_query.Node_RangeVar:
			tables = append(tables, node.RangeVar.Relname)
		case *pg_query.Node_JoinExpr:
			tables = append(tables, collectTablesFromJoin(node.JoinExpr)...)
		case *pg_query.Node_RangeSubselect:
			// Recurse into subselect
			if subselect, ok := node.RangeSubselect.Subquery.Node.(*pg_query.Node_SelectStmt); ok {
				tables = append(tables, collectTablesFromSelect(subselect.SelectStmt)...)
			}
		}
	}
	return tables
}

// collectAllTablesFromUnion recursively finds all base table names within a UNION structure
func collectAllTablesFromUnion(selectStmt *pg_query.SelectStmt) map[string]struct{} {
	allTables := make(map[string]struct{})

	var collect func(*pg_query.Node)
	collect = func(node *pg_query.Node) {
		if node == nil {
			return
		}
		if selStmtNode, ok := node.Node.(*pg_query.Node_SelectStmt); ok {
			subSelectStmt := selStmtNode.SelectStmt
			if subSelectStmt.Op == pg_query.SetOperation_SETOP_UNION { // Check for UNION op
				// Nested UNION - Wrap Larg/Rarg back into Node for recursion
				if subSelectStmt.Larg != nil {
					largNode := &pg_query.Node{Node: &pg_query.Node_SelectStmt{SelectStmt: subSelectStmt.Larg}}
					collect(largNode)
				}
				if subSelectStmt.Rarg != nil {
					rargNode := &pg_query.Node{Node: &pg_query.Node_SelectStmt{SelectStmt: subSelectStmt.Rarg}}
					collect(rargNode)
				}
			} else {
				// Base SELECT statement
				tables := collectTablesFromSelect(subSelectStmt)
				for _, tbl := range tables {
					allTables[tbl] = struct{}{}
				}
			}
		}
		// Handle other node types like RangeSubselect if necessary
	}

	// Start collection from the top-level SelectStmt node
	collect(&pg_query.Node{Node: &pg_query.Node_SelectStmt{SelectStmt: selectStmt}})

	return allTables
}

// collectTablesFromJoin extracts table names from JOIN expressions
func collectTablesFromJoin(joinExpr *pg_query.JoinExpr) []string {
	var tables []string
	if joinExpr.Larg != nil {
		tables = append(tables, collectTablesFromExpr(joinExpr.Larg)...)
	}
	if joinExpr.Rarg != nil {
		tables = append(tables, collectTablesFromExpr(joinExpr.Rarg)...)
	}
	return tables
}

func collectTablesFromExpr(expr *pg_query.Node) []string {
	switch n := expr.Node.(type) {
	case *pg_query.Node_RangeVar:
		return []string{n.RangeVar.Relname}
	case *pg_query.Node_JoinExpr:
		return collectTablesFromJoin(n.JoinExpr)
	case *pg_query.Node_RangeSubselect:
		if subselect, ok := n.RangeSubselect.Subquery.Node.(*pg_query.Node_SelectStmt); ok {
			return collectTablesFromSelect(subselect.SelectStmt)
		}
	}
	return nil
}

// Helper function to get keys from a map[string]bool
func mapsKeys(m map[string]bool) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	return keys
}

func caseInsensitiveTableLookup(tableMap map[string]string, tableName string) (string, bool) {
	// Direct lookup first
	if val, ok := tableMap[tableName]; ok {
		return val, true
	}

	// Case-insensitive lookup if direct lookup fails
	for key, val := range tableMap {
		if strings.EqualFold(key, tableName) {
			return val, true
		}
	}

	return "", false
}

func replaceTableNames(node *pg_query.Node, tableMap map[string]string) {
	if node == nil {
		return
	}

	switch n := node.Node.(type) {
	case *pg_query.Node_RangeVar:
		if n.RangeVar.Schemaname != "" {
			// GetLogger().Debug("replaceTableNames: Skipping schema-qualified table: %s.%s", n.RangeVar.Schemaname, n.RangeVar.Relname)
			return
		}
		originalName := n.RangeVar.Relname
		// GetLogger().Debug("replaceTableNames: Processing RangeVar: '%s', tableMap: %v", originalName, tableMap)
		// Replace table names in RangeVar nodes
		if shardedName, exists := caseInsensitiveTableLookup(tableMap, originalName); exists {
			// GetLogger().Debug("replaceTableNames: Replacing '%s' with '%s'", originalName, shardedName)
			n.RangeVar.Relname = shardedName
			n.RangeVar.Location = -1 // Force quoting
		} else {
			// GetLogger().Debug("replaceTableNames: No replacement found for '%s' in map", originalName)
		}

	case *pg_query.Node_UpdateStmt:
		if newName, ok := tableMap[n.UpdateStmt.Relation.Relname]; ok {
			//GetLogger().Debug("Replacing table name '%s' with sharded name '%s' in UpdateStmt", n.UpdateStmt.Relation.Relname, newName)
			n.UpdateStmt.Relation.Relname = newName
			n.UpdateStmt.Relation.Location = -1 // Force quoting
		}
		replaceTableNames(n.UpdateStmt.WhereClause, tableMap)
		for _, target := range n.UpdateStmt.TargetList {
			replaceTableNames(target, tableMap)
		}
	case *pg_query.Node_DeleteStmt:
		if newName, ok := tableMap[n.DeleteStmt.Relation.Relname]; ok {
			//GetLogger().Debug("Replacing table name '%s' with sharded name '%s' in DeleteStmt", n.DeleteStmt.Relation.Relname, newName)
			n.DeleteStmt.Relation.Relname = newName
			n.DeleteStmt.Relation.Location = -1 // Force quoting
		}
		replaceTableNames(n.DeleteStmt.WhereClause, tableMap)
	case *pg_query.Node_SelectStmt:
		replaceSelectStmtTableName(n.SelectStmt, tableMap)
	case *pg_query.Node_JoinExpr:
		// Recursively process left and right arguments
		replaceTableNames(n.JoinExpr.Larg, tableMap)
		replaceTableNames(n.JoinExpr.Rarg, tableMap)
		replaceTableNames(n.JoinExpr.Quals, tableMap)

		// Also check if the left or right arguments are RangeVar nodes directly
		if larg, ok := n.JoinExpr.Larg.Node.(*pg_query.Node_RangeVar); ok {
			if larg.RangeVar.Schemaname == "" {
				if shardedName, exists := caseInsensitiveTableLookup(tableMap, larg.RangeVar.Relname); exists {
					larg.RangeVar.Relname = shardedName
					larg.RangeVar.Location = -1 // Force quoting
				}
			}
		}

		if rarg, ok := n.JoinExpr.Rarg.Node.(*pg_query.Node_RangeVar); ok {
			if rarg.RangeVar.Schemaname == "" {
				if shardedName, exists := caseInsensitiveTableLookup(tableMap, rarg.RangeVar.Relname); exists {
					rarg.RangeVar.Relname = shardedName
					rarg.RangeVar.Location = -1 // Force quoting
				}
			}
		}
	case *pg_query.Node_SortBy:
		replaceTableNames(n.SortBy.Node, tableMap)
	case *pg_query.Node_ResTarget:
		// Process expressions in the SELECT list
		replaceTableNames(n.ResTarget.Val, tableMap)
		if n.ResTarget.Name != "" {
			n.ResTarget.Location = -1 // Force quoting of column aliases
		}
	case *pg_query.Node_FuncCall:
		// Process function arguments
		for _, arg := range n.FuncCall.Args {
			replaceTableNames(arg, tableMap)
		}
	case *pg_query.Node_ColumnRef:
		fields := n.ColumnRef.Fields
		n.ColumnRef.Location = -1 // Force quoting of column names
		if len(fields) >= 2 {
			// Check if the first field is a table name
			if stringNode, ok := fields[0].Node.(*pg_query.Node_String_); ok {
				originalTableName := stringNode.String_.Sval
				if newTableName, exists := tableMap[originalTableName]; exists {
					// Replace the table name with the sharded name
					//GetLogger().Debug("Replacing table name '%s' with sharded name '%s' in ColumnRef", originalTableName, newTableName)
					stringNode.String_.Sval = newTableName
				}
			}
		}
	case *pg_query.Node_AExpr:
		replaceTableNames(n.AExpr.Lexpr, tableMap)
		replaceTableNames(n.AExpr.Rexpr, tableMap)
	case *pg_query.Node_BoolExpr:
		for _, arg := range n.BoolExpr.Args {
			replaceTableNames(arg, tableMap)
		}
	case *pg_query.Node_NamedArgExpr:
		replaceTableNames(n.NamedArgExpr.Arg, tableMap)
	case *pg_query.Node_Aggref:
		for _, arg := range n.Aggref.Args {
			replaceTableNames(arg, tableMap)
		}
	case *pg_query.Node_RangeSubselect:
		// Process the subquery in RangeSubselect
		if subselect, ok := n.RangeSubselect.Subquery.Node.(*pg_query.Node_SelectStmt); ok {
			replaceSelectStmtTableName(subselect.SelectStmt, tableMap)
		}
	case *pg_query.Node_SubLink:
		// Process subqueries in expressions
		if subselect, ok := n.SubLink.Subselect.Node.(*pg_query.Node_SelectStmt); ok {
			replaceSelectStmtTableName(subselect.SelectStmt, tableMap)
		}
	case *pg_query.Node_NullTest:
		// Handle IS NULL and IS NOT NULL conditions
		if n.NullTest.Arg != nil {
			replaceTableNames(n.NullTest.Arg, tableMap)
		}

	default:
		// Recursively process child nodes if any
		reflectValue := reflect.ValueOf(n)
		if reflectValue.Kind() == reflect.Ptr {
			reflectValue = reflectValue.Elem()
		}
		for i := 0; i < reflectValue.NumField(); i++ {
			field := reflectValue.Field(i)
			if field.Kind() == reflect.Ptr {
				if nodeField, ok := field.Interface().(*pg_query.Node); ok {
					replaceTableNames(nodeField, tableMap)
				}
			} else if field.Kind() == reflect.Slice {
				for j := 0; j < field.Len(); j++ {
					if nodeField, ok := field.Index(j).Interface().(*pg_query.Node); ok {
						replaceTableNames(nodeField, tableMap)
					}
				}
			}
		}
	}
}

func replaceSelectStmtTableName(selectStmt *pg_query.SelectStmt, tableMap map[string]string) {
	// Handle UNION operations by recursing into Larg and Rarg
	if selectStmt.Op == pg_query.SetOperation_SETOP_UNION {
		if selectStmt.Larg != nil {
			// Wrap Larg back into a Node before calling replaceTableNames
			largNode := &pg_query.Node{Node: &pg_query.Node_SelectStmt{SelectStmt: selectStmt.Larg}}
			replaceTableNames(largNode, tableMap)
		}
		if selectStmt.Rarg != nil {
			// Wrap Rarg back into a Node before calling replaceTableNames
			rargNode := &pg_query.Node{Node: &pg_query.Node_SelectStmt{SelectStmt: selectStmt.Rarg}}
			replaceTableNames(rargNode, tableMap)
		}
		// Also process other clauses like ORDER BY, LIMIT if they exist on the UNION node itself
	}

	// Recursively process FROM clause and other relevant clauses for non-UNION selects or the base parts of UNIONs
	for _, item := range selectStmt.FromClause {
		replaceTableNames(item, tableMap)
	}
	for _, target := range selectStmt.TargetList {
		replaceTableNames(target, tableMap)
	}
	replaceTableNames(selectStmt.WhereClause, tableMap)
	// ORDER BY clause
	for _, sortBy := range selectStmt.SortClause {
		replaceTableNames(sortBy, tableMap)
	}
	replaceTableNames(selectStmt.HavingClause, tableMap)
	if len(selectStmt.GroupClause) > 0 && len(selectStmt.SortClause) > 0 {
		// Check if we're ordering by a column that's not in GROUP BY
		for _, sortBy := range selectStmt.SortClause {
			if sortNode, ok := sortBy.Node.(*pg_query.Node_SortBy); ok {
				if colRef, ok := sortNode.SortBy.Node.Node.(*pg_query.Node_ColumnRef); ok {
					// Check if this column is in the GROUP BY
					colName := extractColumnName(colRef.ColumnRef, nil)
					inGroupBy := false

					for _, groupBy := range selectStmt.GroupClause {
						if groupColRef, ok := groupBy.Node.(*pg_query.Node_ColumnRef); ok {
							groupCol := extractColumnName(groupColRef.ColumnRef, nil)
							if colName == groupCol {
								inGroupBy = true
								break
							}
						}
					}

					// If not in GROUP BY, replace with the first GROUP BY column
					if !inGroupBy && len(selectStmt.GroupClause) > 0 {
						if _, ok := selectStmt.GroupClause[0].Node.(*pg_query.Node_ColumnRef); ok {
							// Replace the ORDER BY column with the GROUP BY column
							sortNode.SortBy.Node = selectStmt.GroupClause[0]
						}
					}
				}
			}
		}
	}
	if selectStmt.GroupClause != nil {
		for _, groupItem := range selectStmt.GroupClause {
			replaceTableNames(groupItem, tableMap)

			// Specifically check for ColumnRef nodes in GROUP BY
			if colRef, ok := groupItem.Node.(*pg_query.Node_ColumnRef); ok {
				for _, field := range colRef.ColumnRef.Fields {
					if stringNode, ok := field.Node.(*pg_query.Node_String_); ok {
						// Check if it matches table name
						if shardedTableName, exists := tableMap[stringNode.String_.Sval]; exists {
							stringNode.String_.Sval = shardedTableName
						}
					}
				}
			}
		}
	}

	// Handle RETURNING clause for INSERT statements
	if selectStmt.ValuesLists != nil {
		for _, returning := range selectStmt.ValuesLists {
			if resTarget, ok := returning.Node.(*pg_query.Node_ResTarget); ok {
				if colRef, ok := resTarget.ResTarget.Val.Node.(*pg_query.Node_ColumnRef); ok {
					for _, field := range colRef.ColumnRef.Fields {
						if stringNode, ok := field.Node.(*pg_query.Node_String_); ok {
							originalTableName := stringNode.String_.Sval
							if newTableName, exists := tableMap[originalTableName]; exists {
								stringNode.String_.Sval = newTableName
							}
						}
					}
				}
			}
		}
	}
}

func (s *Sharding) extractValueFromExpr(expr *pg_query.Node, args []interface{}, config Config) (interface{}, error) {
	value, err := extractValueFromExpr(expr, args)
	if err != nil {
		return nil, err
	}

	// If there's a value converter, use it
	if config.ValueConverter != nil {
		return config.ValueConverter(value)
	}

	return value, nil
}

func extractValueFromExpr(expr *pg_query.Node, args []interface{}) (interface{}, error) {
	switch v := expr.Node.(type) {
	case *pg_query.Node_ParamRef:
		// PostgreSQL parameters are 1-based
		index := int(v.ParamRef.Number) - 1
		if index >= 0 && index < len(args) {
			return args[index], nil
		} else {
			return nil, fmt.Errorf("parameter index out of range")
		}
	case *pg_query.Node_AConst:
		return extractValueFromAConst(v.AConst)
	default:
		return nil, fmt.Errorf("unsupported expression type: %T", expr.Node)
	}
}

func extractValueFromAConst(aConst *pg_query.A_Const) (interface{}, error) {
	switch val := aConst.Val.(type) {
	case *pg_query.A_Const_Ival:
		// Integer value
		if val.Ival != nil {
			return int64(val.Ival.Ival), nil
		}
	case *pg_query.A_Const_Sval:
		// String value
		if val.Sval != nil {
			return val.Sval.Sval, nil
		}
	case *pg_query.A_Const_Fval:
		// Float value
		if val.Fval != nil {
			f, err := strconv.ParseFloat(val.Fval.Fval, 64)
			if err != nil {
				return nil, err
			}
			return f, nil
		}
	case *pg_query.A_Const_Boolval:
		// Boolean value
		if val.Boolval != nil {
			return val.Boolval.Boolval, nil
		}
	case *pg_query.A_Const_Bsval:
		// Bit string value
		if val.Bsval != nil {
			return val.Bsval.Bsval, nil
		}
	default:
		return nil, fmt.Errorf("unsupported constant type: %T", val)
	}
	return nil, fmt.Errorf("value is nil in A_Const")
}

func traverseConditionForKey(shardingKey string, node *pg_query.Node, args []interface{}, knownKeys map[string]interface{}, aliasMap map[string]string) (keyFound bool, value interface{}, err error) {
	if node == nil {
		return false, nil, nil
	}

	switch n := node.Node.(type) {
	case *pg_query.Node_AExpr:
		if n.AExpr.Kind == pg_query.A_Expr_Kind_AEXPR_OP && len(n.AExpr.Name) > 0 {
			opName := n.AExpr.Name[0].Node.(*pg_query.Node_String_).String_.Sval
			if opName == "=" {
				var leftColName, rightColName string
				var leftValue, rightValue interface{}
				var leftIsCol, rightIsCol bool

				// Left expression - check for LOWER function
				if colRef, ok := n.AExpr.Lexpr.Node.(*pg_query.Node_ColumnRef); ok {
					leftColName = extractColumnName(colRef.ColumnRef, aliasMap)
					leftIsCol = true
				} else if funcCall, ok := n.AExpr.Lexpr.Node.(*pg_query.Node_FuncCall); ok {
					// Handle LOWER function on left side
					if len(funcCall.FuncCall.Funcname) > 0 {
						funcName := funcCall.FuncCall.Funcname[0].Node.(*pg_query.Node_String_).String_.Sval
						if strings.EqualFold(funcName, "lower") && len(funcCall.FuncCall.Args) > 0 {
							// Extract the column from LOWER(column)
							if argColRef, ok := funcCall.FuncCall.Args[0].Node.(*pg_query.Node_ColumnRef); ok {
								leftColName = extractColumnName(argColRef.ColumnRef, aliasMap)
								leftIsCol = true
								GetLogger().Debug("Detected LOWER function on column: %s", leftColName)

								// Check if this is the sharding key directly
								if getColumnNameWithoutTable(leftColName) == shardingKey {
									// If the right side is a value, we can return it directly
									if rightVal, err := extractValueFromExpr(n.AExpr.Rexpr, args); err == nil && rightVal != nil {
										return true, rightVal, nil
									}
								}
							}
						}
					}
				} else {
					leftValue, _ = extractValueFromExpr(n.AExpr.Lexpr, args)
				}

				// Right expression - check for LOWER function
				if colRef, ok := n.AExpr.Rexpr.Node.(*pg_query.Node_ColumnRef); ok {
					rightColName = extractColumnName(colRef.ColumnRef, aliasMap)
					rightIsCol = true
				} else if funcCall, ok := n.AExpr.Rexpr.Node.(*pg_query.Node_FuncCall); ok {
					// Handle LOWER function on right side
					if len(funcCall.FuncCall.Funcname) > 0 {
						funcName := funcCall.FuncCall.Funcname[0].Node.(*pg_query.Node_String_).String_.Sval
						if strings.EqualFold(funcName, "lower") && len(funcCall.FuncCall.Args) > 0 {
							// Extract the column from LOWER(column)
							if argColRef, ok := funcCall.FuncCall.Args[0].Node.(*pg_query.Node_ColumnRef); ok {
								rightColName = extractColumnName(argColRef.ColumnRef, aliasMap)
								rightIsCol = true
								GetLogger().Debug("Detected LOWER function on column: %s", rightColName)

								// Check if this is the sharding key directly
								if getColumnNameWithoutTable(rightColName) == shardingKey {
									// If the left side is a value, we can return it directly
									if leftVal, err := extractValueFromExpr(n.AExpr.Lexpr, args); err == nil && leftVal != nil {
										return true, leftVal, nil
									}
								}
							}
						}
					}
				} else {
					rightValue, _ = extractValueFromExpr(n.AExpr.Rexpr, args)
				}

				// Store known values with both qualified and unqualified names
				if leftIsCol && !rightIsCol {
					storeKnownKey(knownKeys, leftColName, rightValue)

					// Check if this is our sharding key (with or without table prefix)
					if getColumnNameWithoutTable(leftColName) == shardingKey {
						return true, rightValue, nil
					}
				}
				if rightIsCol && !leftIsCol {
					storeKnownKey(knownKeys, rightColName, leftValue)

					// Check if this is our sharding key (with or without table prefix)
					if getColumnNameWithoutTable(rightColName) == shardingKey {
						return true, leftValue, nil
					}
				}

				// Record known keys for transitive inference
				if leftIsCol && rightIsCol {
					propagateKnownKeys(knownKeys, leftColName, rightColName)
				}

				// Check if we have found the sharding key
				if val, exists := knownKeys[shardingKey]; exists {
					return true, val, nil
				}
			} else {
				// Check if operation involves sharding key but with non-equality operator
				var leftColName, rightColName string

				// Left expression
				if colRef, ok := n.AExpr.Lexpr.Node.(*pg_query.Node_ColumnRef); ok {
					leftColName = extractColumnName(colRef.ColumnRef, aliasMap)
					// If sharding key is used with non-equality operator, return error
					if getColumnNameWithoutTable(leftColName) == shardingKey {
						return false, nil, ErrMissingShardingKey
					}
				} else if funcCall, ok := n.AExpr.Lexpr.Node.(*pg_query.Node_FuncCall); ok {
					// Check if the function contains the sharding key
					if len(funcCall.FuncCall.Args) > 0 {
						if argColRef, ok := funcCall.FuncCall.Args[0].Node.(*pg_query.Node_ColumnRef); ok {
							colName := extractColumnName(argColRef.ColumnRef, aliasMap)
							if getColumnNameWithoutTable(colName) == shardingKey {
								// Special case for LOWER function - allow it to be used with equality
								if len(funcCall.FuncCall.Funcname) > 0 {
									funcName := funcCall.FuncCall.Funcname[0].Node.(*pg_query.Node_String_).String_.Sval
									if strings.EqualFold(funcName, "lower") {
										// If the right side is also a LOWER function or a value, we can use it
										if rightFuncCall, ok := n.AExpr.Rexpr.Node.(*pg_query.Node_FuncCall); ok {
											if len(rightFuncCall.FuncCall.Funcname) > 0 {
												rightFuncName := rightFuncCall.FuncCall.Funcname[0].Node.(*pg_query.Node_String_).String_.Sval
												if strings.EqualFold(rightFuncName, "lower") && len(rightFuncCall.FuncCall.Args) > 0 {
													// Extract the value from the LOWER function argument
													rightVal, err := extractValueFromExpr(rightFuncCall.FuncCall.Args[0], args)
													if err == nil && rightVal != nil {
														return true, rightVal, nil
													}
												}
											}
										} else {
											// Try to extract a direct value from the right side
											rightVal, err := extractValueFromExpr(n.AExpr.Rexpr, args)
											if err == nil && rightVal != nil {
												return true, rightVal, nil
											}
										}
									}
								}
								return false, nil, ErrMissingShardingKey
							}
						}
					}
				}

				// Right expression
				if colRef, ok := n.AExpr.Rexpr.Node.(*pg_query.Node_ColumnRef); ok {
					rightColName = extractColumnName(colRef.ColumnRef, aliasMap)
					// If sharding key is used with non-equality operator, return error
					if getColumnNameWithoutTable(rightColName) == shardingKey {
						return false, nil, ErrMissingShardingKey
					}
				} else if funcCall, ok := n.AExpr.Rexpr.Node.(*pg_query.Node_FuncCall); ok {
					// Check if the function contains the sharding key
					if len(funcCall.FuncCall.Args) > 0 {
						if argColRef, ok := funcCall.FuncCall.Args[0].Node.(*pg_query.Node_ColumnRef); ok {
							colName := extractColumnName(argColRef.ColumnRef, aliasMap)
							if getColumnNameWithoutTable(colName) == shardingKey {
								// Special case for LOWER function - allow it to be used with equality
								if len(funcCall.FuncCall.Funcname) > 0 {
									funcName := funcCall.FuncCall.Funcname[0].Node.(*pg_query.Node_String_).String_.Sval
									if strings.EqualFold(funcName, "lower") {
										// If the left side is also a LOWER function or a value, we can use it
										if leftFuncCall, ok := n.AExpr.Lexpr.Node.(*pg_query.Node_FuncCall); ok {
											if len(leftFuncCall.FuncCall.Funcname) > 0 {
												leftFuncName := leftFuncCall.FuncCall.Funcname[0].Node.(*pg_query.Node_String_).String_.Sval
												if strings.EqualFold(leftFuncName, "lower") && len(leftFuncCall.FuncCall.Args) > 0 {
													// Extract the value from the LOWER function argument
													leftVal, err := extractValueFromExpr(leftFuncCall.FuncCall.Args[0], args)
													if err == nil && leftVal != nil {
														return true, leftVal, nil
													}
												}
											}
										} else {
											// Try to extract a direct value from the left side
											leftVal, err := extractValueFromExpr(n.AExpr.Lexpr, args)
											if err == nil && leftVal != nil {
												return true, leftVal, nil
											}
										}
									}
								}
								return false, nil, ErrMissingShardingKey
							}
						}
					}
				}
			}
		}

		// Handle LIKE and ILIKE operations
		if n.AExpr.Kind == pg_query.A_Expr_Kind_AEXPR_LIKE ||
			n.AExpr.Kind == pg_query.A_Expr_Kind_AEXPR_ILIKE {
			// Left side of the ILIKE operation
			if colRef, ok := n.AExpr.Lexpr.Node.(*pg_query.Node_ColumnRef); ok {
				colName := extractColumnName(colRef.ColumnRef, aliasMap)

				// Check if this is our sharding key
				if getColumnNameWithoutTable(colName) == shardingKey {
					// For ILIKE operations, we'll use a wildcard match
					// Extract the search term from the right side if possible
					if valueExpr, err := extractValueFromExpr(n.AExpr.Rexpr, args); err == nil && valueExpr != nil {
						return true, valueExpr, nil
					}

					// Handle concatenation scenarios like: col ILIKE '%' || param || '%'
					if opExpr, ok := n.AExpr.Rexpr.Node.(*pg_query.Node_AExpr); ok && len(opExpr.AExpr.Name) > 0 {
						if opExpr.AExpr.Name[0].Node.(*pg_query.Node_String_).String_.Sval == "||" {
							// This is a concatenation - attempt to extract the search term
							if searchTerm, err := extractSearchTermFromConcatenation(opExpr.AExpr, args); err == nil {
								return true, searchTerm, nil
							}
						}
					}
				}
			}

			// Right side of the ILIKE operation (less common, but possible)
			if colRef, ok := n.AExpr.Rexpr.Node.(*pg_query.Node_ColumnRef); ok {
				colName := extractColumnName(colRef.ColumnRef, aliasMap)

				// Check if this is our sharding key
				if getColumnNameWithoutTable(colName) == shardingKey {
					// Extract from left side
					if valueExpr, err := extractValueFromExpr(n.AExpr.Lexpr, args); err == nil && valueExpr != nil {
						return true, valueExpr, nil
					}
				}
			}
		}

	case *pg_query.Node_BoolExpr:
		for _, arg := range n.BoolExpr.Args {
			keyFound, value, err = traverseConditionForKey(shardingKey, arg, args, knownKeys, aliasMap)
			if keyFound || err != nil {
				return keyFound, value, err
			}
		}

	case *pg_query.Node_SubLink:
		// Handle subqueries in conditions
		if subselect, ok := n.SubLink.Subselect.Node.(*pg_query.Node_SelectStmt); ok {
			conditions := collectConditionsFromSelect(subselect.SelectStmt)
			for _, condition := range conditions {
				keyFound, value, err = traverseConditionForKey(shardingKey, condition, args, knownKeys, aliasMap)
				if keyFound || err != nil {
					return keyFound, value, err
				}
			}
		}
	}
	return false, nil, nil
}

func extractKeyFromPattern(pattern string) string {
	// Remove SQL wildcards (% and _) to get the core search term
	pattern = strings.ReplaceAll(pattern, "%", "")
	pattern = strings.ReplaceAll(pattern, "_", "")
	return pattern
}

func storeKnownKey(knownKeys map[string]interface{}, colName string, value interface{}) {
	knownKeys[colName] = value
	knownKeys[getColumnNameWithoutTable(colName)] = value
}

func propagateKnownKeys(knownKeys map[string]interface{}, colName1, colName2 string) {
	// If both columns have known values, do nothing
	if _, exists := knownKeys[colName1]; exists {
		if _, exists := knownKeys[colName2]; exists {
			return
		}
	}

	// If one column has a known value, assign it to the other
	if val, exists := knownKeys[colName1]; exists {
		storeKnownKey(knownKeys, colName2, val)
	} else if val, exists := knownKeys[colName2]; exists {
		storeKnownKey(knownKeys, colName1, val)
	} else {
		// Neither column has a known value; store their equivalence
		addColumnEquivalence(colName1, colName2)
	}
}

type EquivalenceClass struct {
	columns map[string]struct{}
}

var equivalenceClasses []EquivalenceClass

func addColumnEquivalence(colName1, colName2 string) {
	// Check if either column is already in an equivalence class
	var class *EquivalenceClass
	for i, eqClass := range equivalenceClasses {
		if _, exists := eqClass.columns[colName1]; exists {
			class = &equivalenceClasses[i]
			class.columns[colName2] = struct{}{}
			return
		}
		if _, exists := eqClass.columns[colName2]; exists {
			class = &equivalenceClasses[i]
			class.columns[colName1] = struct{}{}
			return
		}
	}
	// Neither column is in an equivalence class; create a new one
	newClass := EquivalenceClass{columns: map[string]struct{}{
		colName1: {},
		colName2: {},
	}}
	equivalenceClasses = append(equivalenceClasses, newClass)
}

func getColumnNameWithoutTable(columnName string) string {
	if idx := strings.LastIndex(columnName, "."); idx != -1 {
		return columnName[idx+1:]
	}
	return columnName
}

func extractColumnName(colRef *pg_query.ColumnRef, aliasMap map[string]string) string {
	var parts []string
	for i, field := range colRef.Fields {
		if stringNode, ok := field.Node.(*pg_query.Node_String_); ok {
			val := stringNode.String_.Sval
			// Resolve alias to table name for the first field (table alias)
			if i == 0 && len(colRef.Fields) > 1 {
				if realTable, exists := aliasMap[val]; exists {
					val = realTable
				}
			}
			parts = append(parts, val)
		}
	}
	return strings.Join(parts, ".")
}

func getSuffix(value any, id int64, keyFound bool, r Config) (suffix string, err error) {
	if keyFound && value != nil {
		// Use the sharding key value if available
		suffix, err = r.ShardingAlgorithm(value)
		if err != nil {
			GetLogger().Error("Error in ShardingAlgorithm: %v", err)

			// Fall back to ID-based routing if available
			if id != 0 && r.ShardingAlgorithmByPrimaryKey != nil {
				suffix = r.ShardingAlgorithmByPrimaryKey(id)
				GetLogger().Debug("Falling back to ID-based routing: %d -> %s", id, suffix)
				return suffix, nil
			}

			return "", err
		}
		//GetLogger().Debug("Sharding key value: %v, Suffix: %s", value, suffix)
	} else if id != 0 {
		// Use ID-based routing when no value or value is nil
		if r.ShardingAlgorithmByPrimaryKey == nil {
			err = fmt.Errorf("there is no sharding key and ShardingAlgorithmByPrimaryKey is not configured")
			GetLogger().Error("Error: %v", err)
			return
		}
		suffix = r.ShardingAlgorithmByPrimaryKey(id)
		GetLogger().Debug("Sharding by primary key: %d, Suffix: %s", id, suffix)
	} else {
		err = ErrMissingShardingKey
		return
	}
	return
}

// Helper function to extract search term from concatenation operators
func extractSearchTermFromConcatenation(aExpr *pg_query.A_Expr, args []interface{}) (string, error) {
	// Handle simple cases like: '%' || $1 || '%'
	if len(aExpr.Name) > 0 && aExpr.Name[0].Node.(*pg_query.Node_String_).String_.Sval == "||" {
		// First, check if the left side is a parameter
		lValue, lErr := extractValueFromExpr(aExpr.Lexpr, args)
		if lErr == nil && lValue != nil && lValue != "%" && lValue != "_" {
			// Found a non-wildcard parameter on the left side
			return fmt.Sprintf("%v", lValue), nil
		}

		// Next, check if the right side is a parameter
		rValue, rErr := extractValueFromExpr(aExpr.Rexpr, args)
		if rErr == nil && rValue != nil && rValue != "%" && rValue != "_" {
			// Found a non-wildcard parameter on the right side
			return fmt.Sprintf("%v", rValue), nil
		}

		// If right side is another concatenation, recursively check it
		if rExpr, ok := aExpr.Rexpr.Node.(*pg_query.Node_AExpr); ok {
			if len(rExpr.AExpr.Name) > 0 && rExpr.AExpr.Name[0].Node.(*pg_query.Node_String_).String_.Sval == "||" {
				// Recursively check the right expression
				if searchTerm, err := extractSearchTermFromConcatenation(rExpr.AExpr, args); err == nil {
					return searchTerm, nil
				}
			}
		}

		// If left side is another concatenation, recursively check it
		if lExpr, ok := aExpr.Lexpr.Node.(*pg_query.Node_AExpr); ok {
			if len(lExpr.AExpr.Name) > 0 && lExpr.AExpr.Name[0].Node.(*pg_query.Node_String_).String_.Sval == "||" {
				// Recursively check the left expression
				if searchTerm, err := extractSearchTermFromConcatenation(lExpr.AExpr, args); err == nil {
					return searchTerm, nil
				}
			}
		}
	}

	// Handle more complex cases through recursive traversal
	searchParts := extractAllValuesFromConcatenation(aExpr, args)
	if len(searchParts) > 0 {
		return strings.Join(searchParts, ""), nil
	}

	return "", fmt.Errorf("could not extract search term from concatenation")
}

// Recursively extracts all string values from a concatenation expression tree
func extractAllValuesFromConcatenation(aExpr *pg_query.A_Expr, args []interface{}) []string {
	if aExpr == nil || len(aExpr.Name) == 0 {
		return nil
	}

	opName := aExpr.Name[0].Node.(*pg_query.Node_String_).String_.Sval
	if opName != "||" {
		return nil
	}

	var parts []string

	// Process left side
	if leftVal, err := extractValueFromExpr(aExpr.Lexpr, args); err == nil && leftVal != nil {
		if strVal, ok := leftVal.(string); ok {
			// Skip wildcards in pattern matching for the purpose of sharding
			if strVal != "%" && strVal != "_" {
				parts = append(parts, strVal)
			}
		} else {
			// If it's not a string, convert it to string and add it
			parts = append(parts, fmt.Sprintf("%v", leftVal))
		}
	} else if leftExpr, ok := aExpr.Lexpr.Node.(*pg_query.Node_AExpr); ok {
		// Recursive concatenation on left side
		parts = append(parts, extractAllValuesFromConcatenation(leftExpr.AExpr, args)...)
	}

	// Process right side
	if rightVal, err := extractValueFromExpr(aExpr.Rexpr, args); err == nil && rightVal != nil {
		if strVal, ok := rightVal.(string); ok {
			// Skip wildcards in pattern matching for the purpose of sharding
			if strVal != "%" && strVal != "_" {
				parts = append(parts, strVal)
			}
		} else {
			// If it's not a string, convert it to string and add it
			parts = append(parts, fmt.Sprintf("%v", rightVal))
		}
	} else if rightExpr, ok := aExpr.Rexpr.Node.(*pg_query.Node_AExpr); ok {
		// Recursive concatenation on right side
		parts = append(parts, extractAllValuesFromConcatenation(rightExpr.AExpr, args)...)
	}

	return parts
}

func collectAliasesFromJoin(joinExpr *pg_query.JoinExpr) map[string]string {
	aliasMap := make(map[string]string)
	if joinExpr.Larg != nil {
		mergeMaps(aliasMap, collectAliasesFromExpr(joinExpr.Larg))
	}
	if joinExpr.Rarg != nil {
		mergeMaps(aliasMap, collectAliasesFromExpr(joinExpr.Rarg))
	}
	return aliasMap
}

func collectAliasesFromExpr(expr *pg_query.Node) map[string]string {
	switch n := expr.Node.(type) {
	case *pg_query.Node_RangeVar:
		tableName := n.RangeVar.Relname
		var alias string
		if n.RangeVar.Alias != nil {
			alias = n.RangeVar.Alias.Aliasname
			return map[string]string{alias: tableName}
		}
		return map[string]string{tableName: tableName}
	case *pg_query.Node_JoinExpr:
		return collectAliasesFromJoin(n.JoinExpr)
	}
	return nil
}

func mergeMaps(dest, src map[string]string) {
	for k, v := range src {
		dest[k] = v
	}
}

func isSystemQuery(query string) bool {
	systemTables := []string{
		"information_schema",
		"pg_catalog",
		"CREATE TABLE",
		"CREATE INDEX",
		"CREATE SEQUENCE",
		"ALTER TABLE",
		"DROP TABLE",
		"DROP INDEX",
		// todo: Add other system schemas if necessary
	}
	upperQuery := strings.ToUpper(strings.TrimSpace(query))
	for _, sysTable := range systemTables {
		if strings.Contains(upperQuery, strings.ToUpper(sysTable)) {
			return true
		}
	}
	return false
}

// Function to generate all suffixes for list partitioning
func defaultListSuffixes(config *Config) func() []string {
	return func() []string {
		// Find the maximum partition number
		maxPartition := -1
		for _, partNum := range config.ListValues {
			if partNum > maxPartition {
				maxPartition = partNum
			}
		}

		// Include default partition if specified
		if config.DefaultPartition > maxPartition {
			maxPartition = config.DefaultPartition
		}

		// Generate all suffixes
		suffixes := make([]string, maxPartition+1)
		for i := 0; i <= maxPartition; i++ {
			suffixes[i] = fmt.Sprintf(config.tableFormat, i)
		}

		return suffixes
	}
}

// Function to create a list partitioning algorithm based on config
func defaultListAlgorithm(config *Config) func(value any) (string, error) {
	return func(value any) (string, error) {
		// For nil or zero value, use default partition if specified
		if value == nil {
			if config.DefaultPartition >= 0 {
				return fmt.Sprintf(config.tableFormat, config.DefaultPartition), nil
			}
			return "", fmt.Errorf("nil value not found in partition list")
		}

		// Convert value to string for lookup in ListValues
		var strValue string
		switch v := value.(type) {
		case string:
			strValue = v
		case []byte:
			strValue = string(v)
		case int:
			strValue = fmt.Sprintf("%d", v)
		case int64:
			strValue = fmt.Sprintf("%d", v)
		case int32:
			strValue = fmt.Sprintf("%d", v)
		case int16:
			strValue = fmt.Sprintf("%d", v)
		case int8:
			strValue = fmt.Sprintf("%d", v)
		case uint:
			strValue = fmt.Sprintf("%d", v)
		case uint64:
			strValue = fmt.Sprintf("%d", v)
		case uint32:
			strValue = fmt.Sprintf("%d", v)
		case uint16:
			strValue = fmt.Sprintf("%d", v)
		case uint8:
			strValue = fmt.Sprintf("%d", v)
		case bool:
			strValue = fmt.Sprintf("%t", v)
		default:
			// Try using reflection to get string representation
			if reflect.ValueOf(v).Kind() == reflect.String {
				strValue = reflect.ValueOf(v).String()
			} else {
				strValue = fmt.Sprintf("%v", v)
			}
		}

		// Debug log the exact value being looked up
		GetLogger().Debug("Looking up partition for value: '%s' in ListValues map: %v", strValue, config.ListValues)

		// Look up partition number in ListValues map
		partitionNum, exists := config.ListValues[strValue]
		if !exists {
			// Try looking up with different case variations if first attempt fails
			for key, val := range config.ListValues {
				if strings.EqualFold(key, strValue) {
					partitionNum = val
					exists = true
					GetLogger().Debug("Found partition %d for case-insensitive value '%s' matching key '%s'", partitionNum, strValue, key)
					break
				}
			}

			// If still not found, use default partition if specified, otherwise error
			if !exists {
				if config.DefaultPartition >= 0 {
					partitionNum = config.DefaultPartition
					GetLogger().Debug("Using default partition %d for value '%s'", partitionNum, strValue)
				} else {
					return "", fmt.Errorf("value '%s' not found in partition list", strValue)
				}
			}
		} else {
			GetLogger().Debug("Found partition %d for value '%s'", partitionNum, strValue)
		}

		return fmt.Sprintf(config.tableFormat, partitionNum), nil
	}
}

// Function to create a default hash partitioning algorithm based on config
func defaultHashAlgorithm(config *Config) func(value any) (string, error) {
	return func(value any) (string, error) {
		id := 0
		switch v := value.(type) {
		case int:
			id = v
		case int64:
			id = int(v)
		case int32:
			id = int(v)
		case int16:
			id = int(v)
		case int8:
			id = int(v)
		case uint:
			id = int(v)
		case uint64:
			id = int(v)
		case uint32:
			id = int(v)
		case uint16:
			id = int(v)
		case uint8:
			id = int(v)
		case float64:
			id = int(v)
		case float32:
			id = int(v)
		case string:
			var err error
			id, err = strconv.Atoi(v)
			if err != nil {
				id = int(crc32.ChecksumIEEE([]byte(v)))
			}
		default:
			return "", fmt.Errorf("default algorithm only supports integer and string types")
		}

		return fmt.Sprintf(config.tableFormat, id%int(config.NumberOfShards)), nil
	}
}

// Helper function to check if a condition contains a LOWER function
func containsLowerFunction(node *pg_query.Node) bool {
	if node == nil {
		return false
	}

	switch n := node.Node.(type) {
	case *pg_query.Node_AExpr:
		// Check both sides of the expression
		if containsLowerFunctionInExpr(n.AExpr.Lexpr) || containsLowerFunctionInExpr(n.AExpr.Rexpr) {
			return true
		}

	case *pg_query.Node_BoolExpr:
		// Check all arguments of the boolean expression
		for _, arg := range n.BoolExpr.Args {
			if containsLowerFunction(arg) {
				return true
			}
		}
	}
	return false
}

// Helper function to check if an expression contains a LOWER function
func containsLowerFunctionInExpr(expr *pg_query.Node) bool {
	if expr == nil {
		return false
	}

	switch n := expr.Node.(type) {
	case *pg_query.Node_FuncCall:
		// Check if this is a LOWER function
		if len(n.FuncCall.Funcname) > 0 {
			if stringNode, ok := n.FuncCall.Funcname[0].Node.(*pg_query.Node_String_); ok {
				if strings.EqualFold(stringNode.String_.Sval, "lower") {
					return true
				}
			}
		}
	case *pg_query.Node_AExpr:
		// Recursively check both sides of the expression
		return containsLowerFunctionInExpr(n.AExpr.Lexpr) || containsLowerFunctionInExpr(n.AExpr.Rexpr)
	case *pg_query.Node_BoolExpr:
		// Recursively check all arguments
		for _, arg := range n.BoolExpr.Args {
			if containsLowerFunctionInExpr(arg) {
				return true
			}
		}
	}
	return false
}

// handleMultiShardInsert processes batch inserts with records belonging to different shards.
// It detects this situation and splits the batch into per-shard operations to avoid
// the "can not insert different suffix table in one query" error.
func (s *Sharding) handleMultiShardInsert(db *gorm.DB) error {
	// Extract table name from the statement
	tableName := db.Statement.Table

	// Check if this table is configured for sharding
	config, exists := s.configs[tableName]
	if !exists {
		// Not a sharded table, proceed normally
		return nil
	}

	// Get the value being inserted
	reflectValue := db.Statement.ReflectValue
	if reflectValue.Kind() != reflect.Slice {
		// Not a batch operation, proceed normally
		return nil
	}

	// If we have only 0 or 1 record, proceed normally
	if reflectValue.Len() <= 1 {
		return nil
	}

	// Enhanced logging
	GetLogger().Debug("Processing batch insert with %d records for table %s", reflectValue.Len(), tableName)

	// Group records by sharding key value to ensure complete separation
	recordsByShardingKey := make(map[interface{}][]interface{})
	shardsBySuffix := make(map[string][]interface{}) // For logging purposes

	// Process each record to determine its shard
	for i := 0; i < reflectValue.Len(); i++ {
		// Get the record
		record := reflectValue.Index(i).Interface()

		// Extract sharding key value
		var keyValue interface{}
		recordValue := reflect.ValueOf(record)
		if recordValue.Kind() == reflect.Ptr {
			recordValue = recordValue.Elem()
		}

		// Handle struct records
		if recordValue.Kind() == reflect.Struct {
			for j := 0; j < recordValue.NumField(); j++ {
				fieldName := recordValue.Type().Field(j).Name
				if strings.EqualFold(fieldName, config.ShardingKey) {
					keyValue = recordValue.Field(j).Interface()
					break
				}
			}
		} else if recordValue.Kind() == reflect.Map {
			// Handle map records
			for _, key := range recordValue.MapKeys() {
				if key.String() == config.ShardingKey {
					keyValue = recordValue.MapIndex(key).Interface()
					break
				}
			}
		}

		if keyValue == nil {
			// If we can't find the sharding key, proceed with normal processing
			// It might fail later, but that's the expected behavior
			GetLogger().Debug("Could not extract sharding key %s from record %d, proceeding with normal processing", config.ShardingKey, i)
			return nil
		}

		// Get shard suffix for this record
		suffix, err := getSuffix(keyValue, 0, true, config)
		if err != nil {
			return err
		}

		// Add the sharding key to the logging map
		keysForSuffix := shardsBySuffix[suffix]
		keyExists := false
		for _, existingKey := range keysForSuffix {
			if existingKey == keyValue {
				keyExists = true
				break
			}
		}
		if !keyExists {
			shardsBySuffix[suffix] = append(shardsBySuffix[suffix], keyValue)
		}

		// Add record to the appropriate group BY SHARDING KEY VALUE
		// This is the key change - group by sharding key value, not by suffix
		recordsByShardingKey[keyValue] = append(recordsByShardingKey[keyValue], record)
	}

	// If all records share the same sharding key, proceed normally
	if len(recordsByShardingKey) <= 1 {
		GetLogger().Debug("All %d records have the same sharding key, proceeding with normal processing", reflectValue.Len())
		return nil
	}

	// Enhanced logging to show what sharding keys map to which suffixes
	for suffix, keys := range shardsBySuffix {
		GetLogger().Debug("Shard suffix %s will receive records with sharding keys: %v", suffix, keys)
	}

	// Multiple sharding keys detected - we need to split the operation
	GetLogger().Debug("Detected batch insert with records for %d different sharding key values", len(recordsByShardingKey))

	// Process each group separately
	for keyValue, records := range recordsByShardingKey {
		// Create a new session with the same settings
		session := db.Session(&gorm.Session{})

		// Create a new slice with the same type as the original
		newSlice := reflect.MakeSlice(reflectValue.Type(), 0, len(records))
		for _, record := range records {
			newSlice = reflect.Append(newSlice, reflect.ValueOf(record))
		}

		// Convert to interface
		sliceInterface := newSlice.Interface()

		// Get the suffix for this key value (for logging)
		suffix, _ := getSuffix(keyValue, 0, true, config)

		// Create records for this shard
		GetLogger().Debug("Processing batch of %d records for sharding key %v (suffix %s)",
			len(records), keyValue, suffix)

		if err := session.Create(sliceInterface).Error; err != nil {
			return fmt.Errorf("failed to insert records for sharding key %v (suffix %s): %w",
				keyValue, suffix, err)
		}
	}

	// Skip the default processing since we've handled it
	db.SkipDefaultTransaction = true
	return nil
}

// Close performs cleanup when shutting down the sharding system.
func (s *Sharding) Close() {
	// Stop the health checker if it's running
	if s.healthChecker != nil {
		s.healthChecker.Stop()
	}

	// Other cleanup tasks can be added here in the future
}
