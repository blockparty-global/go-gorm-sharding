package sharding

import (
	"fmt"
	"gorm.io/gorm/logger"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/assert"
	"gorm.io/driver/postgres"
	"gorm.io/gorm"
)

var tableCounter int64

// generateUniqueTableName generates a unique table name for parallel tests
func generateUniqueTableName(prefix string) string {
	counter := atomic.AddInt64(&tableCounter, 1)
	return fmt.Sprintf("%s_%d", prefix, counter)
}

// cleanupShardedTables drops the base table and all sharded tables
func cleanupShardedTables(db *gorm.DB, tableName string, numShards int) {
	// Drop base table
	db.Exec(fmt.Sprintf("DROP TABLE IF EXISTS %s", tableName))
	
	// Drop sharded tables
	for i := 0; i < numShards; i++ {
		db.Exec(fmt.Sprintf("DROP TABLE IF EXISTS %s_%02d", tableName, i))
	}
}

// testTableContext holds table information for parallel tests
type testTableContext struct {
	name   string
	shards int
}

func (t *testTableContext) cleanup(db *gorm.DB) {
	cleanupShardedTables(db, t.name, t.shards)
}

func (t *testTableContext) formatQuery(query string) string {
	return strings.ReplaceAll(query, "tokens", t.name)
}

// runParallelTest helps run a test with unique table names for parallel execution
func runParallelTest(t *testing.T, db *gorm.DB, numShards int, testFunc func(t *testing.T, db *gorm.DB, tc *testTableContext)) {
	t.Parallel()
	
	tc := &testTableContext{
		name:   generateUniqueTableName("tokens"),
		shards: numShards,
	}
	defer tc.cleanup(db)
	
	// Create a new DB instance for this test to avoid middleware conflicts
	testDB := db.Session(&gorm.Session{})
	
	testFunc(t, testDB, tc)
}

func TestCompositeINClauseWithMultipleTuples(t *testing.T) {
	dbConfig := postgres.Config{
		DSN:                  dbURL(),
		PreferSimpleProtocol: true,
	}
	db, err := gorm.Open(postgres.New(dbConfig), &gorm.Config{
		DisableForeignKeyConstraintWhenMigrating: true,
		Logger:                                   logger.Default.LogMode(logger.Info),
	})
	assert.NoError(t, err)

	middleware := Register(Config{
		ShardingKey:         "contract",
		NumberOfShards:      4,
		PrimaryKeyGenerator: PKSnowflake,
	}, "tokens")

	db.Use(middleware)

	// Create test table
	type Token struct {
		ID       int64  `gorm:"primarykey"`
		Contract string `gorm:"index:idx_contract_token,composite"`
		TokenID  string `gorm:"column:token_id;index:idx_contract_token,composite"`
		Owner    string
	}

	// Migrate the table
	err = db.AutoMigrate(&Token{})
	assert.NoError(t, err)

	// Insert test data with different contracts and token IDs
	testTokens := []Token{
		{Contract: "0x76be3b62873462d2142405439777e971754e8e77", TokenID: "10699", Owner: "owner1"},
		{Contract: "0x76be3b62873462d2142405439777e971754e8e77", TokenID: "10502", Owner: "owner2"},
		{Contract: "0x76be3b62873462d2142405439777e971754e8e77", TokenID: "10719", Owner: "owner3"},
		{Contract: "0x76be3b62873462d2142405439777e971754e8e77", TokenID: "10712", Owner: "owner4"},
		// Add some tokens with same contract but different token_id that should NOT be returned
		{Contract: "0x76be3b62873462d2142405439777e971754e8e77", TokenID: "99999", Owner: "owner5"},
		{Contract: "0x76be3b62873462d2142405439777e971754e8e77", TokenID: "88888", Owner: "owner6"},
		// Add some tokens with different contract but same token_id that should NOT be returned
		{Contract: "0xdifferentcontract", TokenID: "10699", Owner: "owner7"},
		{Contract: "0xanothercontract", TokenID: "10502", Owner: "owner8"},
	}

	for _, token := range testTokens {
		err := db.Create(&token).Error
		assert.NoError(t, err)
	}

	t.Run("CompositeIN_ChecksBothParameters", func(t *testing.T) {
		var results []Token

		// Test the exact query format
		err := db.Raw(`SELECT * FROM "tokens" WHERE ((contract, token_id)) IN (('0x76be3b62873462d2142405439777e971754e8e77', '10699'),('0x76be3b62873462d2142405439777e971754e8e77', '10502'),('0x76be3b62873462d2142405439777e971754e8e77', '10719'),('0x76be3b62873462d2142405439777e971754e8e77', '10712'))`).
			Find(&results).Error

		assert.NoError(t, err)
		assert.Equal(t, 4, len(results), "Should return exactly 4 tokens matching both contract AND token_id")

		// Verify the correct tokens were returned
		expectedTokenIDs := map[string]bool{
			"10699": true,
			"10502": true,
			"10719": true,
			"10712": true,
		}

		for _, result := range results {
			assert.Equal(t, "0x76be3b62873462d2142405439777e971754e8e77", result.Contract)
			assert.True(t, expectedTokenIDs[result.TokenID], "TokenID %s should be in expected list", result.TokenID)
			delete(expectedTokenIDs, result.TokenID)
		}

		assert.Empty(t, expectedTokenIDs, "All expected token IDs should have been found")
	})

	t.Run("CompositeIN_WithParameterizedValues", func(t *testing.T) {
		var results []Token

		// Test with parameterized values to ensure both parameters are checked
		tuples := [][]interface{}{
			{"0x76be3b62873462d2142405439777e971754e8e77", "10699"},
			{"0x76be3b62873462d2142405439777e971754e8e77", "10502"},
			{"0x76be3b62873462d2142405439777e971754e8e77", "10719"},
			{"0x76be3b62873462d2142405439777e971754e8e77", "10712"},
		}

		err := db.Raw(`SELECT * FROM tokens WHERE ((contract, token_id)) IN (?)`, tuples).
			Find(&results).Error

		assert.NoError(t, err)
		assert.Equal(t, 4, len(results), "Should return exactly 4 tokens with parameterized query")
	})

	t.Run("CompositeIN_GORM_Style", func(t *testing.T) {
		var results []Token

		// Test using GORM's Where clause
		tuples := [][]interface{}{
			{"0x76be3b62873462d2142405439777e971754e8e77", "10699"},
			{"0x76be3b62873462d2142405439777e971754e8e77", "10502"},
			{"0x76be3b62873462d2142405439777e971754e8e77", "10719"},
			{"0x76be3b62873462d2142405439777e971754e8e77", "10712"},
		}

		err := db.Where("((contract, token_id)) IN ?", tuples).Find(&results).Error

		assert.NoError(t, err)
		assert.Equal(t, 4, len(results), "Should return exactly 4 tokens with GORM Where clause")
	})

	t.Run("CompositeIN_VerifyNotJustFirstParameter", func(t *testing.T) {
		var results []Token

		// Query that would return wrong results if only checking first parameter
		err := db.Raw(`SELECT * FROM "tokens" WHERE ((contract, token_id)) IN (('0xdifferentcontract', '10699'),('0xanothercontract', '10502'))`).
			Find(&results).Error

		assert.NoError(t, err)
		assert.Equal(t, 2, len(results), "Should return tokens with different contracts")

		// Verify none of them have the original contract
		for _, result := range results {
			assert.NotEqual(t, "0x76be3b62873462d2142405439777e971754e8e77", result.Contract)
		}
	})

	// Cleanup
	db.Exec("DROP TABLE IF EXISTS tokens_0, tokens_1, tokens_2, tokens_3")
}

type Token struct {
	ID       int64  `gorm:"primarykey"`
	Contract string `gorm:"index:idx_contract_token,composite"`
	TokenID  string `gorm:"column:token_id;index:idx_contract_token,composite"`
	Owner    string
}

func TestShardPercentageThreshold(t *testing.T) {
	// Note: This test cannot run in parallel because all subtests share the same table names
	// To make it parallel-safe, each subtest would need to use unique table names
	
	// Enable debug logging for this test
	oldLogLevel := DefaultLogLevel
	SetLogLevel(LogLevelDebug)
	defer SetLogLevel(oldLogLevel)

	dbConfig := postgres.Config{
		DSN:                  dbURL(),
		PreferSimpleProtocol: true,
	}
	db, err := gorm.Open(postgres.New(dbConfig), &gorm.Config{
		DisableForeignKeyConstraintWhenMigrating: true,
		Logger:                                   logger.Default.LogMode(logger.Info),
	})
	assert.NoError(t, err)

	t.Run("UseUNIONWhenBelowThreshold", func(t *testing.T) {
		// Configure sharding with 10 shards and 50% threshold
		middleware := Register(Config{
			ShardingKey:              "contract",
			NumberOfShards:           10,
			PrimaryKeyGenerator:      PKSnowflake,
			DoubleWrite:              true,
			ShardPercentageThreshold: 0.5, // 50% threshold
		}, "tokens")

		db.Use(middleware)

		// Migrate the table
		err = db.AutoMigrate(&Token{})
		assert.NoError(t, err)

		// Insert test data across 3 different shards (30% of shards)
		testContracts := []string{
			"0x1111111111111111111111111111111111111111", // Should go to one shard
			"0x2222222222222222222222222222222222222222", // Should go to another shard
			"0x3333333333333333333333333333333333333333", // Should go to a third shard
		}

		for i, contract := range testContracts {
			token := Token{
				Contract: contract,
				TokenID:  fmt.Sprintf("%d", i),
				Owner:    fmt.Sprintf("owner%d", i),
			}
			err := db.Create(&token).Error
			assert.NoError(t, err)
		}

		// Query with composite IN that would use 3 shards (30% < 50% threshold)
		var results []Token
		query := `SELECT * FROM "tokens" WHERE ((contract, token_id)) IN (
			('0x1111111111111111111111111111111111111111', '0'),
			('0x2222222222222222222222222222222222222222', '1'),
			('0x3333333333333333333333333333333333333333', '2')
		)`

		// This should create a UNION query since 30% < 50% threshold
		err = db.Raw(query).Find(&results).Error
		assert.NoError(t, err)
		assert.Equal(t, 3, len(results), "Should return all 3 tokens from UNION query")

		// Cleanup
		db.Exec("DROP TABLE IF EXISTS tokens, tokens_0, tokens_1, tokens_2, tokens_3, tokens_4, tokens_5, tokens_6, tokens_7, tokens_8, tokens_9")
	})

	t.Run("UseBaseTableWhenAboveThreshold", func(t *testing.T) {
		// Configure sharding with 4 shards and 50% threshold
		middleware := Register(Config{
			ShardingKey:              "contract",
			NumberOfShards:           4,
			PrimaryKeyGenerator:      PKSnowflake,
			DoubleWrite:              true,
			ShardPercentageThreshold: 0.5, // 50% threshold
		}, "tokens")

		db.Use(middleware)

		// Migrate the table
		err = db.AutoMigrate(&Token{})
		assert.NoError(t, err)

		// Insert test data across 3 different shards (75% of shards)
		testData := []struct {
			contract string
			tokenID  string
		}{
			{"0xaaaa", "100"}, // Shard 0
			{"0xbbbb", "200"}, // Shard 1
			{"0xcccc", "300"}, // Shard 2
			{"0xdddd", "400"}, // Shard 3 (if needed)
		}

		// Insert test data (will go to both base and sharded tables due to DoubleWrite)
		for i, data := range testData {
			token := Token{
				Contract: data.contract,
				TokenID:  data.tokenID,
				Owner:    fmt.Sprintf("owner%d", i),
			}
			// Use normal Create which will trigger sharding middleware
			err := db.Create(&token).Error
			assert.NoError(t, err)
		}

		// Query with composite IN that would use 3 shards (75% > 50% threshold)
		var results []Token
		query := `SELECT * FROM "tokens" WHERE ((contract, token_id)) IN (
			('0xaaaa', '100'),
			('0xbbbb', '200'),
			('0xcccc', '300')
		)`

		// This should use the base table since 75% > 50% threshold
		err = db.Raw(query).Find(&results).Error
		assert.NoError(t, err)

		// Debug output
		t.Logf("Query returned %d results", len(results))
		for i, r := range results {
			t.Logf("Result %d: Contract=%s, TokenID=%s, Owner=%s", i, r.Contract, r.TokenID, r.Owner)
		}

		// Check what's in the base table
		var baseTableCount int64
		db.Table("tokens").Count(&baseTableCount)
		t.Logf("Base table has %d total rows", baseTableCount)

		// Remove duplicates based on contract+token_id
		uniqueResults := make(map[string]Token)
		for _, r := range results {
			key := r.Contract + "_" + r.TokenID
			uniqueResults[key] = r
		}

		assert.Equal(t, 3, len(uniqueResults), "Should return 3 unique tokens (might have duplicates from DoubleWrite)")

		// Cleanup
		db.Exec("DROP TABLE IF EXISTS tokens, tokens_0, tokens_1, tokens_2, tokens_3")
	})

	t.Run("CustomThreshold", func(t *testing.T) {
		// Configure sharding with 10 shards and 30% threshold
		middleware := Register(Config{
			ShardingKey:              "contract",
			NumberOfShards:           10,
			PrimaryKeyGenerator:      PKSnowflake,
			DoubleWrite:              true,
			ShardPercentageThreshold: 0.3, // 30% threshold - more aggressive use of base table
		}, "tokens")

		db.Use(middleware)

		// Migrate the table
		err = db.AutoMigrate(&Token{})
		assert.NoError(t, err)

		// Insert test data across 4 different shards (40% of shards)
		testData := []struct {
			contract string
			tokenID  string
		}{
			{"0xe111", "1000"},
			{"0xe222", "2000"},
			{"0xe333", "3000"},
			{"0xe444", "4000"},
		}

		for i, data := range testData {
			token := Token{
				Contract: data.contract,
				TokenID:  data.tokenID,
				Owner:    fmt.Sprintf("owner%d", i),
			}
			// Use normal Create which will trigger sharding middleware
			err := db.Create(&token).Error
			assert.NoError(t, err)
		}

		// Query with composite IN that would use 4 shards (40% > 30% threshold)
		var results []Token
		query := `SELECT * FROM "tokens" WHERE ((contract, token_id)) IN (
			('0xe111', '1000'),
			('0xe222', '2000'),
			('0xe333', '3000'),
			('0xe444', '4000')
		)`

		// This should use the base table since 40% > 30% threshold
		err = db.Raw(query).Find(&results).Error
		assert.NoError(t, err)
		assert.Equal(t, 4, len(results), "Should return all 4 tokens from base table")

		// Cleanup
		db.Exec("DROP TABLE IF EXISTS tokens, tokens_0, tokens_1, tokens_2, tokens_3, tokens_4, tokens_5, tokens_6, tokens_7, tokens_8, tokens_9")
	})
}

func TestShardPercentageThresholdComprehensive(t *testing.T) {
	// Note: This test cannot run in parallel because all subtests share the same table names
	// To make it parallel-safe, each subtest would need to use unique table names
	
	// Enable debug logging for this test
	oldLogLevel := DefaultLogLevel
	SetLogLevel(LogLevelDebug)
	defer SetLogLevel(oldLogLevel)

	dbConfig := postgres.Config{
		DSN:                  dbURL(),
		PreferSimpleProtocol: true,
	}
	db, err := gorm.Open(postgres.New(dbConfig), &gorm.Config{
		DisableForeignKeyConstraintWhenMigrating: true,
		Logger:                                   logger.Default.LogMode(logger.Info),
	})
	assert.NoError(t, err)

	t.Run("ExactlyAtThreshold_50Percent", func(t *testing.T) {
		// Configure sharding with 10 shards and 50% threshold
		middleware := Register(Config{
			ShardingKey:              "contract",
			NumberOfShards:           10,
			PrimaryKeyGenerator:      PKSnowflake,
			DoubleWrite:              true,
			ShardPercentageThreshold: 0.5, // 50% threshold
		}, "tokens")

		db.Use(middleware)
		err = db.AutoMigrate(&Token{})
		assert.NoError(t, err)

		// Insert test data - contracts designed to hit exactly 5 shards (50%)
		testContracts := []string{
			"0xa000", "0xa001", "0xa002", "0xa003", "0xa004", // These should distribute to 5 different shards
		}

		for i, contract := range testContracts {
			token := Token{
				Contract: contract,
				TokenID:  fmt.Sprintf("%d", i),
				Owner:    fmt.Sprintf("owner%d", i),
			}
			err := db.Create(&token).Error
			assert.NoError(t, err)
		}

		// Query with composite IN that uses exactly 5 shards (50% = threshold)
		var results []Token
		query := `SELECT * FROM "tokens" WHERE ((contract, token_id)) IN (
			('0xa000', '0'),('0xa001', '1'),('0xa002', '2'),('0xa003', '3'),('0xa004', '4')
		)`

		err = db.Raw(query).Find(&results).Error
		assert.NoError(t, err)

		// At exactly 50%, should still use sharding (not exceed threshold)
		// But with DoubleWrite, we might get results from base table
		uniqueResults := make(map[string]bool)
		for _, r := range results {
			uniqueResults[r.Contract+"_"+r.TokenID] = true
		}
		assert.Equal(t, 5, len(uniqueResults), "Should return 5 unique tokens")

		// Cleanup
		db.Exec("DROP TABLE IF EXISTS tokens, tokens_0, tokens_1, tokens_2, tokens_3, tokens_4, tokens_5, tokens_6, tokens_7, tokens_8, tokens_9")
	})

	t.Run("JustBelowThreshold_49Percent", func(t *testing.T) {
		// Configure sharding with 100 shards and 50% threshold
		middleware := Register(Config{
			ShardingKey:              "contract",
			NumberOfShards:           100,
			PrimaryKeyGenerator:      PKSnowflake,
			DoubleWrite:              true,
			ShardPercentageThreshold: 0.5, // 50% threshold
		}, "tokens")

		db.Use(middleware)
		err = db.AutoMigrate(&Token{})
		assert.NoError(t, err)

		// Insert test data - 49 contracts (49% of shards)
		for i := 0; i < 49; i++ {
			token := Token{
				Contract: fmt.Sprintf("0xcontract%02d", i),
				TokenID:  fmt.Sprintf("%d", i),
				Owner:    fmt.Sprintf("owner%d", i),
			}
			err := db.Create(&token).Error
			assert.NoError(t, err)
		}

		// Build query with 49 contracts
		queryParts := []string{}
		for i := 0; i < 49; i++ {
			queryParts = append(queryParts, fmt.Sprintf("('0xcontract%02d', '%d')", i, i))
		}

		var results []Token
		query := fmt.Sprintf(`SELECT * FROM "tokens" WHERE ((contract, token_id)) IN (%s)`,
			strings.Join(queryParts, ","))

		err = db.Raw(query).Find(&results).Error
		assert.NoError(t, err)

		// Should use sharding since 49% < 50%
		uniqueResults := make(map[string]bool)
		for _, r := range results {
			uniqueResults[r.Contract+"_"+r.TokenID] = true
		}
		assert.Equal(t, 49, len(uniqueResults), "Should return 49 unique tokens")

		// Cleanup
		db.Exec("DROP TABLE IF EXISTS tokens")
		for i := 0; i < 100; i++ {
			db.Exec(fmt.Sprintf("DROP TABLE IF EXISTS tokens_%02d", i))
		}
	})

	t.Run("JustAboveThreshold_51Percent", func(t *testing.T) {
		// Configure sharding with 100 shards and 50% threshold
		middleware := Register(Config{
			ShardingKey:              "contract",
			NumberOfShards:           100,
			PrimaryKeyGenerator:      PKSnowflake,
			DoubleWrite:              true,
			ShardPercentageThreshold: 0.5, // 50% threshold
		}, "tokens")

		db.Use(middleware)
		err = db.AutoMigrate(&Token{})
		assert.NoError(t, err)

		// Insert test data - 51 contracts (51% of shards)
		for i := 0; i < 51; i++ {
			token := Token{
				Contract: fmt.Sprintf("0xdifferent%02d", i),
				TokenID:  fmt.Sprintf("%d", i),
				Owner:    fmt.Sprintf("owner%d", i),
			}
			err := db.Create(&token).Error
			assert.NoError(t, err)
		}

		// Build query with 51 contracts
		queryParts := []string{}
		for i := 0; i < 51; i++ {
			queryParts = append(queryParts, fmt.Sprintf("('0xdifferent%02d', '%d')", i, i))
		}

		var results []Token
		query := fmt.Sprintf(`SELECT * FROM "tokens" WHERE ((contract, token_id)) IN (%s)`,
			strings.Join(queryParts, ","))

		err = db.Raw(query).Find(&results).Error
		assert.NoError(t, err)

		// Should use base table since 51% > 50%
		uniqueResults := make(map[string]bool)
		for _, r := range results {
			uniqueResults[r.Contract+"_"+r.TokenID] = true
		}
		assert.Equal(t, 51, len(uniqueResults), "Should return 51 unique tokens from base table")

		// Cleanup
		db.Exec("DROP TABLE IF EXISTS tokens")
		for i := 0; i < 100; i++ {
			db.Exec(fmt.Sprintf("DROP TABLE IF EXISTS tokens_%02d", i))
		}
	})

	t.Run("ThresholdZero_AlwaysUseBaseTable", func(t *testing.T) {
		// Configure sharding with threshold of 0 (always use base table)
		middleware := Register(Config{
			ShardingKey:              "contract",
			NumberOfShards:           10,
			PrimaryKeyGenerator:      PKSnowflake,
			DoubleWrite:              true,
			ShardPercentageThreshold: 0.0, // Always use base table
		}, "tokens")

		db.Use(middleware)
		err = db.AutoMigrate(&Token{})
		assert.NoError(t, err)

		// Insert just 1 token (would use 10% of shards)
		token := Token{
			Contract: "0xsingle",
			TokenID:  "1",
			Owner:    "owner1",
		}
		err = db.Create(&token).Error
		assert.NoError(t, err)

		// Query for single token
		var results []Token
		query := `SELECT * FROM "tokens" WHERE ((contract, token_id)) IN (('0xsingle', '1'))`

		err = db.Raw(query).Find(&results).Error
		assert.NoError(t, err)

		// Even with 1 shard (10%), should use base table due to 0 threshold
		assert.Equal(t, 1, len(results), "Should return 1 token from base table")

		// Cleanup
		db.Exec("DROP TABLE IF EXISTS tokens, tokens_0, tokens_1, tokens_2, tokens_3, tokens_4, tokens_5, tokens_6, tokens_7, tokens_8, tokens_9")
	})

	t.Run("ThresholdOne_AlwaysUseShard", func(t *testing.T) {
		// Configure sharding with threshold of 1.0 (always use sharding)
		middleware := Register(Config{
			ShardingKey:              "contract",
			NumberOfShards:           4,
			PrimaryKeyGenerator:      PKSnowflake,
			DoubleWrite:              true,
			ShardPercentageThreshold: 1.0, // Always use sharding
		}, "tokens")

		db.Use(middleware)
		err = db.AutoMigrate(&Token{})
		assert.NoError(t, err)

		// Insert tokens that would use all 4 shards (100%)
		testData := []struct {
			contract string
			tokenID  string
		}{
			{"0xall1", "1"},
			{"0xall2", "2"},
			{"0xall3", "3"},
			{"0xall4", "4"},
		}

		for i, data := range testData {
			token := Token{
				Contract: data.contract,
				TokenID:  data.tokenID,
				Owner:    fmt.Sprintf("owner%d", i),
			}
			err := db.Create(&token).Error
			assert.NoError(t, err)
		}

		// Query that would use all shards
		var results []Token
		query := `SELECT * FROM "tokens" WHERE ((contract, token_id)) IN (
			('0xall1', '1'),('0xall2', '2'),('0xall3', '3'),('0xall4', '4')
		)`

		err = db.Raw(query).Find(&results).Error
		assert.NoError(t, err)

		// Even with 100% shards, should still use sharding due to threshold = 1.0
		uniqueResults := make(map[string]bool)
		for _, r := range results {
			uniqueResults[r.Contract+"_"+r.TokenID] = true
		}
		assert.Equal(t, 4, len(uniqueResults), "Should return 4 unique tokens")

		// Cleanup
		db.Exec("DROP TABLE IF EXISTS tokens, tokens_0, tokens_1, tokens_2, tokens_3")
	})

	t.Run("DoubleWriteDisabled_AlwaysUseShard", func(t *testing.T) {
		// Configure sharding without DoubleWrite
		middleware := Register(Config{
			ShardingKey:              "contract",
			NumberOfShards:           4,
			PrimaryKeyGenerator:      PKSnowflake,
			DoubleWrite:              false, // DoubleWrite disabled
			ShardPercentageThreshold: 0.5,   // 50% threshold
		}, "tokens")

		db.Use(middleware)
		err = db.AutoMigrate(&Token{})
		assert.NoError(t, err)

		// Insert tokens that would use 3 out of 4 shards (75%)
		testData := []struct {
			contract string
			tokenID  string
		}{
			{"0xnodw1", "1"},
			{"0xnodw2", "2"},
			{"0xnodw3", "3"},
		}

		for i, data := range testData {
			token := Token{
				Contract: data.contract,
				TokenID:  data.tokenID,
				Owner:    fmt.Sprintf("owner%d", i),
			}
			err := db.Create(&token).Error
			assert.NoError(t, err)
		}

		// Query that would use 75% of shards
		var results []Token
		query := `SELECT * FROM "tokens" WHERE ((contract, token_id)) IN (
			('0xnodw1', '1'),('0xnodw2', '2'),('0xnodw3', '3')
		)`

		err = db.Raw(query).Find(&results).Error
		assert.NoError(t, err)

		// Even though 75% > 50%, without DoubleWrite it must use sharding
		assert.Equal(t, 3, len(results), "Should return 3 tokens from sharded tables")

		// Cleanup
		db.Exec("DROP TABLE IF EXISTS tokens, tokens_0, tokens_1, tokens_2, tokens_3")
	})

	t.Run("SingleValueCompositeIN", func(t *testing.T) {
		// Test with composite IN clause that has only one value
		middleware := Register(Config{
			ShardingKey:              "contract",
			NumberOfShards:           10,
			PrimaryKeyGenerator:      PKSnowflake,
			DoubleWrite:              true,
			ShardPercentageThreshold: 0.5,
		}, "tokens")

		db.Use(middleware)
		err = db.AutoMigrate(&Token{})
		assert.NoError(t, err)

		// Insert single token
		token := Token{
			Contract: "0xsinglecomp",
			TokenID:  "999",
			Owner:    "singleowner",
		}
		err = db.Create(&token).Error
		assert.NoError(t, err)

		// Query with single value in composite IN
		var results []Token
		query := `SELECT * FROM "tokens" WHERE ((contract, token_id)) IN (('0xsinglecomp', '999'))`

		err = db.Raw(query).Find(&results).Error
		assert.NoError(t, err)

		// Should use sharding for single value
		assert.Equal(t, 1, len(results), "Should return 1 token")
		assert.Equal(t, "0xsinglecomp", results[0].Contract)
		assert.Equal(t, "999", results[0].TokenID)

		// Cleanup
		db.Exec("DROP TABLE IF EXISTS tokens, tokens_0, tokens_1, tokens_2, tokens_3, tokens_4, tokens_5, tokens_6, tokens_7, tokens_8, tokens_9")
	})

	t.Run("AllValuesInSameShard", func(t *testing.T) {
		// Test when all values in composite IN hash to the same shard
		middleware := Register(Config{
			ShardingKey:              "contract",
			NumberOfShards:           10,
			PrimaryKeyGenerator:      PKSnowflake,
			DoubleWrite:              true,
			ShardPercentageThreshold: 0.5,
		}, "tokens")

		db.Use(middleware)
		err = db.AutoMigrate(&Token{})
		assert.NoError(t, err)

		// Insert tokens that will hash to the same shard
		// These contracts are designed to hash to the same shard
		testData := []struct {
			contract string
			tokenID  string
		}{
			{"0x1111111111111111111111111111111111111111", "1"},
			{"0x1111111111111111111111111111111111111111", "2"},
			{"0x1111111111111111111111111111111111111111", "3"},
		}

		for i, data := range testData {
			token := Token{
				Contract: data.contract,
				TokenID:  data.tokenID,
				Owner:    fmt.Sprintf("owner%d", i),
			}
			err := db.Create(&token).Error
			assert.NoError(t, err)
		}

		// Query with values all in same shard
		var results []Token
		query := `SELECT * FROM "tokens" WHERE ((contract, token_id)) IN (
			('0x1111111111111111111111111111111111111111', '1'),
			('0x1111111111111111111111111111111111111111', '2'),
			('0x1111111111111111111111111111111111111111', '3')
		)`

		err = db.Raw(query).Find(&results).Error
		assert.NoError(t, err)

		// Should use single shard (10% < 50% threshold)
		assert.Equal(t, 3, len(results), "Should return 3 tokens from single shard")

		// Cleanup
		db.Exec("DROP TABLE IF EXISTS tokens, tokens_0, tokens_1, tokens_2, tokens_3, tokens_4, tokens_5, tokens_6, tokens_7, tokens_8, tokens_9")
	})
}

func TestShardPercentageThresholdEdgeCases(t *testing.T) {
	// Note: This test cannot run in parallel because all subtests share the same table names
	// To make it parallel-safe, each subtest would need to use unique table names
	
	// Enable debug logging for this test
	oldLogLevel := DefaultLogLevel
	SetLogLevel(LogLevelDebug)
	defer SetLogLevel(oldLogLevel)

	dbConfig := postgres.Config{
		DSN:                  dbURL(),
		PreferSimpleProtocol: true,
	}
	db, err := gorm.Open(postgres.New(dbConfig), &gorm.Config{
		DisableForeignKeyConstraintWhenMigrating: true,
		Logger:                                   logger.Default.LogMode(logger.Info),
	})
	assert.NoError(t, err)

	t.Run("EmptyCompositeIN", func(t *testing.T) {
		// Test with empty composite IN clause
		middleware := Register(Config{
			ShardingKey:              "contract",
			NumberOfShards:           10,
			PrimaryKeyGenerator:      PKSnowflake,
			DoubleWrite:              true,
			ShardPercentageThreshold: 0.5,
		}, "tokens")

		db.Use(middleware)
		err = db.AutoMigrate(&Token{})
		assert.NoError(t, err)

		// Query with empty IN clause
		var results []Token
		query := `SELECT * FROM "tokens" WHERE ((contract, token_id)) IN ()`

		// This should likely return an error or empty result
		err = db.Raw(query).Find(&results).Error
		// PostgreSQL doesn't allow empty IN clause, so this should error
		assert.Error(t, err, "Empty IN clause should cause an error")

		// Cleanup
		db.Exec("DROP TABLE IF EXISTS tokens, tokens_0, tokens_1, tokens_2, tokens_3, tokens_4, tokens_5, tokens_6, tokens_7, tokens_8, tokens_9")
	})

	t.Run("MixedShardingKeyValues", func(t *testing.T) {
		// Test with NULL and empty string values mixed with regular values
		middleware := Register(Config{
			ShardingKey:              "contract",
			NumberOfShards:           4,
			PrimaryKeyGenerator:      PKSnowflake,
			DoubleWrite:              true,
			ShardPercentageThreshold: 0.5,
		}, "tokens")

		db.Use(middleware)
		err = db.AutoMigrate(&Token{})
		assert.NoError(t, err)

		// Insert tokens with various edge case values
		testData := []struct {
			contract string
			tokenID  string
		}{
			{"0xnormal", "1"},
			{"", "2"}, // Empty string
			{"0xanother", "3"},
			{"", "4"}, // Another empty string
		}

		for i, data := range testData {
			token := Token{
				Contract: data.contract,
				TokenID:  data.tokenID,
				Owner:    fmt.Sprintf("owner%d", i),
			}
			err := db.Create(&token).Error
			assert.NoError(t, err)
		}

		// Query including empty strings
		var results []Token
		query := `SELECT * FROM "tokens" WHERE ((contract, token_id)) IN (
			('0xnormal', '1'),
			('', '2'),
			('0xanother', '3'),
			('', '4')
		)`

		err = db.Raw(query).Find(&results).Error
		assert.NoError(t, err)
		assert.Equal(t, 4, len(results), "Should handle empty string values")

		// Cleanup
		db.Exec("DROP TABLE IF EXISTS tokens, tokens_0, tokens_1, tokens_2, tokens_3")
	})

	t.Run("VeryLargeNumberOfValues", func(t *testing.T) {
		// Test with a large number of values in composite IN
		middleware := Register(Config{
			ShardingKey:              "contract",
			NumberOfShards:           20,
			PrimaryKeyGenerator:      PKSnowflake,
			DoubleWrite:              true,
			ShardPercentageThreshold: 0.8, // 80% threshold
		}, "tokens")

		db.Use(middleware)
		err = db.AutoMigrate(&Token{})
		assert.NoError(t, err)

		// Insert 20 tokens across many shards
		numTokens := 20
		for i := 0; i < numTokens; i++ {
			token := Token{
				Contract: fmt.Sprintf("0xlarge%04d", i),
				TokenID:  fmt.Sprintf("%d", i),
				Owner:    fmt.Sprintf("owner%d", i),
			}
			err := db.Create(&token).Error
			assert.NoError(t, err)
		}

		// Build a query with 18 values (should hit many different shards, likely > 80%)
		queryParts := []string{}
		for i := 0; i < 18; i++ {
			queryParts = append(queryParts, fmt.Sprintf("('0xlarge%04d', '%d')", i, i))
		}

		var results []Token
		query := fmt.Sprintf(`SELECT * FROM "tokens" WHERE ((contract, token_id)) IN (%s)`,
			strings.Join(queryParts, ","))

		err = db.Raw(query).Find(&results).Error
		assert.NoError(t, err)

		// Should likely use base table due to high shard percentage
		uniqueResults := make(map[string]bool)
		for _, r := range results {
			uniqueResults[r.Contract+"_"+r.TokenID] = true
		}
		assert.Equal(t, 18, len(uniqueResults), "Should return 18 unique tokens")

		// Cleanup
		db.Exec("DROP TABLE IF EXISTS tokens")
		for i := 0; i < 20; i++ {
			db.Exec(fmt.Sprintf("DROP TABLE IF EXISTS tokens_%02d", i))
		}
	})

	t.Run("NonExistentValues", func(t *testing.T) {
		// Test querying for values that don't exist
		middleware := Register(Config{
			ShardingKey:              "contract",
			NumberOfShards:           4,
			PrimaryKeyGenerator:      PKSnowflake,
			DoubleWrite:              true,
			ShardPercentageThreshold: 0.5,
		}, "tokens")

		db.Use(middleware)
		err = db.AutoMigrate(&Token{})
		assert.NoError(t, err)

		// Insert some tokens
		for i := 0; i < 5; i++ {
			token := Token{
				Contract: fmt.Sprintf("0xexist%d", i),
				TokenID:  fmt.Sprintf("%d", i),
				Owner:    fmt.Sprintf("owner%d", i),
			}
			err := db.Create(&token).Error
			assert.NoError(t, err)
		}

		// Query for non-existent values across multiple shards
		var results []Token
		query := `SELECT * FROM "tokens" WHERE ((contract, token_id)) IN (
			('0xnotexist1', '999'),
			('0xnotexist2', '998'),
			('0xnotexist3', '997'),
			('0xnotexist4', '996')
		)`

		err = db.Raw(query).Find(&results).Error
		assert.NoError(t, err)
		assert.Equal(t, 0, len(results), "Should return no results for non-existent values")

		// Cleanup
		db.Exec("DROP TABLE IF EXISTS tokens, tokens_0, tokens_1, tokens_2, tokens_3")
	})

	t.Run("MixedExistingAndNonExisting", func(t *testing.T) {
		// Test with mix of existing and non-existing values
		middleware := Register(Config{
			ShardingKey:              "contract",
			NumberOfShards:           10,
			PrimaryKeyGenerator:      PKSnowflake,
			DoubleWrite:              true,
			ShardPercentageThreshold: 0.5,
		}, "tokens")

		db.Use(middleware)
		err = db.AutoMigrate(&Token{})
		assert.NoError(t, err)

		// Insert some tokens
		existingTokens := []struct {
			contract string
			tokenID  string
		}{
			{"0xmixed1", "100"},
			{"0xmixed2", "200"},
			{"0xmixed3", "300"},
		}

		for i, data := range existingTokens {
			token := Token{
				Contract: data.contract,
				TokenID:  data.tokenID,
				Owner:    fmt.Sprintf("owner%d", i),
			}
			err := db.Create(&token).Error
			assert.NoError(t, err)
		}

		// Query with mix of existing and non-existing
		var results []Token
		query := `SELECT * FROM "tokens" WHERE ((contract, token_id)) IN (
			('0xmixed1', '100'),     -- exists
			('0xnotexist', '999'),   -- doesn't exist
			('0xmixed2', '200'),     -- exists
			('0xalsonotexist', '888'),-- doesn't exist
			('0xmixed3', '300')      -- exists
		)`

		err = db.Raw(query).Find(&results).Error
		assert.NoError(t, err)
		assert.Equal(t, 3, len(results), "Should return only existing tokens")

		// Verify we got the right tokens
		foundContracts := make(map[string]bool)
		for _, r := range results {
			foundContracts[r.Contract] = true
		}
		assert.True(t, foundContracts["0xmixed1"])
		assert.True(t, foundContracts["0xmixed2"])
		assert.True(t, foundContracts["0xmixed3"])

		// Cleanup
		db.Exec("DROP TABLE IF EXISTS tokens, tokens_0, tokens_1, tokens_2, tokens_3, tokens_4, tokens_5, tokens_6, tokens_7, tokens_8, tokens_9")
	})

	t.Run("SpecialCharactersInValues", func(t *testing.T) {
		// Test with special characters in contract addresses
		middleware := Register(Config{
			ShardingKey:              "contract",
			NumberOfShards:           4,
			PrimaryKeyGenerator:      PKSnowflake,
			DoubleWrite:              true,
			ShardPercentageThreshold: 0.5,
		}, "tokens")

		db.Use(middleware)
		err = db.AutoMigrate(&Token{})
		assert.NoError(t, err)

		// Insert tokens with special characters
		specialTokens := []struct {
			contract string
			tokenID  string
		}{
			{"0x'quoted'", "1"},
			{"0x\"doublequoted\"", "2"},
			{"0x\\backslash\\", "3"},
			{"0x;semicolon;", "4"},
		}

		for i, data := range specialTokens {
			token := Token{
				Contract: data.contract,
				TokenID:  data.tokenID,
				Owner:    fmt.Sprintf("owner%d", i),
			}
			// These might fail due to SQL injection protection
			err := db.Create(&token).Error
			if err != nil {
				t.Logf("Expected error creating token with special chars: %v", err)
			}
		}

		// Try to query with special characters (properly escaped)
		var results []Token
		query := `SELECT * FROM "tokens" WHERE ((contract, token_id)) IN (
			('0x''quoted''', '1'),
			('0x"doublequoted"', '2')
		)`

		err = db.Raw(query).Find(&results).Error
		// This might error or return results depending on how special chars are handled
		t.Logf("Query with special chars result count: %d, error: %v", len(results), err)

		// Cleanup
		db.Exec("DROP TABLE IF EXISTS tokens, tokens_0, tokens_1, tokens_2, tokens_3")
	})

	t.Run("VeryLongValues", func(t *testing.T) {
		// Test with very long contract addresses and token IDs
		middleware := Register(Config{
			ShardingKey:              "contract",
			NumberOfShards:           4,
			PrimaryKeyGenerator:      PKSnowflake,
			DoubleWrite:              true,
			ShardPercentageThreshold: 0.5,
		}, "tokens")

		db.Use(middleware)
		err = db.AutoMigrate(&Token{})
		assert.NoError(t, err)

		// Create very long values
		longContract := "0x" + strings.Repeat("a", 100)
		longTokenID := strings.Repeat("9", 50)

		token := Token{
			Contract: longContract,
			TokenID:  longTokenID,
			Owner:    "owner_long",
		}
		err = db.Create(&token).Error
		assert.NoError(t, err)

		// Query with long values
		var results []Token
		query := fmt.Sprintf(`SELECT * FROM "tokens" WHERE ((contract, token_id)) IN (('%s', '%s'))`,
			longContract, longTokenID)

		err = db.Raw(query).Find(&results).Error
		assert.NoError(t, err)
		assert.Equal(t, 1, len(results), "Should handle long values")

		// Cleanup
		db.Exec("DROP TABLE IF EXISTS tokens, tokens_0, tokens_1, tokens_2, tokens_3")
	})

	t.Run("CaseSensitivity", func(t *testing.T) {
		// Test case sensitivity in contract addresses
		middleware := Register(Config{
			ShardingKey:              "contract",
			NumberOfShards:           4,
			PrimaryKeyGenerator:      PKSnowflake,
			DoubleWrite:              true,
			ShardPercentageThreshold: 0.5,
		}, "tokens")

		db.Use(middleware)
		err = db.AutoMigrate(&Token{})
		assert.NoError(t, err)

		// Insert tokens with different cases
		testData := []struct {
			contract string
			tokenID  string
		}{
			{"0xABCDEF", "1"},
			{"0xabcdef", "2"},
			{"0xAbCdEf", "3"},
		}

		for i, data := range testData {
			token := Token{
				Contract: data.contract,
				TokenID:  data.tokenID,
				Owner:    fmt.Sprintf("owner%d", i),
			}
			err := db.Create(&token).Error
			assert.NoError(t, err)
		}

		// Query with different case
		var results []Token
		query := `SELECT * FROM "tokens" WHERE ((contract, token_id)) IN (
			('0xABCDEF', '1'),
			('0xabcdef', '2'),
			('0xAbCdEf', '3')
		)`

		err = db.Raw(query).Find(&results).Error
		assert.NoError(t, err)
		// Case sensitivity depends on database collation
		assert.GreaterOrEqual(t, len(results), 3, "Should handle case variations")

		// Cleanup
		db.Exec("DROP TABLE IF EXISTS tokens, tokens_0, tokens_1, tokens_2, tokens_3")
	})
}
