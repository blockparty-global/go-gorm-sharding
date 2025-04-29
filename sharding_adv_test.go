package sharding

import (
	"fmt"
	"testing"
	"time"

	tassert "github.com/stretchr/testify/assert"
	"gorm.io/driver/postgres"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
	"gorm.io/hints"
)

type TokenWithHashPartition struct {
	ID                  int64  `gorm:"primarykey"`
	Contract            string `gorm:"index:idx_contract"`
	TokenID             string `gorm:"index:idx_token_id"`
	TokenURIStatus      string
	TokenURI            string
	Name                string
	Description         string
	LastTokenURICheck   *time.Time
	MetadataStatus      string
	MetadataContentType string
	MetadataContent     string
	MetadataAttempts    int
	LastMetadataAttempt *time.Time
	CreatedAt           time.Time
	CreatedBlock        *int64
	BurnedAt            *time.Time
	BurnedBlock         *int64
	ErrorMsg            string
	Expired             bool
	MetadataChecks      int
	LastMetadataCheck   *time.Time
	UpdatedAt           time.Time
}

// ContractWithHashPartition represents a blockchain contract with hash partitioning
type ContractWithHashPartition struct {
	ID        int64  `gorm:"primarykey"`
	Address   string `gorm:"uniqueIndex"`
	Name      string `gorm:"index:idx_name"`
	Type      string `gorm:"index:idx_type"`
	IsERC20   bool
	IsERC721  bool
	IsERC1155 bool
	CreatedAt time.Time
	UpdatedAt time.Time
}

func TestLowerFunctionOnShardingKey(t *testing.T) {
	// Create a test DB with proper configuration
	testDB, err := gorm.Open(postgres.New(dbConfig), &gorm.Config{
		DisableForeignKeyConstraintWhenMigrating: true,
		Logger:                                   logger.Default.LogMode(logger.Info),
	})
	if err != nil {
		t.Fatalf("Failed to connect to database: %v", err)
	}

	// Set up hash partitioning with token_id as the sharding key for tokens
	tokenConfig := Config{
		DoubleWrite:         true,
		ShardingKey:         "token_id", // token_id is the sharding key for tokens
		PartitionType:       PartitionTypeHash,
		NumberOfShards:      4,
		ShardingAlgorithm:   shardingHasher4Algorithm,
		PrimaryKeyGenerator: PKSnowflake,
		ShardingSuffixs: func() []string {
			return []string{"_0", "_1", "_2", "_3"}
		},
	}

	// Set up hash partitioning with name as the sharding key for contracts
	contractConfig := Config{
		DoubleWrite:         true,
		ShardingKey:         "name", // name is the sharding key for contracts
		PartitionType:       PartitionTypeHash,
		NumberOfShards:      4,
		ShardingAlgorithm:   shardingHasher4Algorithm,
		PrimaryKeyGenerator: PKSnowflake,
		ShardingSuffixs: func() []string {
			return []string{"_0", "_1", "_2", "_3"}
		},
	}

	// Register the middleware with both configurations
	configs := map[string]Config{
		"token_with_hash_partitions":    tokenConfig,
		"contract_with_hash_partitions": contractConfig,
	}

	middleware := Register(configs, &TokenWithHashPartition{}, &ContractWithHashPartition{})
	testDB.Use(middleware)

	// Drop and recreate tables
	testDB.Exec("DROP TABLE IF EXISTS token_with_hash_partitions")
	testDB.Exec("DROP TABLE IF EXISTS contract_with_hash_partitions")
	for i := 0; i < 4; i++ {
		testDB.Exec(fmt.Sprintf("DROP TABLE IF EXISTS token_with_hash_partitions_%d", i))
		testDB.Exec(fmt.Sprintf("DROP TABLE IF EXISTS contract_with_hash_partitions_%d", i))
	}

	// Auto migrate to create the tables
	err = testDB.AutoMigrate(&TokenWithHashPartition{}, &ContractWithHashPartition{})
	if err != nil {
		t.Fatalf("Failed to migrate tables: %v", err)
	}

	// Create sharded tables manually
	for i := 0; i < 4; i++ {
		// Create token tables
		testDB.Exec(fmt.Sprintf(`CREATE TABLE IF NOT EXISTS token_with_hash_partitions_%d (
			id bigint PRIMARY KEY,
			contract text,
			token_id text,
			token_uri_status text,
			token_uri text,
			name text,
			description text,
			created_at timestamp with time zone,
			updated_at timestamp with time zone
		)`, i))

		// Create contract tables
		testDB.Exec(fmt.Sprintf(`CREATE TABLE IF NOT EXISTS contract_with_hash_partitions_%d (
			id bigint PRIMARY KEY,
			address text,
			name text,
			type text,
			is_erc20 boolean,
			is_erc721 boolean,
			is_erc1155 boolean,
			created_at timestamp with time zone,
			updated_at timestamp with time zone
		)`, i))
	}

	// Insert test contracts
	contracts := []ContractWithHashPartition{
		{Address: "0xabc123", Name: "NOBUYSOK", Type: "ERC721", IsERC721: true},
		{Address: "0xdef456", Name: "nobuysok", Type: "ERC721", IsERC721: true},
		{Address: "0xghi789", Name: "Nobuysok", Type: "ERC721", IsERC721: true},
		{Address: "0xjkl012", Name: "OtherToken", Type: "ERC721", IsERC721: true},
	}

	// Insert the contracts
	for _, contract := range contracts {
		err := testDB.Create(&contract).Error
		tassert.NoError(t, err, "Failed to insert contract")
		t.Logf("Created contract with address %s, name %s, ID %d", contract.Address, contract.Name, contract.ID)
	}

	// Insert tokens for each contract
	for _, contract := range contracts {
		// Create multiple tokens per contract
		for i := 1; i <= 3; i++ {
			token := TokenWithHashPartition{
				Contract:       contract.Address,
				TokenID:        fmt.Sprintf("%d", i),
				TokenURIStatus: "READY",
				Name:           fmt.Sprintf("%s #%d", contract.Name, i),
				Description:    fmt.Sprintf("Token %d for contract %s", i, contract.Name),
				CreatedAt:      time.Now(),
				UpdatedAt:      time.Now(),
			}

			err := testDB.Create(&token).Error
			tassert.NoError(t, err, "Failed to insert token")
			t.Logf("Created token with ID %d, contract %s, tokenID %s", token.ID, token.Contract, token.TokenID)
		}
	}

	// Test 1: This test reproduces the error - using LOWER on the sharding key
	t.Run("ReproduceError_LowerOnShardingKey", func(t *testing.T) {
		var results []struct {
			TokenWithHashPartition
			ContractName string
		}

		// This query should fail because it uses LOWER on the sharding key (contracts.name)
		// without providing a direct equality condition on the sharding key
		err := testDB.Table("token_with_hash_partitions").
			Select("token_with_hash_partitions.*, contract_with_hash_partitions.name as contract_name").
			Joins("JOIN contract_with_hash_partitions ON token_with_hash_partitions.contract = contract_with_hash_partitions.address").
			Where("LOWER(contract_with_hash_partitions.name) = LOWER(?)", "NOBUYSOK").
			Limit(10).
			Find(&results).Error

		// This should fail with "sharding key or id required, and use operator ="
		//tassert.Error(t, err, "Query should fail with sharding key error")
		//tassert.Contains(t, err.Error(), "sharding key or id required", "Error should mention sharding key requirement")
		tassert.NoError(t, err, "Query with direct equality should succeed")
		t.Logf("Last query: %s", middleware.LastQuery())
	})

	// Test 2: Fix 1 - Use direct equality on the sharding key
	t.Run("Fix1_DirectEqualityOnShardingKey", func(t *testing.T) {
		var results []struct {
			TokenWithHashPartition
			ContractName string
		}

		// This query should succeed because it uses direct equality on both sharding keys
		err := testDB.Table("token_with_hash_partitions").
			Select("token_with_hash_partitions.*, contract_with_hash_partitions.name as contract_name").
			Joins("JOIN contract_with_hash_partitions ON token_with_hash_partitions.contract = contract_with_hash_partitions.address").
			Where("contract_with_hash_partitions.name = ?", "NOBUYSOK").
			Where("token_with_hash_partitions.token_id = ?", "1"). // Add token_id sharding key
			Limit(10).
			Find(&results).Error

		tassert.NoError(t, err, "Query with direct equality should succeed")
		tassert.GreaterOrEqual(t, len(results), 1, "Should find at least one result")

		t.Logf("Found %d results with direct equality", len(results))
		t.Logf("Last query: %s", middleware.LastQuery())
	})

	// Test 3: Fix 2 - Use IN clause for case-insensitive matching
	t.Run("Fix2_InClauseForCaseInsensitiveMatching", func(t *testing.T) {
		var results []struct {
			TokenWithHashPartition
			ContractName string
		}

		// This query should succeed because it uses direct equality with multiple case variations
		err := testDB.Table("token_with_hash_partitions").
			Select("token_with_hash_partitions.*, contract_with_hash_partitions.name as contract_name").
			Joins("JOIN contract_with_hash_partitions ON token_with_hash_partitions.contract = contract_with_hash_partitions.address").
			Where("contract_with_hash_partitions.name IN ?", []string{"NOBUYSOK", "nobuysok", "Nobuysok"}).
			Limit(10).
			Find(&results).Error

		tassert.NoError(t, err, "Query with IN clause should succeed")
		tassert.GreaterOrEqual(t, len(results), 3, "Should find at least three results (one for each case variation)")

		t.Logf("Found %d results with IN clause", len(results))
		t.Logf("Last query: %s", middleware.LastQuery())
	})

	// Test 4: Fix 3 - Use nosharding hint for queries that need LOWER function
	t.Run("Fix3_UseNoshardingHint", func(t *testing.T) {
		var results []struct {
			TokenWithHashPartition
			ContractName string
		}

		// This query should succeed because it uses the nosharding hint
		err := testDB.Table("token_with_hash_partitions").
			Clauses(hints.New("nosharding")).
			Select("token_with_hash_partitions.*, contract_with_hash_partitions.name as contract_name").
			Joins("JOIN contract_with_hash_partitions ON token_with_hash_partitions.contract = contract_with_hash_partitions.address").
			Where("LOWER(contract_with_hash_partitions.name) = LOWER(?)", "NOBUYSOK").
			Limit(10).
			Find(&results).Error

		tassert.NoError(t, err, "Query with nosharding hint should succeed")
		tassert.GreaterOrEqual(t, len(results), 3, "Should find at least three results (one for each case variation)")

		t.Logf("Found %d results with nosharding hint", len(results))
		t.Logf("Last query: %s", middleware.LastQuery())
	})

	// Test 5: Fix 4 - Use a subquery to get the contract IDs first, then query tokens
	t.Run("Fix4_UseSubqueryApproach", func(t *testing.T) {
		// First, get the contract addresses with the case-insensitive name match
		var contractAddresses []string
		err := testDB.Table("contract_with_hash_partitions").
			Clauses(hints.New("nosharding")).
			Where("LOWER(name) = LOWER(?)", "NOBUYSOK").
			Pluck("address", &contractAddresses).Error

		tassert.NoError(t, err, "Subquery for contract addresses should succeed")
		tassert.GreaterOrEqual(t, len(contractAddresses), 3, "Should find at least three contract addresses")

		// If we found contract addresses, now query tokens with those addresses
		if len(contractAddresses) > 0 {
			var results []struct {
				TokenWithHashPartition
				ContractName string
			}

			err := testDB.Table("token_with_hash_partitions").
				Select("token_with_hash_partitions.*, contract_with_hash_partitions.name as contract_name").
				Joins("JOIN contract_with_hash_partitions ON token_with_hash_partitions.contract = contract_with_hash_partitions.address").
				Where("token_with_hash_partitions.contract IN ?", contractAddresses).
				Limit(10).
				Find(&results).Error

			tassert.NoError(t, err, "Query with contract addresses should succeed")
			tassert.GreaterOrEqual(t, len(results), 3, "Should find at least three results")

			t.Logf("Found %d results with subquery approach", len(results))
			t.Logf("Last query: %s", middleware.LastQuery())
		}
	})
}

func TestHashPartitioningWithLowerFunction(t *testing.T) {

	// Create a test DB with proper configuration
	testDB, err := gorm.Open(postgres.New(dbConfig), &gorm.Config{
		DisableForeignKeyConstraintWhenMigrating: true,
		Logger:                                   logger.Default.LogMode(logger.Info),
	})
	if err != nil {
		t.Fatalf("Failed to connect to database: %v", err)
	}

	// Register cleanup function
	t.Cleanup(func() {
		// Drop tables after test completion
		testDB.Exec("DROP TABLE IF EXISTS token_with_hash_partitions")
		testDB.Exec("DROP TABLE IF EXISTS contract_with_hash_partitions")
		for i := 0; i < 4; i++ {
			testDB.Exec(fmt.Sprintf("DROP TABLE IF EXISTS token_with_hash_partitions_%d", i))
			testDB.Exec(fmt.Sprintf("DROP TABLE IF EXISTS contract_with_hash_partitions_%d", i))
		}
	})

	// Set up hash partitioning with contract as the sharding key for tokens
	hashTokenConfig := Config{
		DoubleWrite:         true,
		ShardingKey:         "contract",
		PartitionType:       PartitionTypeHash,
		NumberOfShards:      4,
		ShardingAlgorithm:   shardingHasher4Algorithm,
		PrimaryKeyGenerator: PKSnowflake,
		ShardingSuffixs: func() []string {
			return []string{"_0", "_1", "_2", "_3"}
		},
	}

	// Set up hash partitioning with address as the sharding key for contracts
	hashContractConfig := Config{
		DoubleWrite:         true,
		ShardingKey:         "address",
		PartitionType:       PartitionTypeHash,
		NumberOfShards:      4,
		ShardingAlgorithm:   shardingHasher4Algorithm,
		PrimaryKeyGenerator: PKSnowflake,
		ShardingSuffixs: func() []string {
			return []string{"_0", "_1", "_2", "_3"}
		},
	}

	// Register the middleware with both configurations
	hashConfigs := map[string]Config{
		"token_with_hash_partitions":    hashTokenConfig,
		"contract_with_hash_partitions": hashContractConfig,
	}

	hashMiddleware := Register(hashConfigs, &TokenWithHashPartition{}, &ContractWithHashPartition{})
	testDB.Use(hashMiddleware)

	// Drop and recreate tables
	testDB.Exec("DROP TABLE IF EXISTS token_with_hash_partitions")
	testDB.Exec("DROP TABLE IF EXISTS contract_with_hash_partitions")
	for i := 0; i < 4; i++ {
		testDB.Exec(fmt.Sprintf("DROP TABLE IF EXISTS token_with_hash_partitions_%d", i))
		testDB.Exec(fmt.Sprintf("DROP TABLE IF EXISTS contract_with_hash_partitions_%d", i))
	}

	// Auto migrate to create the tables
	migrateErr := testDB.AutoMigrate(&TokenWithHashPartition{}, &ContractWithHashPartition{})
	if migrateErr != nil {
		t.Fatalf("Failed to migrate tables: %v", migrateErr)
	}

	// Create sharded tables manually
	for i := 0; i < 4; i++ {
		// Create token tables
		testDB.Exec(fmt.Sprintf(`CREATE TABLE IF NOT EXISTS token_with_hash_partitions_%d (
			id bigint PRIMARY KEY,
			contract text,
			token_id text,
			token_uri_status text,
			token_uri text,
			name text,
			description text,
			created_at timestamp with time zone,
			updated_at timestamp with time zone
		)`, i))

		// Create contract tables
		testDB.Exec(fmt.Sprintf(`CREATE TABLE IF NOT EXISTS contract_with_hash_partitions_%d (
			id bigint PRIMARY KEY,
			address text,
			name text,
			type text,
			is_erc20 boolean,
			is_erc721 boolean,
			is_erc1155 boolean,
			created_at timestamp with time zone,
			updated_at timestamp with time zone
		)`, i))
	}

	// Insert test contracts with different case variations of the same name
	contracts := []ContractWithHashPartition{
		{Address: "0xabc123", Name: "MTKN", Type: "ERC721", IsERC721: true},
		{Address: "0xdef456", Name: "mtkn", Type: "ERC721", IsERC721: true},
		{Address: "0xghi789", Name: "Mtkn", Type: "ERC721", IsERC721: true},
		{Address: "0xjkl012", Name: "MTkn", Type: "ERC721", IsERC721: true},
		{Address: "0xmno345", Name: "DifferentToken", Type: "ERC721", IsERC721: true},
	}

	// Insert the contracts
	for _, contract := range contracts {
		err := testDB.Create(&contract).Error
		tassert.NoError(t, err, "Failed to insert contract")
		t.Logf("Created contract with address %s, name %s, ID %d", contract.Address, contract.Name, contract.ID)
	}

	// Insert tokens for each contract
	for _, contract := range contracts {
		// Create multiple tokens per contract
		for i := 1; i <= 3; i++ {
			token := TokenWithHashPartition{
				ID:             int64(i),
				Contract:       contract.Address,
				TokenID:        fmt.Sprintf("%d", i),
				TokenURIStatus: "READY",
				Name:           fmt.Sprintf("%s #%d", contract.Name, i),
				Description:    fmt.Sprintf("Token %d for contract %s", i, contract.Name),
				CreatedAt:      time.Now(),
				UpdatedAt:      time.Now(),
			}

			err := testDB.Create(&token).Error
			tassert.NoError(t, err, "Failed to insert token")
			t.Logf("Created token with ID %d, contract %s, tokenID %s", token.ID, token.Contract, token.TokenID)
		}
	}

	// Test 1: Join contracts and tokens using LOWER function with contract address as sharding key
	t.Run("JoinWithLowerFunctionAndContractAddress", func(t *testing.T) {
		var results []struct {
			TokenWithHashPartition
			ContractName string
		}

		// First, pick a specific contract address to query with
		targetContract := contracts[0]

		// Execute the query with a specific contract address (sharding key) and LOWER function
		err := testDB.Table("token_with_hash_partitions").
			Select("token_with_hash_partitions.*, contract_with_hash_partitions.name as contract_name").
			Joins("JOIN contract_with_hash_partitions ON token_with_hash_partitions.contract = contract_with_hash_partitions.address").
			Where("token_with_hash_partitions.contract = ?", targetContract.Address).
			Where("LOWER(contract_with_hash_partitions.name) = LOWER(?)", "MTKN").
			Find(&results).Error

		// Verify no errors and we found the right results
		tassert.NoError(t, err, "Query should execute without errors")
		tassert.Equal(t, 3, len(results), "Should find 3 tokens for the contract")

		t.Logf("Last query: %s", hashMiddleware.LastQuery())

		// Verify the data
		for _, result := range results {
			t.Logf("Found token: ID=%d, Contract=%s, TokenID=%s, ContractName=%s",
				result.ID, result.Contract, result.TokenID, result.ContractName)
			tassert.Equal(t, targetContract.Address, result.Contract, "Contract address should match")
		}
	})

	// Test 2: Query all MTKN tokens across all contracts (simulates your error case)
	t.Run("QueryAllMTKNTokens", func(t *testing.T) {
		var results []struct {
			TokenWithHashPartition
			ContractName string
		}

		// Execute the query without a specific contract
		err := testDB.
			Table("token_with_hash_partitions").Clauses(hints.New("nosharding")).
			Select("token_with_hash_partitions.*, contract_with_hash_partitions.name as contract_name").
			Joins("JOIN contract_with_hash_partitions ON token_with_hash_partitions.contract = contract_with_hash_partitions.address").
			Where("LOWER(contract_with_hash_partitions.name) = LOWER(?)", "MTKN").
			Find(&results).Error

		// Verify no errors with nosharding hint
		tassert.NoError(t, err, "Query with nosharding should execute without errors")

		// Should find tokens for all 4 contract variations of "MTKN"
		t.Logf("Found %d tokens for MTKN contracts with nosharding", len(results))
		tassert.GreaterOrEqual(t, len(results), 3, "Should find at least 3 tokens")
	})

	// Test 3: Query by specific token ID and contract address
	t.Run("QueryByTokenIDAndContract", func(t *testing.T) {
		var results []struct {
			TokenWithHashPartition
			ContractName string
		}

		// Pick a specific contract and token ID
		targetContract := contracts[0]
		targetTokenID := "1"

		// Query with both contract address and token ID
		err := testDB.Table("token_with_hash_partitions").
			Select("token_with_hash_partitions.*, contract_with_hash_partitions.name as contract_name").
			Joins("JOIN contract_with_hash_partitions ON token_with_hash_partitions.contract = contract_with_hash_partitions.address").
			Where("token_with_hash_partitions.contract = ?", targetContract.Address).
			Where("token_with_hash_partitions.token_id = ?", targetTokenID).
			Where("LOWER(contract_with_hash_partitions.name) = LOWER(?)", "MTKN").
			Find(&results).Error

		// Verify results
		tassert.NoError(t, err, "Query should execute without errors")
		tassert.Equal(t, 1, len(results), "Should find exactly 1 token")

		t.Logf("Last query: %s", hashMiddleware.LastQuery())

		if len(results) > 0 {
			t.Logf("Found token: ID=%d, Contract=%s, TokenID=%s, ContractName=%s",
				results[0].ID, results[0].Contract, results[0].TokenID, results[0].ContractName)
			tassert.Equal(t, targetContract.Address, results[0].Contract, "Contract address should match")
			tassert.Equal(t, targetTokenID, results[0].TokenID, "Token ID should match")
		}
	})

	// Test 4: Query with IN clause for contract addresses
	t.Run("QueryWithContractAddressesIN", func(t *testing.T) {
		var results []struct {
			TokenWithHashPartition
			ContractName string
		}

		// Get addresses for contracts with "MTKN" variations
		var targetAddresses []string
		for _, c := range contracts[:4] { // First 4 are MTKN variations
			targetAddresses = append(targetAddresses, c.Address)
		}

		// Query using IN clause for contract addresses + LOWER() function
		// This requires nosharding because we're querying across shards
		err := testDB.
			Table("token_with_hash_partitions").Clauses(hints.New("nosharding")).
			Select("token_with_hash_partitions.*, contract_with_hash_partitions.name as contract_name").
			Joins("JOIN contract_with_hash_partitions ON token_with_hash_partitions.contract = contract_with_hash_partitions.address").
			Where("token_with_hash_partitions.contract IN ?", targetAddresses).
			Where("LOWER(contract_with_hash_partitions.name) = LOWER(?)", "MTKN").
			Find(&results).Error

		tassert.NoError(t, err, "Query with nosharding and IN clause should execute without errors")
		tassert.Equal(t, 3, len(results), "Should find 3 tokens in total")

		t.Logf("Found %d tokens with contract addresses IN clause", len(results))
	})

	// Test 5: Alternative approach with individual queries per contract
	t.Run("IndividualQueriesPerContract", func(t *testing.T) {
		// Get all contract addresses with "MTKN" (case-insensitive)
		var mtkContracts []ContractWithHashPartition
		err := testDB.
			Where("LOWER(name) = LOWER(?)", "MTKN").
			Find(&mtkContracts).Error
		tassert.NoError(t, err, "Query for contracts should succeed")
		tassert.GreaterOrEqual(t, len(mtkContracts), 1, "Should find at least one MTKN contract")

		// Query tokens for one specific contract
		if len(mtkContracts) > 0 {
			var contractTokens []TokenWithHashPartition
			err := testDB.Where("contract = ?", mtkContracts[0].Address).Find(&contractTokens).Error
			tassert.NoError(t, err, "Query for tokens should succeed")

			t.Logf("Found %d tokens for contract %s (%s)", len(contractTokens), mtkContracts[0].Address, mtkContracts[0].Name)
			tassert.Equal(t, 3, len(contractTokens), "Should find exactly 3 tokens for the contract")
		}
	})

	// This is a no-op test to make sure the package is tested correctly
	t.Run("NoOp", func(t *testing.T) {
		tassert.True(t, true, "This test should always pass")
	})
}

func TestTokenMetadataQueryWithSharding(t *testing.T) {
	// Create a test DB with proper configuration
	testDB, err := gorm.Open(postgres.New(dbConfig), &gorm.Config{
		DisableForeignKeyConstraintWhenMigrating: true,
		Logger:                                   logger.Default.LogMode(logger.Info),
	})
	if err != nil {
		t.Fatalf("Failed to connect to database: %v", err)
	}

	// Set up hash partitioning with contract as the sharding key for tokens
	tokenConfig := Config{
		DoubleWrite:         true,
		ShardingKey:         "contract", // contract is the sharding key for tokens
		PartitionType:       PartitionTypeHash,
		NumberOfShards:      4,
		ShardingAlgorithm:   shardingHasher4Algorithm,
		PrimaryKeyGenerator: PKSnowflake,
		ShardingSuffixs: func() []string {
			return []string{"_0", "_1", "_2", "_3"}
		},
	}

	// Register the middleware with the configuration
	configs := map[string]Config{
		"token_with_hash_partitions": tokenConfig,
	}

	middleware := Register(configs, &TokenWithHashPartition{})
	testDB.Use(middleware)

	// Drop and recreate tables
	testDB.Exec("DROP TABLE IF EXISTS token_with_hash_partitions")
	for i := 0; i < 4; i++ {
		testDB.Exec(fmt.Sprintf("DROP TABLE IF EXISTS token_with_hash_partitions_%d", i))
	}

	// Auto migrate to create the tables
	err = testDB.AutoMigrate(&TokenWithHashPartition{})
	if err != nil {
		t.Fatalf("Failed to migrate tables: %v", err)
	}

	// Create sharded tables with all the necessary fields for the metadata query
	for i := 0; i < 4; i++ {
		testDB.Exec(fmt.Sprintf(`CREATE TABLE IF NOT EXISTS token_with_hash_partitions_%d (
			id bigint PRIMARY KEY,
			contract text,
			token_id text,
			token_uri_status text,
			token_uri text,
			name text,
			description text,
			last_token_uri_check timestamp with time zone,
			metadata_status text,
			metadata_content_type text,
			metadata_content text,
			metadata_attempts integer,
			last_metadata_attempt timestamp with time zone,
			created_at timestamp with time zone,
			created_block bigint,
			burned_at timestamp with time zone,
			burned_block bigint,
			error_msg text,
			expired boolean,
			metadata_checks integer,
			last_metadata_check timestamp with time zone,
			updated_at timestamp with time zone
		)`, i))
	}

	// Insert test tokens with different metadata statuses and timestamps
	// We'll create tokens that match the query conditions and some that don't
	now := time.Now()
	twoWeeksAgo := now.Add(-time.Hour * 24 * 14)

	testTokens := []TokenWithHashPartition{
		// Token that matches all conditions (should be returned by the query)
		{
			Contract:            "0xabc123",
			TokenID:             "1",
			TokenURIStatus:      "READY",
			MetadataStatus:      "PENDING",
			LastMetadataCheck:   &twoWeeksAgo,
			LastMetadataAttempt: &twoWeeksAgo,
			BurnedBlock:         nil, // Not burned
		},
		// Token with different metadata status but still matches (should be returned)
		{
			Contract:            "0xabc123",
			TokenID:             "2",
			TokenURIStatus:      "READY",
			MetadataStatus:      "FAILED",
			LastMetadataCheck:   &twoWeeksAgo,
			LastMetadataAttempt: &twoWeeksAgo,
			BurnedBlock:         nil, // Not burned
		},
		// Token with recent metadata check (should NOT be returned)
		{
			Contract:            "0xabc123",
			TokenID:             "3",
			TokenURIStatus:      "READY",
			MetadataStatus:      "PENDING",
			LastMetadataCheck:   &now, // Too recent
			LastMetadataAttempt: &twoWeeksAgo,
			BurnedBlock:         nil, // Not burned
		},
		// Token with recent metadata attempt (should NOT be returned)
		{
			Contract:            "0xabc123",
			TokenID:             "4",
			TokenURIStatus:      "READY",
			MetadataStatus:      "PENDING",
			LastMetadataCheck:   &twoWeeksAgo,
			LastMetadataAttempt: &now, // Too recent
			BurnedBlock:         nil,  // Not burned
		},
		// Token that is burned (should NOT be returned)
		{
			Contract:            "0xabc123",
			TokenID:             "5",
			TokenURIStatus:      "READY",
			MetadataStatus:      "PENDING",
			LastMetadataCheck:   &twoWeeksAgo,
			LastMetadataAttempt: &twoWeeksAgo,
			BurnedBlock:         new(int64), // Burned
		},
		// Token with wrong token_uri_status (should NOT be returned)
		{
			Contract:            "0xabc123",
			TokenID:             "6",
			TokenURIStatus:      "FAILED", // Wrong status
			MetadataStatus:      "PENDING",
			LastMetadataCheck:   &twoWeeksAgo,
			LastMetadataAttempt: &twoWeeksAgo,
			BurnedBlock:         nil, // Not burned
		},
		// Token with different contract (for testing sharding key)
		{
			Contract:            "0xdef456",
			TokenID:             "1",
			TokenURIStatus:      "READY",
			MetadataStatus:      "PENDING",
			LastMetadataCheck:   &twoWeeksAgo,
			LastMetadataAttempt: &twoWeeksAgo,
			BurnedBlock:         nil, // Not burned
		},
	}

	// Insert the test tokens
	for _, token := range testTokens {
		err := testDB.Create(&token).Error
		tassert.NoError(t, err, "Failed to insert token")
		t.Logf("Created token with contract %s, tokenID %s", token.Contract, token.TokenID)
	}

	// Test 1: Reproduce the error - Query without specifying the sharding key
	t.Run("ReproduceError_MissingShardingKey", func(t *testing.T) {
		var results []TokenWithHashPartition

		// This query should fail because it doesn't specify the contract (sharding key)
		err := testDB.Model(&TokenWithHashPartition{}).
			Where("(metadata_status = ? OR metadata_status = ?) AND burned_block IS NULL AND token_uri_status != ?",
				"PENDING", "FAILED", "FAILED").
			Where("last_metadata_check IS NOT NULL AND last_metadata_check < NOW() - INTERVAL '10080 minutes'").
			Where("last_metadata_attempt IS NOT NULL AND last_metadata_attempt < NOW() - INTERVAL '10080 minutes'").
			Order("last_metadata_check").
			Limit(10000).
			Find(&results).Error

		// This should fail with "sharding key or id required, and use operator ="
		tassert.Error(t, err, "Query should fail with sharding key error")
		//tassert.Contains(t, err.Error(), "sharding key or id required", "Error should mention sharding key requirement")
	})

	// Test 2: Fix 1 - Add the sharding key condition
	t.Run("Fix1_AddShardingKey", func(t *testing.T) {
		var results []TokenWithHashPartition

		// This query should succeed because it includes the contract (sharding key)
		err := testDB.Model(&TokenWithHashPartition{}).
			Where("contract = ?", "0xabc123"). // Add sharding key
			Where("(metadata_status = ? OR metadata_status = ?) AND burned_block IS NULL AND token_uri_status != ?",
				"PENDING", "FAILED", "FAILED").
			Where("last_metadata_check IS NOT NULL AND last_metadata_check < NOW() - INTERVAL '10080 minutes'").
			Where("last_metadata_attempt IS NOT NULL AND last_metadata_attempt < NOW() - INTERVAL '10080 minutes'").
			Order("last_metadata_check").
			Limit(10000).
			Find(&results).Error

		tassert.NoError(t, err, "Query with sharding key should succeed")
		tassert.GreaterOrEqual(t, len(results), 2, "Should find at least 2 matching tokens")

		t.Logf("Found %d results with sharding key", len(results))
		t.Logf("Last query: %s", middleware.LastQuery())
	})

	// Test 3: Fix 2 - Use nosharding hint for cross-shard queries
	t.Run("Fix2_UseNoshardingHint", func(t *testing.T) {
		var results []TokenWithHashPartition

		// This query should succeed because it uses the nosharding hint
		err := testDB.Model(&TokenWithHashPartition{}).
			Clauses(hints.New("nosharding")).
			Where("(metadata_status = ? OR metadata_status = ?) AND burned_block IS NULL AND token_uri_status != ?",
				"PENDING", "FAILED", "FAILED").
			Where("last_metadata_check IS NOT NULL AND last_metadata_check < NOW() - INTERVAL '10080 minutes'").
			Where("last_metadata_attempt IS NOT NULL AND last_metadata_attempt < NOW() - INTERVAL '10080 minutes'").
			Order("last_metadata_check").
			Limit(10000).
			Find(&results).Error

		tassert.NoError(t, err, "Query with nosharding hint should succeed")
		tassert.GreaterOrEqual(t, len(results), 3, "Should find at least 3 matching tokens across all shards")

		t.Logf("Found %d results with nosharding hint", len(results))
		t.Logf("Last query: %s", middleware.LastQuery())
	})

	// Test 4: Fix 3 - Use IN clause for multiple contracts
	t.Run("Fix3_UseInClauseForMultipleContracts", func(t *testing.T) {
		var results []TokenWithHashPartition

		// This query should succeed because it uses IN clause for multiple contracts
		err := testDB.Model(&TokenWithHashPartition{}).
			Where("contract IN ?", []string{"0xabc123", "0xdef456"}). // Multiple contracts
			Where("(metadata_status = ? OR metadata_status = ?) AND burned_block IS NULL AND token_uri_status != ?",
				"PENDING", "FAILED", "FAILED").
			Where("last_metadata_check IS NOT NULL AND last_metadata_check < NOW() - INTERVAL '10080 minutes'").
			Where("last_metadata_attempt IS NOT NULL AND last_metadata_attempt < NOW() - INTERVAL '10080 minutes'").
			Order("last_metadata_check").
			Limit(10000).
			Find(&results).Error

		tassert.NoError(t, err, "Query with IN clause should succeed")
		tassert.GreaterOrEqual(t, len(results), 3, "Should find at least 3 matching tokens")

		t.Logf("Found %d results with IN clause", len(results))
		t.Logf("Last query: %s", middleware.LastQuery())
	})

	// Test 5: Fix 4 - Use separate queries per contract and combine results
	t.Run("Fix4_SeparateQueriesPerContract", func(t *testing.T) {
		var allResults []TokenWithHashPartition
		contracts := []string{"0xabc123", "0xdef456"}

		// Query each contract separately and combine results
		for _, contract := range contracts {
			var results []TokenWithHashPartition
			err := testDB.Model(&TokenWithHashPartition{}).
				Where("contract = ?", contract).
				Where("(metadata_status = ? OR metadata_status = ?) AND burned_block IS NULL AND token_uri_status != ?",
					"PENDING", "FAILED", "FAILED").
				Where("last_metadata_check IS NOT NULL AND last_metadata_check < NOW() - INTERVAL '10080 minutes'").
				Where("last_metadata_attempt IS NOT NULL AND last_metadata_attempt < NOW() - INTERVAL '10080 minutes'").
				Order("last_metadata_check").
				Limit(10000).
				Find(&results).Error

			tassert.NoError(t, err, "Query for contract should succeed")
			allResults = append(allResults, results...)
		}

		tassert.GreaterOrEqual(t, len(allResults), 3, "Should find at least 3 matching tokens across all contracts")
		t.Logf("Found %d total results with separate queries", len(allResults))
	})
}
