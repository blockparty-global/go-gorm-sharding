package sharding

import (
	"gorm.io/gorm/logger"
	"testing"

	"github.com/stretchr/testify/assert"
	"gorm.io/driver/postgres"
	"gorm.io/gorm"
)

// BalanceBalance represents the balance_balances table structure
type BalanceBalance struct {
	ID       int64   `gorm:"primarykey"`
	Contract string  `gorm:"index:idx_contract"` // Sharding key
	Account  string  `gorm:"index:idx_account"`
	Owner    string  `gorm:"index:idx_owner"`
	Type     string  `gorm:"index:idx_type"`
	Value    *string `gorm:"type:numeric"`
}

// TableName specifies the table name for BalanceBalance
func (BalanceBalance) TableName() string {
	return "balance_balances"
}

// TestBalanceQueryWithSharding tests the query for counting distinct account+owner combinations
// with contract as the sharding key
func TestBalanceQueryWithSharding(t *testing.T) {
	// Create a test database connection
	dbConfig := postgres.Config{
		DSN:                  dbURL(),
		PreferSimpleProtocol: true,
	}
	testDB, err := gorm.Open(postgres.New(dbConfig), &gorm.Config{
		DisableForeignKeyConstraintWhenMigrating: true,
		Logger:                                   logger.Default.LogMode(logger.Info),
	})
	assert.NoError(t, err, "Failed to connect to database")

	// Configure sharding for balance_balances table with contract as the sharding key
	balanceConfig := Config{
		DoubleWrite:    true,
		ShardingKey:    "contract",
		NumberOfShards: 4,
		PartitionType:  PartitionTypeHash,
		// Use the hash algorithm for contract addresses
		ShardingAlgorithm: shardingHasher4Algorithm,
		ShardingSuffixs: func() []string {
			return []string{"_0", "_1", "_2", "_3"}
		},
	}

	// Register the sharding middleware
	configs := map[string]Config{
		"balance_balances": balanceConfig,
	}
	middleware := Register(configs, &BalanceBalance{})
	testDB.Use(middleware)

	// Create the balance_balances table and sharded tables
	err = testDB.AutoMigrate(&BalanceBalance{})
	assert.NoError(t, err, "Failed to migrate balance_balances table")

	// Clean up existing test data
	testDB.Exec("TRUNCATE TABLE balance_balances")

	// Get the shard suffix for our test contract
	testContract := "0xdac17f958d2ee523a2206206994597c13d831ec7"
	suffix, err := balanceConfig.ShardingAlgorithm(testContract)
	assert.NoError(t, err, "Failed to calculate shard suffix")

	shardedTable := "balance_balances" + suffix
	testDB.Exec("TRUNCATE TABLE " + shardedTable)

	// Insert test data
	testData := []BalanceBalance{
		// Records that should be counted (ERC721 type)
		{Contract: testContract, Account: "account1", Owner: "owner1", Type: "ERC721", Value: nil},
		{Contract: testContract, Account: "account2", Owner: "owner2", Type: "ERC721", Value: nil},

		// Records that should be counted (value > 0)
		{Contract: testContract, Account: "account3", Owner: "owner3", Type: "ERC20", Value: strPtr("100")},
		{Contract: testContract, Account: "account4", Owner: "owner4", Type: "ERC20", Value: strPtr("200")},

		// Records that should NOT be counted (value = 0)
		{Contract: testContract, Account: "account5", Owner: "owner5", Type: "ERC20", Value: strPtr("0")},

		// Records that should NOT be counted (value is NULL and not ERC721)
		{Contract: testContract, Account: "account6", Owner: "owner6", Type: "ERC20", Value: nil},

		// Records that should NOT be counted (different contract)
		{Contract: "0xdifferent", Account: "account7", Owner: "owner7", Type: "ERC721", Value: nil},
		{Contract: "0xdifferent", Account: "account8", Owner: "owner8", Type: "ERC20", Value: strPtr("300")},

		// Duplicate account+owner combination that should be counted only once
		{Contract: testContract, Account: "account1", Owner: "owner1", Type: "ERC721", Value: nil},
	}

	for _, record := range testData {
		err := testDB.Create(&record).Error
		assert.NoError(t, err, "Failed to insert test data")
	}

	// Execute the query
	var result struct {
		Count int64
	}

	query := `
		SELECT count(DISTINCT balance_balances.account || balance_balances.owner) AS count
		FROM balance_balances
		WHERE balance_balances.contract = ? 
		AND (balance_balances.type = 'ERC721' OR balance_balances.value IS NOT NULL AND balance_balances.value > 0::numeric)
	`

	err = testDB.Raw(query, testContract).Scan(&result).Error
	assert.NoError(t, err, "Failed to execute query")

	// We expect 4 distinct account+owner combinations:
	// 1. account1+owner1 (ERC721)
	// 2. account2+owner2 (ERC721)
	// 3. account3+owner3 (value = 100)
	// 4. account4+owner4 (value = 200)
	assert.Equal(t, int64(4), result.Count, "Query should return 4 distinct account+owner combinations")

	// Verify the query was routed to the correct shard
	lastQuery := middleware.LastQuery()
	assert.Contains(t, lastQuery, shardedTable, "Query should be routed to the correct shard table")

	// Clean up
	testDB.Exec("TRUNCATE TABLE balance_balances")
	testDB.Exec("TRUNCATE TABLE " + shardedTable)
}

// TestSpecificContractQueryWithSharding tests the exact query provided in the requirements
// with contract as the sharding key
func TestSpecificContractQueryWithSharding(t *testing.T) {
	// Create a test database connection
	dbConfig := postgres.Config{
		DSN:                  dbURL(),
		PreferSimpleProtocol: true,
	}
	testDB, err := gorm.Open(postgres.New(dbConfig), &gorm.Config{
		DisableForeignKeyConstraintWhenMigrating: true,
		Logger:                                   logger.Default.LogMode(logger.Info),
	})
	assert.NoError(t, err, "Failed to connect to database")

	// Configure sharding for balance_balances table with contract as the sharding key
	balanceConfig := Config{
		DoubleWrite:    true,
		ShardingKey:    "contract",
		NumberOfShards: 4,
		PartitionType:  PartitionTypeHash,
		// Use the hash algorithm for contract addresses
		ShardingAlgorithm: shardingHasher4Algorithm,
		ShardingSuffixs: func() []string {
			return []string{"_0", "_1", "_2", "_3"}
		},
	}

	// Register the sharding middleware
	configs := map[string]Config{
		"balance_balances": balanceConfig,
	}
	middleware := Register(configs, &BalanceBalance{})
	testDB.Use(middleware)

	// Create the balance_balances table and sharded tables
	err = testDB.AutoMigrate(&BalanceBalance{})
	assert.NoError(t, err, "Failed to migrate balance_balances table")

	// Clean up existing test data
	testDB.Exec("TRUNCATE TABLE balance_balances")

	// Get the shard suffix for our test contract
	testContract := "0xdac17f958d2ee523a2206206994597c13d831ec7"
	suffix, err := balanceConfig.ShardingAlgorithm(testContract)
	assert.NoError(t, err, "Failed to calculate shard suffix")

	shardedTable := "balance_balances" + suffix
	testDB.Exec("TRUNCATE TABLE " + shardedTable)

	// Insert test data
	testData := []BalanceBalance{
		// Records that should be counted (ERC721 type)
		{Contract: testContract, Account: "account1", Owner: "owner1", Type: "ERC721", Value: nil},
		{Contract: testContract, Account: "account2", Owner: "owner2", Type: "ERC721", Value: nil},

		// Records that should be counted (value > 0)
		{Contract: testContract, Account: "account3", Owner: "owner3", Type: "ERC20", Value: strPtr("100")},
		{Contract: testContract, Account: "account4", Owner: "owner4", Type: "ERC20", Value: strPtr("200")},

		// Records that should NOT be counted (value = 0)
		{Contract: testContract, Account: "account5", Owner: "owner5", Type: "ERC20", Value: strPtr("0")},

		// Records that should NOT be counted (value is NULL and not ERC721)
		{Contract: testContract, Account: "account6", Owner: "owner6", Type: "ERC20", Value: nil},

		// Records that should NOT be counted (different contract)
		{Contract: "0xdifferent", Account: "account7", Owner: "owner7", Type: "ERC721", Value: nil},
		{Contract: "0xdifferent", Account: "account8", Owner: "owner8", Type: "ERC20", Value: strPtr("300")},

		// Duplicate account+owner combination that should be counted only once
		{Contract: testContract, Account: "account1", Owner: "owner1", Type: "ERC721", Value: nil},
	}

	for _, record := range testData {
		err := testDB.Create(&record).Error
		assert.NoError(t, err, "Failed to insert test data")
	}

	// Execute the exact query from the requirements
	var result struct {
		Count int64
	}

	// This is the exact query from the requirements
	query := `
		SELECT count(DISTINCT balance_balances.account || balance_balances.owner) AS count
		FROM balance_balances
		WHERE balance_balances.contract = '0xdac17f958d2ee523a2206206994597c13d831ec7' 
		AND (balance_balances.type = 'ERC721' OR balance_balances.value IS NOT NULL AND balance_balances.value > 0::numeric)
	`

	err = testDB.Raw(query).Scan(&result).Error
	assert.NoError(t, err, "Failed to execute query")

	// We expect 4 distinct account+owner combinations:
	// 1. account1+owner1 (ERC721)
	// 2. account2+owner2 (ERC721)
	// 3. account3+owner3 (value = 100)
	// 4. account4+owner4 (value = 200)
	assert.Equal(t, int64(4), result.Count, "Query should return 4 distinct account+owner combinations")

	// Verify the query was routed to the correct shard
	lastQuery := middleware.LastQuery()
	assert.Contains(t, lastQuery, shardedTable, "Query should be routed to the correct shard table")

	// Verify that the query was properly rewritten to use the sharded table
	assert.NotContains(t, lastQuery, "balance_balances WHERE", "Query should not use the base table")
	assert.Contains(t, lastQuery, shardedTable+" WHERE", "Query should use the sharded table")

	// Clean up
	testDB.Exec("TRUNCATE TABLE balance_balances")
	testDB.Exec("TRUNCATE TABLE " + shardedTable)
}

// TestMultipleContractsSharding tests that queries for different contracts
// are correctly routed to their respective shards
func TestMultipleContractsSharding(t *testing.T) {
	// Create a test database connection
	dbConfig := postgres.Config{
		DSN:                  dbURL(),
		PreferSimpleProtocol: true,
	}
	testDB, err := gorm.Open(postgres.New(dbConfig), &gorm.Config{
		DisableForeignKeyConstraintWhenMigrating: true,
		Logger:                                   logger.Default.LogMode(logger.Info),
	})
	assert.NoError(t, err, "Failed to connect to database")

	// Configure sharding for balance_balances table with contract as the sharding key
	balanceConfig := Config{
		DoubleWrite:    true,
		ShardingKey:    "contract",
		NumberOfShards: 4,
		PartitionType:  PartitionTypeHash,
		// Use the hash algorithm for contract addresses
		ShardingAlgorithm: shardingHasher4Algorithm,
		ShardingSuffixs: func() []string {
			return []string{"_0", "_1", "_2", "_3"}
		},
	}

	// Register the sharding middleware
	configs := map[string]Config{
		"balance_balances": balanceConfig,
	}
	middleware := Register(configs, &BalanceBalance{})
	testDB.Use(middleware)

	// Create the balance_balances table and sharded tables
	err = testDB.AutoMigrate(&BalanceBalance{})
	assert.NoError(t, err, "Failed to migrate balance_balances table")

	// Clean up existing test data
	testDB.Exec("TRUNCATE TABLE balance_balances")

	// Define multiple test contracts
	testContracts := []string{
		"0xdac17f958d2ee523a2206206994597c13d831ec7", // USDT
		"0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48", // USDC
		"0x2260fac5e5542a773aa44fbcfedf7c193bc2c599", // WBTC
	}

	// Map to store shard tables for each contract
	shardTables := make(map[string]string)

	// Get shard suffix for each contract and clean up tables
	for _, contract := range testContracts {
		suffix, err := balanceConfig.ShardingAlgorithm(contract)
		assert.NoError(t, err, "Failed to calculate shard suffix for contract "+contract)

		shardTable := "balance_balances" + suffix
		shardTables[contract] = shardTable

		testDB.Exec("TRUNCATE TABLE " + shardTable)
	}

	// Insert test data for each contract
	for _, contract := range testContracts {
		// Insert records for this contract
		testData := []BalanceBalance{
			// Records that should be counted (ERC721 type)
			{Contract: contract, Account: "account1", Owner: "owner1", Type: "ERC721", Value: nil},

			// Records that should be counted (value > 0)
			{Contract: contract, Account: "account2", Owner: "owner2", Type: "ERC20", Value: strPtr("100")},
		}

		for _, record := range testData {
			err := testDB.Create(&record).Error
			assert.NoError(t, err, "Failed to insert test data for contract "+contract)
		}
	}

	// Test query for each contract
	for _, contract := range testContracts {
		// Execute the query for this contract
		var result struct {
			Count int64
		}

		query := `
			SELECT count(DISTINCT balance_balances.account || balance_balances.owner) AS count
			FROM balance_balances
			WHERE balance_balances.contract = ? 
			AND (balance_balances.type = 'ERC721' OR balance_balances.value IS NOT NULL AND balance_balances.value > 0::numeric)
		`

		err = testDB.Raw(query, contract).Scan(&result).Error
		assert.NoError(t, err, "Failed to execute query for contract "+contract)

		// We expect 2 distinct account+owner combinations for each contract
		assert.Equal(t, int64(2), result.Count, "Query should return 2 distinct account+owner combinations for contract "+contract)

		// Verify the query was routed to the correct shard
		lastQuery := middleware.LastQuery()
		expectedShardTable := shardTables[contract]
		assert.Contains(t, lastQuery, expectedShardTable, "Query should be routed to the correct shard table for contract "+contract)
	}

	// Clean up
	for _, shardTable := range shardTables {
		testDB.Exec("TRUNCATE TABLE " + shardTable)
	}
	testDB.Exec("TRUNCATE TABLE balance_balances")
}

// Helper function to create a string pointer
func strPtr(s string) *string {
	return &s
}
