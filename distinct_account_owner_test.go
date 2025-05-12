package sharding

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"gorm.io/driver/postgres"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
)

// TestDistinctAccountOwnerConcatenation tests a query that counts distinct combinations
// of account and owner using concatenation with contract as the sharding key
func TestDistinctAccountOwnerConcatenation(t *testing.T) {
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
	testContract := "0xf62c45f81b8978cb14406bb35be8be23eef3742d"
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

		// Same account but different owners (should be counted separately)
		{Contract: testContract, Account: "account9", Owner: "owner9a", Type: "ERC721", Value: nil},
		{Contract: testContract, Account: "account9", Owner: "owner9b", Type: "ERC721", Value: nil},

		// Same owner but different accounts (should be counted separately)
		{Contract: testContract, Account: "account10a", Owner: "owner10", Type: "ERC721", Value: nil},
		{Contract: testContract, Account: "account10b", Owner: "owner10", Type: "ERC721", Value: nil},
	}

	for _, record := range testData {
		err := testDB.Create(&record).Error
		assert.NoError(t, err, "Failed to insert test data")
	}

	// Execute the query
	var result struct {
		Count int64
	}

	// This is the exact query from the requirements
	query := `
		SELECT COUNT(DISTINCT(account || owner)) FROM balance_balances 
		WHERE contract = '0xf62c45f81b8978cb14406bb35be8be23eef3742d' 
		AND (type = 'ERC721' OR (value IS NOT NULL AND value > 0))
	`

	err = testDB.Raw(query).Scan(&result).Error
	assert.NoError(t, err, "Failed to execute query")

	// We expect 8 distinct account+owner combinations:
	// 1. account1+owner1 (ERC721)
	// 2. account2+owner2 (ERC721)
	// 3. account3+owner3 (value = 100)
	// 4. account4+owner4 (value = 200)
	// 5. account9+owner9a (ERC721)
	// 6. account9+owner9b (ERC721)
	// 7. account10a+owner10 (ERC721)
	// 8. account10b+owner10 (ERC721)
	assert.Equal(t, int64(8), result.Count, "Query should return 8 distinct account+owner combinations")

	// Verify the query was routed to the correct shard
	lastQuery := middleware.LastQuery()
	assert.Contains(t, lastQuery, shardedTable, "Query should be routed to the correct shard table")

	// Verify that the query was properly rewritten to use the sharded table
	assert.NotContains(t, lastQuery, "balance_balances WHERE", "Query should not use the base table")
	assert.Contains(t, lastQuery, shardedTable+" WHERE", "Query should use the sharded table")

	// Test with parameterized query
	var paramResult struct {
		Count int64
	}

	paramQuery := `
		SELECT COUNT(DISTINCT(account || owner)) FROM balance_balances 
		WHERE contract = ? 
		AND (type = 'ERC721' OR (value IS NOT NULL AND value > 0))
	`

	err = testDB.Raw(paramQuery, testContract).Scan(&paramResult).Error
	assert.NoError(t, err, "Failed to execute parameterized query")
	assert.Equal(t, int64(8), paramResult.Count, "Parameterized query should return 8 distinct account+owner combinations")

	// Clean up
	testDB.Exec("TRUNCATE TABLE balance_balances")
	testDB.Exec("TRUNCATE TABLE " + shardedTable)
}

// TestDistinctAccountOwnerWithDifferentContracts tests the query with multiple contracts
// to ensure proper sharding behavior
func TestDistinctAccountOwnerWithDifferentContracts(t *testing.T) {
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
		"0xf62c45f81b8978cb14406bb35be8be23eef3742d", // Contract 1
		"0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48", // Contract 2 (USDC)
		"0x2260fac5e5542a773aa44fbcfedf7c193bc2c599", // Contract 3 (WBTC)
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
	for i, contract := range testContracts {
		// Insert records for this contract
		testData := []BalanceBalance{
			// Records that should be counted (ERC721 type)
			{Contract: contract, Account: "account1", Owner: "owner1", Type: "ERC721", Value: nil},
			{Contract: contract, Account: "account2", Owner: "owner2", Type: "ERC721", Value: nil},

			// Records that should be counted (value > 0)
			{Contract: contract, Account: "account3", Owner: "owner3", Type: "ERC20", Value: strPtr("100")},
			{Contract: contract, Account: "account4", Owner: "owner4", Type: "ERC20", Value: strPtr("200")},

			// Records that should NOT be counted (value = 0)
			{Contract: contract, Account: "account5", Owner: "owner5", Type: "ERC20", Value: strPtr("0")},

			// Records that should NOT be counted (value is NULL and not ERC721)
			{Contract: contract, Account: "account6", Owner: "owner6", Type: "ERC20", Value: nil},

			// Duplicate account+owner combination that should be counted only once
			{Contract: contract, Account: "account1", Owner: "owner1", Type: "ERC721", Value: nil},

			// Contract-specific records to ensure unique counts per contract
			{Contract: contract, Account: "unique", Owner: "contract" + string(rune('A'+i)), Type: "ERC721", Value: nil},
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
			SELECT COUNT(DISTINCT(account || owner)) FROM balance_balances 
			WHERE contract = ? 
			AND (type = 'ERC721' OR (value IS NOT NULL AND value > 0))
		`

		err = testDB.Raw(query, contract).Scan(&result).Error
		assert.NoError(t, err, "Failed to execute query for contract "+contract)

		// We expect 5 distinct account+owner combinations for each contract:
		// 1. account1+owner1 (ERC721)
		// 2. account2+owner2 (ERC721)
		// 3. account3+owner3 (value = 100)
		// 4. account4+owner4 (value = 200)
		// 5. unique+contractX (ERC721, where X is specific to the contract)
		assert.Equal(t, int64(5), result.Count, "Query should return 5 distinct account+owner combinations for contract "+contract)

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

// TestDistinctAccountOwnerWithVariousConditions tests the query with various edge cases
// to ensure the DISTINCT(account || owner) works correctly with different conditions
func TestDistinctAccountOwnerWithVariousConditions(t *testing.T) {
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
		DoubleWrite:       true,
		ShardingKey:       "contract",
		NumberOfShards:    4,
		PartitionType:     PartitionTypeHash,
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

	// Test contract address
	testContract := "0xf62c45f81b8978cb14406bb35be8be23eef3742d"
	suffix, err := balanceConfig.ShardingAlgorithm(testContract)
	assert.NoError(t, err, "Failed to calculate shard suffix")
	shardedTable := "balance_balances" + suffix
	testDB.Exec("TRUNCATE TABLE " + shardedTable)

	// Insert test data with various edge cases
	testData := []BalanceBalance{
		// Basic cases that should be counted
		{Contract: testContract, Account: "account1", Owner: "owner1", Type: "ERC721", Value: nil},
		{Contract: testContract, Account: "account2", Owner: "owner2", Type: "ERC20", Value: strPtr("100")},

		// Empty strings for account/owner (should still be counted as distinct)
		{Contract: testContract, Account: "", Owner: "owner3", Type: "ERC721", Value: nil},
		{Contract: testContract, Account: "account4", Owner: "", Type: "ERC721", Value: nil},
		{Contract: testContract, Account: "", Owner: "", Type: "ERC721", Value: nil},

		// Special characters in account/owner (should be handled correctly)
		{Contract: testContract, Account: "account-5", Owner: "owner.5", Type: "ERC721", Value: nil},
		{Contract: testContract, Account: "account@6", Owner: "owner#6", Type: "ERC20", Value: strPtr("200")},

		// Long but reasonable account/owner strings
		{Contract: testContract, Account: "account7" + strings.Repeat("x", 20), Owner: "owner7", Type: "ERC721", Value: nil},
		{Contract: testContract, Account: "account8", Owner: "owner8" + strings.Repeat("x", 20), Type: "ERC20", Value: strPtr("300")},

		// Duplicate combinations with different types/values (should be counted once)
		{Contract: testContract, Account: "dup", Owner: "dup", Type: "ERC721", Value: nil},
		{Contract: testContract, Account: "dup", Owner: "dup", Type: "ERC20", Value: strPtr("400")},

		// Cases that should NOT be counted
		{Contract: testContract, Account: "no1", Owner: "no1", Type: "ERC20", Value: nil},           // Not ERC721 and value is NULL
		{Contract: testContract, Account: "no2", Owner: "no2", Type: "ERC20", Value: strPtr("0")},   // Value is 0
		{Contract: testContract, Account: "no3", Owner: "no3", Type: "ERC20", Value: strPtr("-10")}, // Value is negative

		// Different contract (should not be counted)
		{Contract: "0xdifferent", Account: "wrong", Owner: "wrong", Type: "ERC721", Value: nil},
	}

	for _, record := range testData {
		err := testDB.Create(&record).Error
		assert.NoError(t, err, "Failed to insert test data")
	}

	// Execute the query with the exact SQL statement
	var result struct {
		Count int64
	}

	query := `
		SELECT COUNT(DISTINCT(account || owner)) FROM balance_balances
		WHERE contract = '0xf62c45f81b8978cb14406bb35be8be23eef3742d'
		AND (type = 'ERC721' OR (value IS NOT NULL AND value > 0))
	`

	err = testDB.Raw(query).Scan(&result).Error
	assert.NoError(t, err, "Failed to execute query")

	// We expect 10 distinct account+owner combinations:
	// 1. account1+owner1 (ERC721)
	// 2. account2+owner2 (ERC20, value=100)
	// 3. +owner3 (empty account, ERC721)
	// 4. account4+ (empty owner, ERC721)
	// 5. + (both empty, ERC721)
	// 6. account-5+owner.5 (special chars, ERC721)
	// 7. account@6+owner#6 (special chars, ERC20, value=200)
	// 8. account7[long]+owner7 (long account, ERC721)
	// 9. account8+owner8[long] (long owner, ERC20, value=300)
	// 10. dup+dup (duplicate combinations, counted once)
	assert.Equal(t, int64(10), result.Count, "Query should return 10 distinct account+owner combinations")

	// Test with parameterized query
	var paramResult struct {
		Count int64
	}

	paramQuery := `
		SELECT COUNT(DISTINCT(account || owner)) FROM balance_balances
		WHERE contract = ?
		AND (type = 'ERC721' OR (value IS NOT NULL AND value > 0))
	`

	err = testDB.Raw(paramQuery, testContract).Scan(&paramResult).Error
	assert.NoError(t, err, "Failed to execute parameterized query")
	assert.Equal(t, int64(10), paramResult.Count, "Parameterized query should return 10 distinct account+owner combinations")

	// Clean up
	testDB.Exec("TRUNCATE TABLE balance_balances")
	testDB.Exec("TRUNCATE TABLE " + shardedTable)
}
