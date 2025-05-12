package sharding

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gorm.io/driver/postgres"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
)

// TestCreateOrReplaceViewWithSharding tests that CREATE OR REPLACE VIEW statements work correctly with sharding
func TestCreateOrReplaceViewWithSharding(t *testing.T) {
	// Create a test database connection
	dbConfig := postgres.Config{
		DSN:                  dbURL(),
		PreferSimpleProtocol: true,
	}
	testDB, err := gorm.Open(postgres.New(dbConfig), &gorm.Config{
		DisableForeignKeyConstraintWhenMigrating: true,
		Logger:                                   logger.Default.LogMode(logger.Info),
	})
	require.NoError(t, err, "Failed to connect to database")

	// Configure sharding for generic_balances table with contract as the sharding key
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
		"generic_balances": balanceConfig,
	}
	middleware := Register(configs, &GenericBalance{})
	testDB.Use(middleware)

	// Create the generic_balances table and sharded tables
	err = testDB.AutoMigrate(&GenericBalance{})
	require.NoError(t, err, "Failed to migrate generic_balances table")

	// Clean up existing test data and views
	testDB.Exec("DROP VIEW IF EXISTS holders_0xcontract5")
	testDB.Exec("TRUNCATE TABLE generic_balances")

	// Get the shard suffix for our test contract
	testContract := "0xcontract5"
	suffix, err := balanceConfig.ShardingAlgorithm(testContract)
	require.NoError(t, err, "Failed to calculate shard suffix for contract")
	shardedTable := "generic_balances" + suffix
	testDB.Exec("TRUNCATE TABLE " + shardedTable)

	// Insert test data
	testData := []GenericBalance{
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
	}

	for _, record := range testData {
		err := testDB.Create(&record).Error
		require.NoError(t, err, "Failed to insert test data")
	}

	// Test: Create a view using CREATE OR REPLACE VIEW with sharding
	t.Run("CreateOrReplaceViewWithSharding", func(t *testing.T) {
		// Create the view using CREATE OR REPLACE VIEW with a WHERE clause that includes the sharding key
		viewQuery := `
			CREATE OR REPLACE VIEW "holders_0xcontract5" AS 
			SELECT COUNT(DISTINCT(account || owner)) 
			FROM "generic_balances" 
			WHERE contract = '0xcontract5' AND (type = 'ERC721' OR (value IS NOT NULL AND value > 0))
		`
		err := testDB.Exec(viewQuery).Error
		assert.NoError(t, err, "CREATE OR REPLACE VIEW with sharding should work correctly")

		// Query the view to verify it returns the correct count
		var viewCount int64
		err = testDB.Raw("SELECT * FROM holders_" + testContract).Scan(&viewCount).Error
		require.NoError(t, err, "Failed to query the view")
		assert.Equal(t, int64(4), viewCount, "View should return 4 distinct account+owner combinations")

		// Verify the query was routed to the correct shard
		lastQuery := middleware.LastQuery()
		assert.Contains(t, lastQuery, shardedTable, "Query should be routed to the correct shard table")
	})

	// Clean up
	testDB.Exec("DROP VIEW IF EXISTS holders_" + testContract)
	testDB.Exec("TRUNCATE TABLE generic_balances")
	testDB.Exec("TRUNCATE TABLE " + shardedTable)
}
