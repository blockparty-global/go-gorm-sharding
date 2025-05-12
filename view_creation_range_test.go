package sharding

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gorm.io/driver/postgres"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
)

// TestCreateHoldersViewWithRangePartitioning tests that the view creation works correctly
// with range partitioning strategy
func TestCreateHoldersViewWithRangePartitioning(t *testing.T) {
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

	// Configure hash partitioning for generic_balances table with ID as the sharding key
	// We'll simulate range partitioning by using a custom sharding algorithm
	rangeConfig := Config{
		DoubleWrite:    true,
		ShardingKey:    "id",
		NumberOfShards: 3,
		PartitionType:  PartitionTypeHash,
		ShardingSuffixs: func() []string {
			return []string{"_0", "_1", "_2"}
		},
		// Custom sharding algorithm that simulates range partitioning
		ShardingAlgorithm: func(value any) (string, error) {
			var id int64
			switch v := value.(type) {
			case int:
				id = int64(v)
			case int64:
				id = v
			case int32:
				id = int64(v)
			default:
				return "", fmt.Errorf("unsupported type for ID range partitioning: %T", value)
			}

			// Range partitioning logic
			if id < 1000 {
				return "_0", nil // Shard 0: id < 1000
			} else if id < 10000 {
				return "_1", nil // Shard 1: 1000 <= id < 10000
			} else {
				return "_2", nil // Shard 2: id >= 10000
			}
		},
	}

	// Register the sharding middleware
	configs := map[string]Config{
		"generic_balances": rangeConfig,
	}
	middleware := Register(configs, &GenericBalance{})
	testDB.Use(middleware)

	// Create the generic_balances table and sharded tables
	err = testDB.AutoMigrate(&GenericBalance{})
	require.NoError(t, err, "Failed to migrate generic_balances table")

	// Clean up existing test data and views
	testDB.Exec("DROP VIEW IF EXISTS holders_0xcontract4")
	testDB.Exec("TRUNCATE TABLE generic_balances")
	testDB.Exec("TRUNCATE TABLE generic_balances_0")
	testDB.Exec("TRUNCATE TABLE generic_balances_1")
	testDB.Exec("TRUNCATE TABLE generic_balances_2")

	// Insert test data across different range partitions
	testContract := "0xcontract4"
	testData := []GenericBalance{
		// Shard 0: id < 1000
		{ID: 100, Contract: testContract, Account: "account1", Owner: "owner1", Type: "ERC721", Value: nil},
		{ID: 500, Contract: testContract, Account: "account2", Owner: "owner2", Type: "ERC721", Value: nil},

		// Shard 1: 1000 <= id < 10000
		{ID: 1500, Contract: testContract, Account: "account3", Owner: "owner3", Type: "ERC20", Value: strPtr("100")},
		{ID: 5000, Contract: testContract, Account: "account4", Owner: "owner4", Type: "ERC20", Value: strPtr("200")},
		{ID: 9000, Contract: testContract, Account: "account5", Owner: "owner5", Type: "ERC20", Value: strPtr("0")},

		// Shard 2: id >= 10000
		{ID: 15000, Contract: testContract, Account: "account6", Owner: "owner6", Type: "ERC20", Value: nil},
		{ID: 20000, Contract: testContract, Account: "account7", Owner: "owner7", Type: "ERC1155", Value: strPtr("300")},
	}

	for _, record := range testData {
		err := testDB.Create(&record).Error
		require.NoError(t, err, "Failed to insert test data")
	}

	// Create a service instance
	service := &BalanceService{db: testDB}

	// Test 1: Call the function to get the holders count
	t.Run("GetHoldersCount", func(t *testing.T) {
		count, err := service.GetHoldersCount(testContract)
		require.NoError(t, err, "Failed to get holders count with range partitioning")
		assert.Equal(t, int64(5), count, "Query should return 5 distinct account+owner combinations")

		// Verify the query was properly partitioned
		lastQuery := middleware.LastQuery()
		assert.Contains(t, lastQuery, "UNION ALL", "Query should use UNION ALL for range partitioning")
	})

	// Test 2: Create a view and verify it works
	t.Run("CreateAndVerifyView", func(t *testing.T) {
		// Create the view
		err := service.CreateHoldersView(testContract)
		require.NoError(t, err, "Failed to create holders view")

		// Query the view to verify it returns the correct count
		var viewCount int64
		err = testDB.Raw("SELECT * FROM holders_" + testContract).Scan(&viewCount).Error
		require.NoError(t, err, "Failed to query the view")
		assert.Equal(t, int64(5), viewCount, "View should return 5 distinct account+owner combinations")

		// Verify the view creation query used UNION ALL
		lastQuery := middleware.LastQuery()
		assert.Contains(t, lastQuery, "UNION ALL", "View creation should use UNION ALL for range partitioning")
	})

	// Test 3: Verify the count is correct by using the Model approach directly
	t.Run("DirectModelQuery", func(t *testing.T) {
		var directCount int64
		err = testDB.Model(&GenericBalance{}).
			Where("contract = ?", testContract).
			Where("type = 'ERC721' OR (value IS NOT NULL AND value > 0)").
			Select("COUNT(DISTINCT(account || owner))").
			Count(&directCount).Error
		require.NoError(t, err, "Failed to execute direct query")
		assert.Equal(t, int64(5), directCount, "Direct query should return 5 distinct account+owner combinations")
	})

	// Test 4: Verify that data is correctly distributed across shards
	t.Run("VerifyDataDistribution", func(t *testing.T) {
		// Check shard 0
		var count0 int64
		err = testDB.Table("generic_balances_0").Where("contract = ?", testContract).Count(&count0).Error
		require.NoError(t, err, "Failed to count records in shard 0")
		assert.Equal(t, int64(2), count0, "Shard 0 should have 2 records")

		// Check shard 1
		var count1 int64
		err = testDB.Table("generic_balances_1").Where("contract = ?", testContract).Count(&count1).Error
		require.NoError(t, err, "Failed to count records in shard 1")
		assert.Equal(t, int64(3), count1, "Shard 1 should have 3 records")

		// Check shard 2
		var count2 int64
		err = testDB.Table("generic_balances_2").Where("contract = ?", testContract).Count(&count2).Error
		require.NoError(t, err, "Failed to count records in shard 2")
		assert.Equal(t, int64(2), count2, "Shard 2 should have 2 records")
	})

	// Clean up
	testDB.Exec("DROP VIEW IF EXISTS holders_" + testContract)
	testDB.Exec("TRUNCATE TABLE generic_balances")
	testDB.Exec("TRUNCATE TABLE generic_balances_0")
	testDB.Exec("TRUNCATE TABLE generic_balances_1")
	testDB.Exec("TRUNCATE TABLE generic_balances_2")
}
