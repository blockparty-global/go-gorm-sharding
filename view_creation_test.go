package sharding

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gorm.io/driver/postgres"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
)

// GenericBalance represents a balance table with contract, account, owner, type, and value fields
type GenericBalance struct {
	ID       int64   `gorm:"primarykey"`
	Contract string  `gorm:"index:idx_contract"` // Sharding key
	Account  string  `gorm:"index:idx_account"`
	Owner    string  `gorm:"index:idx_owner"`
	Type     string  `gorm:"index:idx_type"`
	Value    *string `gorm:"type:numeric"`
}

// TableName specifies the table name for GenericBalance
func (GenericBalance) TableName() string {
	return "generic_balances"
}

// BalanceService is a mock service that provides access to the database
type BalanceService struct {
	db *gorm.DB
}

// GetConnector returns the database connection
func (s *BalanceService) GetConnector() *gorm.DB {
	return s.db
}

// GetHoldersCount returns the count of distinct account+owner combinations
// for a specific contract where type is ERC721 or value > 0
func (s *BalanceService) GetHoldersCount(contract string) (int64, error) {
	var count int64
	query := s.GetConnector().Model(&GenericBalance{}).Where("contract = ?", contract)
	query = query.Where("type = 'ERC721' OR (value IS NOT NULL AND value > 0)")
	query = query.Select("COUNT(DISTINCT(account || owner))")

	err := query.Count(&count).Error
	return count, err
}

// CreateHoldersView creates a view that counts distinct account+owner combinations
func (s *BalanceService) CreateHoldersView(contract string) error {
	query := s.GetConnector().Model(&GenericBalance{}).Where("contract = ?", contract)
	query = query.Where("type = 'ERC721' OR (value IS NOT NULL AND value > 0)")
	query = query.Select("COUNT(DISTINCT(account || owner))")
	return s.GetConnector().Migrator().CreateView("holders_"+contract, gorm.ViewOption{
		Replace: true,
		Query:   query,
	})
}

// TestCreateHoldersView tests the creation of a view that counts distinct account+owner combinations
func TestCreateHoldersView(t *testing.T) {
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
	testDB.Exec("DROP VIEW IF EXISTS holders_0xcontract1")
	testDB.Exec("DROP VIEW IF EXISTS holders_0xcontract2")
	testDB.Exec("TRUNCATE TABLE generic_balances")

	// Get the shard suffix for our test contracts
	testContract1 := "0xcontract1"
	suffix1, err := balanceConfig.ShardingAlgorithm(testContract1)
	require.NoError(t, err, "Failed to calculate shard suffix for contract1")
	shardedTable1 := "generic_balances" + suffix1
	testDB.Exec("TRUNCATE TABLE " + shardedTable1)

	testContract2 := "0xcontract2"
	suffix2, err := balanceConfig.ShardingAlgorithm(testContract2)
	require.NoError(t, err, "Failed to calculate shard suffix for contract2")
	shardedTable2 := "generic_balances" + suffix2
	testDB.Exec("TRUNCATE TABLE " + shardedTable2)

	// Insert test data for contract1
	testData1 := []GenericBalance{
		// Records that should be counted (ERC721 type)
		{Contract: testContract1, Account: "account1", Owner: "owner1", Type: "ERC721", Value: nil},
		{Contract: testContract1, Account: "account2", Owner: "owner2", Type: "ERC721", Value: nil},

		// Records that should be counted (value > 0)
		{Contract: testContract1, Account: "account3", Owner: "owner3", Type: "ERC20", Value: strPtr("100")},
		{Contract: testContract1, Account: "account4", Owner: "owner4", Type: "ERC20", Value: strPtr("200")},

		// Records that should NOT be counted (value = 0)
		{Contract: testContract1, Account: "account5", Owner: "owner5", Type: "ERC20", Value: strPtr("0")},

		// Records that should NOT be counted (value is NULL and not ERC721)
		{Contract: testContract1, Account: "account6", Owner: "owner6", Type: "ERC20", Value: nil},

		// Duplicate account+owner combination that should be counted only once
		{Contract: testContract1, Account: "account1", Owner: "owner1", Type: "ERC721", Value: nil},
	}

	for _, record := range testData1 {
		err := testDB.Create(&record).Error
		require.NoError(t, err, "Failed to insert test data for contract1")
	}

	// Insert test data for contract2
	testData2 := []GenericBalance{
		// Records that should be counted (ERC721 type)
		{Contract: testContract2, Account: "account1", Owner: "owner1", Type: "ERC721", Value: nil},

		// Records that should be counted (value > 0)
		{Contract: testContract2, Account: "account2", Owner: "owner2", Type: "ERC20", Value: strPtr("100")},

		// Records that should NOT be counted (value = 0)
		{Contract: testContract2, Account: "account3", Owner: "owner3", Type: "ERC20", Value: strPtr("0")},
	}

	for _, record := range testData2 {
		err := testDB.Create(&record).Error
		require.NoError(t, err, "Failed to insert test data for contract2")
	}

	// Create a service instance
	service := &BalanceService{db: testDB}

	// Test 1: Query for contract1
	t.Run("QueryForContract1", func(t *testing.T) {
		// Call the function to get the holders count
		count, err := service.GetHoldersCount(testContract1)
		require.NoError(t, err, "Failed to get holders count for contract1")
		assert.Equal(t, int64(4), count, "Query should return 4 distinct account+owner combinations")

		// Verify the query was routed to the correct shard
		lastQuery := middleware.LastQuery()
		assert.Contains(t, lastQuery, shardedTable1, "Query should be routed to the correct shard table")
	})

	// Test 2: Query for contract2
	t.Run("QueryForContract2", func(t *testing.T) {
		// Call the function to get the holders count
		count, err := service.GetHoldersCount(testContract2)
		require.NoError(t, err, "Failed to get holders count for contract2")
		assert.Equal(t, int64(2), count, "Query should return 2 distinct account+owner combinations")

		// Verify the query was routed to the correct shard
		lastQuery := middleware.LastQuery()
		assert.Contains(t, lastQuery, shardedTable2, "Query should be routed to the correct shard table")
	})

	// Test 3: Verify query consistency
	t.Run("VerifyQueryConsistency", func(t *testing.T) {
		// Call the function to get the holders count again
		count, err := service.GetHoldersCount(testContract1)
		require.NoError(t, err, "Failed to get holders count for contract1")
		assert.Equal(t, int64(4), count, "Query should consistently return 4 distinct account+owner combinations")
	})

	// Test 4: Create and verify view with hash partitioning
	t.Run("CreateAndVerifyViewWithHashPartitioning", func(t *testing.T) {
		// Create the view for contract1
		err := service.CreateHoldersView(testContract1)
		require.NoError(t, err, "Failed to create holders view for contract1")

		// Query the view to verify it returns the correct count
		var viewCount int64
		err = testDB.Raw("SELECT * FROM holders_" + testContract1).Scan(&viewCount).Error
		require.NoError(t, err, "Failed to query the view")
		assert.Equal(t, int64(4), viewCount, "View should return 4 distinct account+owner combinations")

		// Verify the view creation query was routed to the correct shard
		lastQuery := middleware.LastQuery()
		assert.Contains(t, lastQuery, shardedTable1, "View creation should be routed to the correct shard table")

		// Create the view for contract2
		err = service.CreateHoldersView(testContract2)
		require.NoError(t, err, "Failed to create holders view for contract2")

		// Query the view to verify it returns the correct count
		err = testDB.Raw("SELECT * FROM holders_" + testContract2).Scan(&viewCount).Error
		require.NoError(t, err, "Failed to query the view")
		assert.Equal(t, int64(2), viewCount, "View should return 2 distinct account+owner combinations")

		// Verify the view creation query was routed to the correct shard
		lastQuery = middleware.LastQuery()
		assert.Contains(t, lastQuery, shardedTable2, "View creation should be routed to the correct shard table")
	})

	// Clean up
	testDB.Exec("TRUNCATE TABLE generic_balances")
	testDB.Exec("TRUNCATE TABLE " + shardedTable1)
	testDB.Exec("TRUNCATE TABLE " + shardedTable2)
}

// TestCreateHoldersViewWithPartitioning tests that the view creation works correctly
// with different partitioning strategies
func TestCreateHoldersViewWithPartitioning(t *testing.T) {
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

	// Configure list partitioning for generic_balances table with type as the sharding key
	listConfig := Config{
		DoubleWrite:    true,
		ShardingKey:    "type",
		NumberOfShards: 3,
		PartitionType:  PartitionTypeList,
		ListValues: map[string]int{
			"ERC20":   0,
			"ERC721":  1,
			"ERC1155": 2,
		},
		DefaultPartition: -1, // Error if unknown type
		ShardingSuffixs: func() []string {
			return []string{"_0", "_1", "_2"}
		},
	}

	// Register the sharding middleware
	configs := map[string]Config{
		"generic_balances": listConfig,
	}
	middleware := Register(configs, &GenericBalance{})
	testDB.Use(middleware)

	// Create the generic_balances table and sharded tables
	err = testDB.AutoMigrate(&GenericBalance{})
	require.NoError(t, err, "Failed to migrate generic_balances table")

	// Clean up existing test data and views
	testDB.Exec("DROP VIEW IF EXISTS holders_0xcontract3")
	testDB.Exec("TRUNCATE TABLE generic_balances")

	// Insert test data across different partitions
	testContract := "0xcontract3"
	testData := []GenericBalance{
		// ERC721 partition
		{Contract: testContract, Account: "account1", Owner: "owner1", Type: "ERC721", Value: nil},
		{Contract: testContract, Account: "account2", Owner: "owner2", Type: "ERC721", Value: nil},

		// ERC20 partition
		{Contract: testContract, Account: "account3", Owner: "owner3", Type: "ERC20", Value: strPtr("100")},
		{Contract: testContract, Account: "account4", Owner: "owner4", Type: "ERC20", Value: strPtr("200")},
		{Contract: testContract, Account: "account5", Owner: "owner5", Type: "ERC20", Value: strPtr("0")},
		{Contract: testContract, Account: "account6", Owner: "owner6", Type: "ERC20", Value: nil},

		// ERC1155 partition
		{Contract: testContract, Account: "account7", Owner: "owner7", Type: "ERC1155", Value: strPtr("300")},
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
		require.NoError(t, err, "Failed to get holders count with list partitioning")
		assert.Equal(t, int64(5), count, "Query should return 5 distinct account+owner combinations")

		// Verify the query was properly partitioned
		lastQuery := middleware.LastQuery()
		assert.Contains(t, lastQuery, "UNION ALL", "Query should use UNION ALL for list partitioning")
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
		assert.Contains(t, lastQuery, "UNION ALL", "View creation should use UNION ALL for list partitioning")
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

	// Test 4: Execute a raw query to force the middleware to show its partitioning strategy
	t.Run("RawQueryWithUnionAll", func(t *testing.T) {
		var rawCount int64

		// Use a more complex query that will force the middleware to use UNION ALL
		// We need to use a subquery to ensure the middleware combines results from all partitions
		err = testDB.Raw(`
		WITH combined_data AS (
			SELECT DISTINCT account || owner as account_owner
			FROM generic_balances 
			WHERE contract = ? AND (type = 'ERC721' OR (value IS NOT NULL AND value > 0))
		)
		SELECT COUNT(*) FROM combined_data`, testContract).Scan(&rawCount).Error
		require.NoError(t, err, "Failed to execute raw query")

		// Verify the query was properly partitioned
		lastQuery := middleware.LastQuery()
		t.Logf("Last query: %s", lastQuery)

		// The query should include UNION ALL to combine results from different partitions
		assert.Contains(t, lastQuery, "UNION ALL", "Query should use UNION ALL for list partitioning")

		// Verify the raw count matches our expected count
		assert.Equal(t, int64(5), rawCount, "Raw query should return 5 distinct account+owner combinations")
	})

	// Clean up
	testDB.Exec("TRUNCATE TABLE generic_balances")
	testDB.Exec("TRUNCATE TABLE generic_balances_0")
	testDB.Exec("TRUNCATE TABLE generic_balances_1")
	testDB.Exec("TRUNCATE TABLE generic_balances_2")
}
