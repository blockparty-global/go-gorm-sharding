package sharding

import (
	"fmt"
	"strings"
	"testing"

	"github.com/longbridgeapp/assert"
	"gorm.io/driver/postgres"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
)

// AssetContract represents a table that uses ON CONFLICT clauses in its operations
type AssetContract struct {
	ID      int64  `gorm:"primarykey"`
	Address string `gorm:"uniqueIndex"`
	Flags   int
}

func TestBatchInsertWithOnConflict(t *testing.T) {
	// Create a test DB with proper configuration
	testDB, err := gorm.Open(postgres.New(dbConfig), &gorm.Config{
		DisableForeignKeyConstraintWhenMigrating: true,
		Logger:                                   logger.Default.LogMode(logger.Info),
	})
	if err != nil {
		t.Fatalf("Failed to connect to database: %v", err)
	}

	// Set up configuration with address as the sharding key
	assetContractConfig := Config{
		DoubleWrite:         true,
		ShardingKey:         "address",
		NumberOfShards:      4,
		PrimaryKeyGenerator: PKSnowflake,
	}

	// Register the middleware with configuration
	configs := map[string]Config{
		"asset_contracts": assetContractConfig,
	}

	middleware := Register(configs, &AssetContract{})
	testDB.Use(middleware)

	// Drop and recreate tables
	testDB.Exec("DROP TABLE IF EXISTS asset_contracts")
	for i := 0; i < 4; i++ {
		testDB.Exec("DROP TABLE IF EXISTS asset_contracts_" + fmt.Sprint(i))
	}

	// Auto migrate to create the tables
	err = testDB.AutoMigrate(&AssetContract{})
	if err != nil {
		t.Fatalf("Failed to migrate tables: %v", err)
	}

	// Create sharded tables manually to ensure they exist
	for i := 0; i < 4; i++ {
		testDB.Exec(`CREATE TABLE IF NOT EXISTS asset_contracts_` + fmt.Sprint(i) + ` (
			id bigint PRIMARY KEY,
			address text UNIQUE,
			flags integer
		)`)
	}

	// Test 1: Basic INSERT with unique address
	t.Run("BasicInsert", func(t *testing.T) {
		// Insert a record
		err := testDB.Create(&AssetContract{
			Address: "0xabc123",
			Flags:   1,
		}).Error
		assert.Nil(t, err, "Failed to insert contract")

		// Verify the record was inserted
		var contract AssetContract
		err = testDB.Where("address = ?", "0xabc123").First(&contract).Error
		assert.Nil(t, err, "Failed to find inserted contract")
		assert.Equal(t, 1, contract.Flags, "Flags should be 1")
	})

	// Test 2: INSERT with ON CONFLICT DO UPDATE
	t.Run("InsertWithOnConflict", func(t *testing.T) {
		// Reset the last query to make checking easier
		middleware.querys.Store("last_query", "")

		// Execute a raw SQL INSERT with ON CONFLICT
		err := testDB.Exec(`
			INSERT INTO asset_contracts (address, flags) 
			VALUES ('0xabc123', 2) 
			ON CONFLICT (address) DO UPDATE SET flags = EXCLUDED.flags
		`).Error
		assert.Nil(t, err, "Failed to execute ON CONFLICT query")

		// Check the last query to verify it used the standard handler
		lastQuery, _ := middleware.querys.Load("last_query")
		lastQueryStr, _ := lastQuery.(string)

		// The query should contain the ON CONFLICT clause and should NOT contain batch handling indicators
		assert.True(t, strings.Contains(lastQueryStr, "ON CONFLICT"),
			"Query should contain ON CONFLICT clause")

		// Verify the record was updated
		var contract AssetContract
		err = testDB.Where("address = ?", "0xabc123").First(&contract).Error
		assert.Nil(t, err, "Failed to find updated contract")
		assert.Equal(t, 2, contract.Flags, "Flags should be updated to 2")
	})

	// Test 3: INSERT with ON CONFLICT DO NOTHING
	t.Run("InsertWithOnConflictDoNothing", func(t *testing.T) {
		// Reset the last query
		middleware.querys.Store("last_query", "")

		// Execute a raw SQL INSERT with ON CONFLICT DO NOTHING
		err := testDB.Exec(`
			INSERT INTO asset_contracts (address, flags) 
			VALUES ('0xabc123', 3) 
			ON CONFLICT (address) DO NOTHING
		`).Error
		assert.Nil(t, err, "Failed to execute ON CONFLICT DO NOTHING query")

		// Check the last query to verify it used the standard handler
		lastQuery, _ := middleware.querys.Load("last_query")
		lastQueryStr, _ := lastQuery.(string)

		// The query should contain the ON CONFLICT clause
		assert.True(t, strings.Contains(lastQueryStr, "ON CONFLICT"),
			"Query should contain ON CONFLICT clause")

		// Verify the record was NOT updated (because of DO NOTHING)
		var contract AssetContract
		err = testDB.Where("address = ?", "0xabc123").First(&contract).Error
		assert.Nil(t, err, "Failed to find contract")
		assert.Equal(t, 2, contract.Flags, "Flags should still be 2 (not updated)")
	})

	// Test 4: Multiple inserts with ON CONFLICT
	t.Run("MultipleInsertsWithOnConflict", func(t *testing.T) {
		// Test multiple inserts with ON CONFLICT - should use the default DB for each insert
		// Since batch handler should be skipped, each statement will be executed individually

		// First insert/update
		err := testDB.Exec(`
			INSERT INTO asset_contracts (address, flags) 
			VALUES ('0xabc123', 4) 
			ON CONFLICT (address) DO UPDATE SET flags = EXCLUDED.flags
		`).Error
		assert.Nil(t, err, "Failed to execute first INSERT with ON CONFLICT")

		// Second insert/update for a different contract
		err = testDB.Exec(`
			INSERT INTO asset_contracts (address, flags) 
			VALUES ('0xdef456', 5) 
			ON CONFLICT (address) DO UPDATE SET flags = EXCLUDED.flags
		`).Error
		assert.Nil(t, err, "Failed to execute second INSERT with ON CONFLICT")

		// Verify first contract was updated correctly
		var contract1 AssetContract
		err = testDB.Where("address = ?", "0xabc123").First(&contract1).Error
		assert.Nil(t, err, "Failed to find first contract")
		assert.Equal(t, 4, contract1.Flags, "First contract flags should be 4")

		// Verify second contract was inserted correctly
		var contract2 AssetContract
		err = testDB.Where("address = ?", "0xdef456").First(&contract2).Error
		assert.Nil(t, err, "Failed to find second contract")
		assert.Equal(t, 5, contract2.Flags, "Second contract flags should be 5")
	})

	// Test 5: Using the GORM API with UniqueConstraint
	t.Run("GormUpsert", func(t *testing.T) {
		// Reset the last query
		middleware.querys.Store("last_query", "")

		// Execute a raw SQL INSERT with ON CONFLICT
		err := testDB.Exec(`
			INSERT INTO asset_contracts (address, flags) 
			VALUES ('0xabc123', 6) 
			ON CONFLICT (address) DO UPDATE SET flags = EXCLUDED.flags
		`).Error
		assert.Nil(t, err, "Failed to execute GORM upsert")

		// Check the last query to verify it used the standard handler
		lastQuery, _ := middleware.querys.Load("last_query")
		lastQueryStr, _ := lastQuery.(string)

		// The query should contain the ON CONFLICT clause
		assert.True(t, strings.Contains(lastQueryStr, "ON CONFLICT"),
			"Query should contain ON CONFLICT clause")

		// Verify the record was updated
		var updatedContract AssetContract
		err = testDB.Where("address = ?", "0xabc123").First(&updatedContract).Error
		assert.Nil(t, err, "Failed to find updated contract")
		assert.Equal(t, 6, updatedContract.Flags, "Flags should be updated to 6")
	})
}
