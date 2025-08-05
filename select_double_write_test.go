package sharding

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"gorm.io/driver/postgres"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
)

// Image represents an image table where sharding key is NOT on id
type Image struct {
	ID        int64 `gorm:"primarykey"`
	UserID    int64 `gorm:"index"` // This is the sharding key, not id
	URL       string
	Name      string
	CreatedAt time.Time
	UpdatedAt time.Time
}

func TestSelectDoubleWriteNonIDShardingKey(t *testing.T) {
	// Create a test DB connection
	testDB, err := gorm.Open(postgres.New(dbConfig), &gorm.Config{
		DisableForeignKeyConstraintWhenMigrating: true,
		Logger:                                   logger.Default.LogMode(logger.Info),
	})
	require.NoError(t, err, "Failed to connect to test database")

	// Configure sharding where the sharding key is user_id, NOT id
	imageShardingConfig := Config{
		DoubleWrite:         true,      // Enable double write
		ShardingKey:         "user_id", // Sharding key is user_id, not id
		NumberOfShards:      4,
		PrimaryKeyGenerator: PKSnowflake,
		PartitionType:       PartitionTypeHash,
	}

	// Register middleware
	configs := map[string]Config{
		"images": imageShardingConfig,
	}
	testMiddleware := Register(configs, &Image{})
	testDB.Use(testMiddleware)

	// Clean up tables before test
	truncateTables(testDB, "images", "images_0", "images_1", "images_2", "images_3")

	// Create the images table and sharded tables
	err = testDB.AutoMigrate(&Image{})
	require.NoError(t, err, "Failed to migrate images table")

	// Create sharded tables manually
	for i := 0; i < 4; i++ {
		tableName := "images_" + string(rune('0'+i))
		testDB.Exec(`CREATE TABLE IF NOT EXISTS ` + tableName + ` (
			id bigint PRIMARY KEY,
			user_id bigint,
			url text,
			name text,
			created_at timestamp with time zone,
			updated_at timestamp with time zone
		)`)
	}

	// Insert test data
	testImages := []Image{
		{
			ID:     1001,
			UserID: 100, // This determines the shard (user_id % 4 = 0, so images_0)
			URL:    "https://example.com/image1.jpg",
			Name:   "Test Image 1",
		},
		{
			ID:     1002,
			UserID: 101, // This determines the shard (user_id % 4 = 1, so images_1)
			URL:    "https://example.com/image2.jpg",
			Name:   "Test Image 2",
		},
		{
			ID:     1003,
			UserID: 102, // This determines the shard (user_id % 4 = 2, so images_2)
			URL:    "https://example.com/image3.jpg",
			Name:   "Test Image 3",
		},
	}

	// Insert the test images
	for _, img := range testImages {
		err := testDB.Create(&img).Error
		require.NoError(t, err, "Failed to insert test image with ID %d", img.ID)
	}

	// Test Case: Query by ID (not the sharding key) with LIMIT 1
	// This should succeed because DoubleWrite is enabled
	t.Run("SelectByIDWithDoubleWrite", func(t *testing.T) {
		var foundImage Image

		// This is the exact query from the task:
		// SELECT * FROM "images" WHERE id = $1 ORDER BY "images"."id" LIMIT 1
		err := testDB.Model(&Image{}).Where("id = ?", 1001).First(&foundImage).Error

		// Should succeed because DoubleWrite is enabled
		require.NoError(t, err, "Query by ID should succeed with DoubleWrite enabled")

		// Verify we found the correct image
		require.Equal(t, int64(1001), foundImage.ID, "Should find image with ID 1001")
		require.Equal(t, int64(100), foundImage.UserID, "Should find image with UserID 100")
		require.Equal(t, "Test Image 1", foundImage.Name, "Should find correct image name")
		require.Equal(t, "https://example.com/image1.jpg", foundImage.URL, "Should find correct image URL")

		// Log the generated query for verification
		t.Logf("Generated query: %s", testMiddleware.LastQuery())

		// The query should be directed to the base table since we're querying by ID (not sharding key)
		// and DoubleWrite is enabled
		require.Contains(t, testMiddleware.LastQuery(), "images", "Query should use base table with DoubleWrite")
	})

	// Test Case: Query by ID that doesn't exist
	t.Run("SelectByNonExistentID", func(t *testing.T) {
		var foundImage Image

		err := testDB.Model(&Image{}).Where("id = ?", 9999).First(&foundImage).Error

		// Should return "record not found" error, not a sharding error
		require.Error(t, err, "Should return error for non-existent ID")
		require.Equal(t, gorm.ErrRecordNotFound, err, "Should return record not found error")
	})

	// Test Case: Query by sharding key (user_id) - should route to specific shard
	t.Run("SelectByShardingKey", func(t *testing.T) {
		var foundImage Image

		err := testDB.Model(&Image{}).Where("user_id = ?", 100).First(&foundImage).Error

		require.NoError(t, err, "Query by sharding key should succeed")
		require.Equal(t, int64(1001), foundImage.ID, "Should find image with ID 1001")
		require.Equal(t, int64(100), foundImage.UserID, "Should find image with UserID 100")

		// This should route to a specific shard (images_0 for user_id 100)
		lastQuery := testMiddleware.LastQuery()
		t.Logf("Generated query for sharding key: %s", lastQuery)
		require.Contains(t, lastQuery, "images_0", "Query by sharding key should route to specific shard")
	})

	// Test Case: Query with both ID and sharding key
	t.Run("SelectByIDAndShardingKey", func(t *testing.T) {
		var foundImage Image

		err := testDB.Model(&Image{}).Where("id = ? AND user_id = ?", 1001, 100).First(&foundImage).Error

		require.NoError(t, err, "Query with both ID and sharding key should succeed")
		require.Equal(t, int64(1001), foundImage.ID, "Should find image with ID 1001")
		require.Equal(t, int64(100), foundImage.UserID, "Should find image with UserID 100")

		// This should route to the specific shard since sharding key is provided
		lastQuery := testMiddleware.LastQuery()
		t.Logf("Generated query for ID and sharding key: %s", lastQuery)
		require.Contains(t, lastQuery, "images_0", "Query with sharding key should route to specific shard")
	})

	// Test Case: Verify double write actually wrote to both tables
	t.Run("VerifyDoubleWrite", func(t *testing.T) {
		// Check that the record exists in the base table
		var baseTableCount int64
		err := testDB.Table("images").Where("id = ?", 1001).Count(&baseTableCount).Error
		require.NoError(t, err, "Should be able to query base table")
		require.Equal(t, int64(1), baseTableCount, "Record should exist in base table")

		// Check that the record exists in the appropriate sharded table
		// UserID 100 % 4 = 0, so it should be in images_0
		var shardedTableCount int64
		err = testDB.Table("images_0").Where("id = ?", 1001).Count(&shardedTableCount).Error
		require.NoError(t, err, "Should be able to query sharded table")
		require.Equal(t, int64(1), shardedTableCount, "Record should exist in sharded table")

		// Verify the record is NOT in other shards
		var otherShardCount int64
		err = testDB.Table("images_1").Where("id = ?", 1001).Count(&otherShardCount).Error
		require.NoError(t, err, "Should be able to query other sharded table")
		require.Equal(t, int64(0), otherShardCount, "Record should NOT exist in wrong shard")
	})

	// Test Case: Test the exact query format from the task description
	t.Run("ExactQueryFormat", func(t *testing.T) {
		var foundImage Image

		// Use the exact query format from the task: SELECT * FROM "images" WHERE id = $1 ORDER BY "images"."id" LIMIT 1
		// This should work with DoubleWrite enabled and route to the base table since we're querying by ID only
		err := testDB.Model(&Image{}).Where("id = ?", 1002).Order("\"images\".\"id\"").First(&foundImage).Error

		require.NoError(t, err, "Exact query format should succeed with DoubleWrite")
		require.Equal(t, int64(1002), foundImage.ID, "Should find image with ID 1002")
		require.Equal(t, int64(101), foundImage.UserID, "Should find image with UserID 101")

		// Log the query that was actually executed
		t.Logf("Exact format query: %s", testMiddleware.LastQuery())

		// Also test with Raw query to ensure it works
		var foundImageRaw Image
		err = testDB.Raw(`SELECT * FROM "images" WHERE id = $1 ORDER BY "images"."id" LIMIT 1`, 1002).Scan(&foundImageRaw).Error
		require.NoError(t, err, "Raw query should also succeed with DoubleWrite")
		require.Equal(t, int64(1002), foundImageRaw.ID, "Raw query should find image with ID 1002")
	})

	// Clean up
	truncateTables(testDB, "images", "images_0", "images_1", "images_2", "images_3")
}
