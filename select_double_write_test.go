package sharding

import (
	"fmt"
	"gorm.io/gorm/clause"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"gorm.io/driver/postgres"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
)

// Image represents an image table where sharding key is NOT on id
type Image struct {
	ID        int64  `gorm:"primarykey"`
	UserID    int64  `gorm:"index"` // This is the sharding key, not id
	Status    string `gorm:"not null"`
	URL       string `gorm:"not null"`
	Name      string
	CreatedAt time.Time `gorm:"not null"`
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
			status text NOT NULL,
			url text NOT NULL,
			name text,
			created_at timestamp with time zone NOT NULL,
			updated_at timestamp with time zone
		)`)
	}

	// Insert test data
	testImages := []Image{
		{
			ID:        1001,
			UserID:    100, // This determines the shard (user_id % 4 = 0, so images_0)
			Status:    "active",
			URL:       "https://example.com/image1.jpg",
			Name:      "Test Image 1",
			CreatedAt: time.Now(),
		},
		{
			ID:        1002,
			UserID:    101, // This determines the shard (user_id % 4 = 1, so images_1)
			Status:    "active",
			URL:       "https://example.com/image2.jpg",
			Name:      "Test Image 2",
			CreatedAt: time.Now(),
		},
		{
			ID:        1003,
			UserID:    102, // This determines the shard (user_id % 4 = 2, so images_2)
			Status:    "active",
			URL:       "https://example.com/image3.jpg",
			Name:      "Test Image 3",
			CreatedAt: time.Now(),
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

		// The query should be directed to the base table since we're querying by ID (not sharding key)
		// and DoubleWrite is enabled
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
	})

	// Test Case: Query with both ID and sharding key
	t.Run("SelectByIDAndShardingKey", func(t *testing.T) {
		var foundImage Image

		err := testDB.Model(&Image{}).Where("id = ? AND user_id = ?", 1001, 100).First(&foundImage).Error

		require.NoError(t, err, "Query with both ID and sharding key should succeed")
		require.Equal(t, int64(1001), foundImage.ID, "Should find image with ID 1001")
		require.Equal(t, int64(100), foundImage.UserID, "Should find image with UserID 100")

		// This should route to the specific shard since sharding key is provided
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

		// Query executed successfully

		// Also test with Raw query to ensure it works
		var foundImageRaw Image
		err = testDB.Raw(`SELECT * FROM "images" WHERE id = $1 ORDER BY "images"."id" LIMIT 1`, 1002).Scan(&foundImageRaw).Error
		require.NoError(t, err, "Raw query should also succeed with DoubleWrite")
		require.Equal(t, int64(1002), foundImageRaw.ID, "Raw query should find image with ID 1002")
	})

	// Clean up
	truncateTables(testDB, "images", "images_0", "images_1", "images_2", "images_3")
}

// ImageStatus represents an image with status tracking
type ImageStatus struct {
	ID          int64     `gorm:"primarykey"`
	Status      string    `gorm:"not null"`
	URL         string    `gorm:"not null"`
	ContentType string    `gorm:"column:content_type"`
	Width       int       `gorm:"default:0"`
	Height      int       `gorm:"default:0"`
	Frames      int       `gorm:"default:0"`
	Size        int64     `gorm:"default:0"`
	CreatedAt   time.Time `gorm:"not null"`
	ErrorMsg    string    `gorm:"column:error_msg"`
}

func (ImageStatus) TableName() string {
	return "images"
}

func TestInsertOnConflictWithSharding(t *testing.T) {
	// Create a test DB connection
	testDB, err := gorm.Open(postgres.New(dbConfig), &gorm.Config{
		DisableForeignKeyConstraintWhenMigrating: true,
		Logger:                                   logger.Default.LogMode(logger.Info),
	})
	require.NoError(t, err, "Failed to connect to test database")

	// Configure sharding where the sharding key is ID
	imageShardingConfig := Config{
		DoubleWrite:         true,
		ShardingKey:         "id",
		NumberOfShards:      4,
		PrimaryKeyGenerator: PKSnowflake,
		PartitionType:       PartitionTypeHash,
	}

	// Register middleware
	configs := map[string]Config{
		"images": imageShardingConfig,
	}
	testMiddleware := Register(configs, &ImageStatus{})
	testDB.Use(testMiddleware)

	// Clean up tables before test
	truncateTables(testDB, "images", "images_0", "images_1", "images_2", "images_3")

	// Create the images table and sharded tables
	err = testDB.AutoMigrate(&ImageStatus{})
	require.NoError(t, err, "Failed to migrate images table")

	// Create sharded tables
	for i := 0; i < 4; i++ {
		tableName := fmt.Sprintf("images_%d", i)
		err := testDB.Table(tableName).AutoMigrate(&ImageStatus{})
		require.NoError(t, err, "Failed to migrate %s table", tableName)
	}

	t.Run("Insert with ON CONFLICT DO UPDATE", func(t *testing.T) {
		// Create initial image
		image := &ImageStatus{
			Status:      "pending",
			URL:         "https://example.com/image1.jpg",
			ContentType: "image/jpeg",
			Width:       800,
			Height:      600,
			Frames:      1,
			Size:        102400,
			CreatedAt:   time.Now(),
			ErrorMsg:    "",
		}

		// First insert
		err := testDB.Create(image).Error
		require.NoError(t, err, "Failed to create initial image")
		require.NotZero(t, image.ID, "Image ID should be generated")

		// Store the generated ID
		generatedID := image.ID

		// Update the same image using ON CONFLICT
		updatedImage := &ImageStatus{
			ID:          generatedID, // Use the same ID to trigger conflict
			Status:      "completed",
			URL:         "https://example.com/image1-processed.jpg",
			ContentType: "image/jpeg",
			Width:       1600,
			Height:      1200,
			Frames:      1,
			Size:        204800,
			CreatedAt:   time.Now(),
			ErrorMsg:    "",
		}

		// Perform INSERT with ON CONFLICT DO UPDATE
		err = testDB.Clauses(clause.OnConflict{
			Columns: []clause.Column{{Name: "id"}},
			DoUpdates: clause.AssignmentColumns([]string{
				"status", "url", "content_type", "width", "height", "frames", "size", "error_msg",
			}),
		}).Create(updatedImage).Error
		require.NoError(t, err, "Failed to upsert image")

		// Verify the update in the sharded table (primary write)
		shardID := generatedID % 4
		var resultShard ImageStatus
		err = testDB.Table(fmt.Sprintf("images_%d", shardID)).Where("id = ?", generatedID).First(&resultShard).Error
		require.NoError(t, err, "Failed to query sharded table")
		require.Equal(t, "completed", resultShard.Status)
		require.Equal(t, "https://example.com/image1-processed.jpg", resultShard.URL)
		require.Equal(t, 1600, resultShard.Width)
		require.Equal(t, 1200, resultShard.Height)
		require.Equal(t, int64(204800), resultShard.Size)

		// Verify the update in the base table (double write)
		var resultBase ImageStatus
		err = testDB.Table("images").Where("id = ?", generatedID).First(&resultBase).Error
		require.NoError(t, err, "Failed to query base table")
		require.Equal(t, "completed", resultBase.Status)
		require.Equal(t, "https://example.com/image1-processed.jpg", resultBase.URL)
		require.Equal(t, 1600, resultBase.Width)
		require.Equal(t, 1200, resultBase.Height)
		require.Equal(t, int64(204800), resultBase.Size)
	})

	t.Run("Insert with ON CONFLICT DO UPDATE - Multiple Fields", func(t *testing.T) {
		// Create initial image with error
		image := &ImageStatus{
			Status:      "failed",
			URL:         "https://example.com/broken-image.jpg",
			ContentType: "image/jpeg",
			Width:       0,
			Height:      0,
			Frames:      0,
			Size:        0,
			CreatedAt:   time.Now(),
			ErrorMsg:    "Failed to download image",
		}

		// First insert
		err := testDB.Create(image).Error
		require.NoError(t, err, "Failed to create initial image")
		require.NotZero(t, image.ID, "Image ID should be generated")

		generatedID := image.ID

		// Retry with successful download
		retryImage := &ImageStatus{
			ID:          generatedID,
			Status:      "completed",
			URL:         "https://example.com/broken-image.jpg",
			ContentType: "image/jpeg",
			Width:       1920,
			Height:      1080,
			Frames:      1,
			Size:        307200,
			CreatedAt:   time.Now(),
			ErrorMsg:    "", // Clear error message
		}

		// Perform INSERT with ON CONFLICT DO UPDATE using raw SQL style
		result := testDB.Exec(`
			INSERT INTO "images" ("id","status","url","content_type","width","height","frames","size","created_at","error_msg") 
			VALUES (?,?,?,?,?,?,?,?,?,?) 
			ON CONFLICT ("id") DO UPDATE SET 
				"status"="excluded"."status",
				"url"="excluded"."url",
				"content_type"="excluded"."content_type",
				"width"="excluded"."width",
				"height"="excluded"."height",
				"frames"="excluded"."frames",
				"size"="excluded"."size",
				"error_msg"="excluded"."error_msg"
			RETURNING "id"`,
			retryImage.ID,
			retryImage.Status,
			retryImage.URL,
			retryImage.ContentType,
			retryImage.Width,
			retryImage.Height,
			retryImage.Frames,
			retryImage.Size,
			retryImage.CreatedAt,
			retryImage.ErrorMsg,
		)
		require.NoError(t, result.Error, "Failed to execute raw upsert")
		require.Equal(t, int64(1), result.RowsAffected, "Should affect one row")

		// Verify the update
		var resultBase ImageStatus
		err = testDB.Table("images").Where("id = ?", generatedID).First(&resultBase).Error
		require.NoError(t, err, "Failed to query base table")
		require.Equal(t, "completed", resultBase.Status)
		require.Equal(t, 1920, resultBase.Width)
		require.Equal(t, 1080, resultBase.Height)
		require.Equal(t, "", resultBase.ErrorMsg)

		// Verify in sharded table
		shardID := generatedID % 4
		var resultShard ImageStatus
		err = testDB.Table(fmt.Sprintf("images_%d", shardID)).Where("id = ?", generatedID).First(&resultShard).Error
		require.NoError(t, err, "Failed to query sharded table")
		require.Equal(t, "completed", resultShard.Status)
		require.Equal(t, 1920, resultShard.Width)
		require.Equal(t, 1080, resultShard.Height)
		require.Equal(t, "", resultShard.ErrorMsg)
	})

	t.Run("Insert with ON CONFLICT - Workaround with Explicit ID", func(t *testing.T) {
		// WORKAROUND: Generate ID explicitly when using ON CONFLICT on ID
		// This is necessary because GORM doesn't include auto-generated IDs in INSERT statements
		config := configs["images"]
		generatedID := config.PrimaryKeyGeneratorFn(0)

		// Create a new image with explicit ID
		newImage := &ImageStatus{
			ID:          generatedID, // Set ID explicitly
			Status:      "processing",
			URL:         "https://example.com/new-image.jpg",
			ContentType: "image/png",
			Width:       2048,
			Height:      1536,
			Frames:      1,
			Size:        512000,
			CreatedAt:   time.Now(),
			ErrorMsg:    "",
		}

		// Now ON CONFLICT works correctly because ID is included in the INSERT
		err := testDB.Clauses(clause.OnConflict{
			Columns: []clause.Column{{Name: "id"}},
			DoUpdates: clause.AssignmentColumns([]string{
				"status", "url", "content_type", "width", "height", "frames", "size", "error_msg",
			}),
		}).Create(newImage).Error
		require.NoError(t, err, "Failed to insert new image with explicit ID")

		// Verify correct sharding
		shardID := newImage.ID % 4
		var resultShard ImageStatus
		err = testDB.Table(fmt.Sprintf("images_%d", shardID)).Where("id = ?", newImage.ID).First(&resultShard).Error
		require.NoError(t, err, "Failed to find record in sharded table")
		require.Equal(t, "processing", resultShard.Status)
		require.Equal(t, "https://example.com/new-image.jpg", resultShard.URL)

		// Verify double write
		var resultBase ImageStatus
		err = testDB.Table("images").Where("id = ?", newImage.ID).First(&resultBase).Error
		require.NoError(t, err, "Failed to find record in base table")
		require.Equal(t, "processing", resultBase.Status)
	})

	// Clean up
	truncateTables(testDB, "images", "images_0", "images_1", "images_2", "images_3")
}
