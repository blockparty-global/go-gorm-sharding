package test

import (
	"regexp"
	"testing"

	"github.com/stretchr/testify/assert"
	"gorm.io/sharding"
)

// Import the setupTestSharding function from the package

// TestUpdateRegexPatternNew tests that the system can properly identify and parse UPDATE statements
func TestUpdateRegexPatternNew(t *testing.T) {
	// The UPDATE regex pattern should not exist yet, so this test should fail
	// In the green phase, we'll implement this pattern

	// Access the updateRegex pattern that will be defined later
	// This will fail since the pattern doesn't exist yet
	updateRegex, ok := getUpdateRegexPatternNew()
	assert.True(t, ok, "Update regex pattern should be defined")
	assert.NotNil(t, updateRegex, "Update regex pattern should not be nil")

	// Sample UPDATE statement
	updateSQL := `UPDATE "users" SET "name"=$1, "email"=$2 WHERE "user_id" = $3`

	// Test that our regex matches the UPDATE statement
	assert.True(t, updateRegex.MatchString(updateSQL), "Regex should match valid UPDATE statement")

	// Test that it extracts the correct components
	matches := updateRegex.FindStringSubmatch(updateSQL)
	assert.NotEmpty(t, matches, "Should extract components from UPDATE statement")

	// Define the test expectations once the regex is implemented
	// The exact indices will depend on the final regex implementation
	// but we expect to extract: table name, SET clause, and WHERE clause
}

// TestUpdateColumnExtractionNew tests that the system can extract column names from UPDATE statements
func TestUpdateColumnExtractionNew(t *testing.T) {
	// Sample UPDATE statement
	updateSQL := `UPDATE "users" SET "name"=$1, "email"=$2 WHERE "user_id" = $3`

	// Try to extract columns from the UPDATE statement
	// This function doesn't exist yet so this test should fail
	tableName, setColumns, whereClause, err := extractUpdateComponentsNew(updateSQL)

	// In green phase, the extraction should succeed
	assert.NoError(t, err, "Should extract components without error")
	assert.Equal(t, "users", tableName, "Should extract correct table name")
	assert.Contains(t, setColumns, "name", "Should extract name column from SET clause")
	assert.Contains(t, setColumns, "email", "Should extract email column from SET clause")
	assert.Contains(t, whereClause, "user_id", "Should extract user_id from WHERE clause")
}

// TestShardingKeyExtractionFromUpdateNew tests that the system can extract sharding keys from UPDATE statements
func TestShardingKeyExtractionFromUpdateNew(t *testing.T) {
	// Sample UPDATE statement with a sharding key in the WHERE clause
	updateSQL := `UPDATE "users" SET "name"=$1, "email"=$2 WHERE "user_id" = $3`
	args := []interface{}{"John Doe", "john@example.com", 42}

	// Create a mock sharding object just for testing
	// We won't use the real setupTestSharding since we're getting conflicts
	s := createMockSharding()

	// Try to extract the sharding key value from the UPDATE statement
	// This function doesn't exist yet, so this test should fail
	shardingKeyValue, found, err := extractShardingKeyFromUpdateNew(s, "users", updateSQL, args)

	// In green phase, the extraction should succeed
	assert.NoError(t, err, "Should extract sharding key without error")
	assert.True(t, found, "Should find sharding key in WHERE clause")
	assert.Equal(t, 42, shardingKeyValue, "Should extract correct sharding key value")

	// Test with a quoted identifier for the sharding key
	updateSQL2 := `UPDATE "users" SET "name"=$1, "email"=$2 WHERE "user_id"=$3`
	shardingKeyValue2, found2, err2 := extractShardingKeyFromUpdateNew(s, "users", updateSQL2, args)

	assert.NoError(t, err2, "Should extract sharding key without error (quoted)")
	assert.True(t, found2, "Should find sharding key in WHERE clause (quoted)")
	assert.Equal(t, 42, shardingKeyValue2, "Should extract correct sharding key value (quoted)")

	// Test with multiple conditions in WHERE clause
	updateSQL3 := `UPDATE "users" SET "name"=$1 WHERE "active"=true AND "user_id"=$2`
	args3 := []interface{}{"John Doe", 42}

	shardingKeyValue3, found3, err3 := extractShardingKeyFromUpdateNew(s, "users", updateSQL3, args3)

	assert.NoError(t, err3, "Should extract sharding key without error (complex WHERE)")
	assert.True(t, found3, "Should find sharding key in complex WHERE clause")
	assert.Equal(t, 42, shardingKeyValue3, "Should extract correct sharding key value (complex WHERE)")
}

// TestMultiShardUpdateRoutingNew tests that UPDATE statements affecting multiple shards are properly split and routed
func TestMultiShardUpdateRoutingNew(t *testing.T) {
	// Sample multi-shard UPDATE statement (affects multiple user_ids)
	updateSQL := `UPDATE "users" SET "active"=$1 WHERE "user_id" IN ($2, $3, $4)`
	args := []interface{}{true, 1, 2, 3} // Three different user_ids = potentially three different shards

	// Create a mock sharding object just for testing
	s := createMockSharding()

	// Try to split the UPDATE statement into shard-specific statements
	// This function doesn't exist yet, so this test should fail
	queries, queryParams, err := splitUpdateByShards(s, updateSQL, args)

	// In green phase, the splitting should succeed
	assert.NoError(t, err, "Should split UPDATE without error")
	assert.Len(t, queries, 3, "Should generate 3 shard-specific UPDATE statements")
	assert.Len(t, queryParams, 3, "Should generate 3 sets of parameters")

	// Each query should target a specific shard
	// The exact format will depend on the final implementation
	for _, query := range queries {
		assert.Contains(t, query, "UPDATE", "Should be an UPDATE statement")
		assert.Contains(t, query, "WHERE", "Should include WHERE clause")
		// Each query should mention only one user_id
		assert.NotContains(t, query, "IN", "Shard-specific queries shouldn't use IN clause")
	}
}

// Helper function to create a mock sharding object for tests
func createMockSharding() *sharding.Sharding {
	// Create a sharding config with user_id as the sharding key
	shardingConfig := sharding.Config{
		ShardingKey:    "user_id",
		NumberOfShards: 4,
		// Use a simple mod sharding algorithm for testing
		ShardingAlgorithm: func(value interface{}) (string, error) {
			// Convert value to int for modulo operation
			if uid, ok := value.(int); ok {
				return "_" + string(rune('0'+uid%4)), nil
			}
			return "", assert.AnError
		},
	}

	// Create and return a sharding instance for testing
	// Register the users table with our config
	configs := map[string]sharding.Config{
		"users": shardingConfig,
	}

	s := sharding.Register(configs, []interface{}{"users"})

	return s
}

// Helper function to access the updateRegex pattern from batch_handler.go
func getUpdateRegexPatternNew() (*regexp.Regexp, bool) {
	// Access the exported updateRegex pattern from the sharding package
	updateRegex := sharding.GetUpdateRegexPattern()
	return updateRegex, updateRegex != nil
}

// Helper function that extracts components from an UPDATE statement
func extractUpdateComponentsNew(query string) (tableName string, setColumns []string, whereClause string, err error) {
	// Use the existing ExtractUpdateComponents function from the sharding package
	var columnParams map[string]int
	tableName, columnParams, whereClause, err = sharding.ExtractUpdateComponents(query)

	// Extract column names from the map
	setColumns = make([]string, 0, len(columnParams))
	for col := range columnParams {
		setColumns = append(setColumns, col)
	}

	return
}

// Helper function to extract sharding key from UPDATE statement
func extractShardingKeyFromUpdateNew(s *sharding.Sharding, table string, query string, args []interface{}) (shardingKeyValue interface{}, found bool, err error) {
	// Use the existing ExtractShardingKeyFromUpdate function from the sharding package
	return s.ExtractShardingKeyFromUpdate(table, query, args)
}

// Helper function to split an UPDATE statement into shard-specific statements
func splitUpdateByShards(s *sharding.Sharding, query string, args []interface{}) ([]string, [][]interface{}, error) {
	// Use the existing SplitUpdateByShards function from the sharding package
	return s.SplitUpdateByShards(query, args)
}
