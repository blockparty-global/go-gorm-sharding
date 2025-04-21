package test

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	sharding "gorm.io/sharding"
)

// TestSplitBatchInsertByShards_MultipleShards_Red tests the batch handling when records
// belong to different shards - Red phase test
func TestSplitBatchInsertByShards_MultipleShards_Red(t *testing.T) {
	// Set up the sharding configuration with multiple shards
	config := make(map[string]sharding.Config)

	// Create a simple hash algorithm for testing
	hashAlgorithm := func(value interface{}) (string, error) {
		// Simple algorithm: convert value to int and mod 3
		switch v := value.(type) {
		case string:
			// Use first character code % 3 for strings
			if len(v) > 0 {
				return fmt.Sprintf("_%d", int(v[0])%3), nil
			}
			return "_0", nil
		default:
			return "_0", fmt.Errorf("unsupported type for test hash algorithm: %T", value)
		}
	}

	// Configure a test table with 3 shards
	config["balance_balances"] = sharding.Config{
		ShardingKey:       "contract",
		NumberOfShards:    3,
		PartitionType:     sharding.PartitionTypeHash,
		ShardingAlgorithm: hashAlgorithm,
	}

	// Create sharding instance with the test configuration
	s := sharding.Register(config, nil) // Pass nil as the second argument

	// Create a batch INSERT query with values going to different shards
	query := `INSERT INTO "balance_balances" ("contract","type","account","value","owner","block","tx_hash","token_id") 
              VALUES ($1,$2,$3,$4,$5,$6,$7,$8),($9,$10,$11,$12,$13,$14,$15,$16),($17,$18,$19,$20,$21,$22,$23,$24)`

	// Create parameters for different shards
	// First group goes to shard 0 (contract starts with 'a')
	// Second group goes to shard 1 (contract starts with 'b')
	// Third group goes to shard 2 (contract starts with 'c')
	args := []interface{}{
		"a0xd8b9105b07c0e29253c6e659afec45752614df37", "1", "account1", "100", "owner1", "123", "tx1", "token1",
		"b0xbbd3edd4d3b519c0d14965d9311185cfac8c3220", "1", "account2", "200", "owner2", "456", "tx2", "token2",
		"c0x340a5b718557801f20afd6e244c78fcd1c0b2212", "1", "account3", "300", "owner3", "789", "tx3", "token3",
	}

	// Call the function that should split the batch by sharding key
	queries, queryParams, err := s.SplitBatchInsertByShards(query, args)

	// The test should fail because the batch insert handling is defective
	assert.Error(t, err, "The function should return an error when processing a batch insert with multiple shards")
	assert.Nil(t, queries, "Expected nil queries due to error")
	assert.Nil(t, queryParams, "Expected nil query parameters due to error")
}

// TestSplitBatchInsertByShards_LargeParameterCount_Red simulates the specific edge case
// from the logs with a large number of parameters - Red phase test
func TestSplitBatchInsertByShards_LargeParameterCount_Red(t *testing.T) {
	// Set up the sharding configuration with multiple shards
	config := make(map[string]sharding.Config)

	// Create a simple hash algorithm that uses the first character of contract address
	hashAlgorithm := func(value interface{}) (string, error) {
		switch v := value.(type) {
		case string:
			// Extract first character as shard key
			if len(v) > 2 {
				return fmt.Sprintf("_%c", v[2]), nil
			}
			return "_0", nil
		default:
			return "_0", fmt.Errorf("unsupported type for test hash algorithm: %T", value)
		}
	}

	// Configure a test table with enough shards
	config["balance_balances"] = sharding.Config{
		ShardingKey:       "contract",
		NumberOfShards:    12, // Matching the number from logs
		PartitionType:     sharding.PartitionTypeHash,
		ShardingAlgorithm: hashAlgorithm,
	}

	// Create sharding instance with the test configuration
	s := sharding.Register(config, nil) // Pass nil as the second argument

	// Create a query similar to the one in the logs with 36+ value groups
	// Using a simplified version with the same pattern
	query := `INSERT INTO "balance_balances" ("contract","type","account","value","owner","block","tx_hash","token_id") VALUES `

	// We'll create 36 value groups (8 parameters each = 288 parameters)
	valueGroups := make([]string, 36)
	args := make([]interface{}, 36*8)

	for i := 0; i < 36; i++ {
		// Create different contract addresses to ensure they go to different shards
		// Using different first characters to ensure they hash to different shards
		contract := fmt.Sprintf("0x%c%d", 'a'+i%12, i) // Different first chars for different shards

		valueGroups[i] = fmt.Sprintf("($%d,$%d,$%d,$%d,$%d,$%d,$%d,$%d)",
			i*8+1, i*8+2, i*8+3, i*8+4, i*8+5, i*8+6, i*8+7, i*8+8)

		// Set up the parameters
		args[i*8] = contract
		args[i*8+1] = fmt.Sprintf("type%d", i)
		args[i*8+2] = fmt.Sprintf("account%d", i)
		args[i*8+3] = fmt.Sprintf("value%d", i)
		args[i*8+4] = fmt.Sprintf("owner%d", i)
		args[i*8+5] = fmt.Sprintf("block%d", i)
		args[i*8+6] = fmt.Sprintf("tx%d", i)
		args[i*8+7] = fmt.Sprintf("token%d", i)
	}

	// Join all value groups with commas
	query += fmt.Sprintf("%s", valueGroups[0])
	for i := 1; i < 36; i++ {
		query += "," + valueGroups[i]
	}

	// Add ON CONFLICT clause similar to the one in the logs
	query += ` ON CONFLICT ("contract","type","account","token_id") DO UPDATE SET "value"="excluded"."value","owner"="excluded"."owner","block"="excluded"."block","tx_hash"="excluded"."tx_hash" RETURNING "token_id"`

	// Call the function that should split the batch by sharding key
	queries, queryParams, err := s.SplitBatchInsertByShards(query, args)

	// Assert that we get an error about parameter count mismatch
	assert.Error(t, err, "Expected error due to parameter index out of bounds")
	assert.Contains(t, err.Error(), "parameter count mismatch", "Expected specific error about parameter count mismatch")
	assert.Nil(t, queries, "Expected nil queries due to error")
	assert.Nil(t, queryParams, "Expected nil query parameters due to error")
}

// TestParameterExtractionAndTracking_Red tests specific parameter extraction and tracking
// to identify issues with index calculation - Red phase test
func TestParameterExtractionAndTracking_Red(t *testing.T) {
	// Set up the sharding configuration
	config := make(map[string]sharding.Config)

	// Simple hash algorithm for testing
	hashAlgorithm := func(value interface{}) (string, error) {
		return "_0", nil // Always return same suffix for simplicity
	}

	// Configure a test table
	config["test_table"] = sharding.Config{
		ShardingKey:       "key",
		NumberOfShards:    1,
		PartitionType:     sharding.PartitionTypeHash,
		ShardingAlgorithm: hashAlgorithm,
	}

	// Create sharding instance with the test configuration
	s := sharding.Register(config, nil) // Pass nil as the second argument

	// Test various parameter patterns that specifically trigger the index tracking issue
	testCases := []struct {
		name  string
		query string
		args  []interface{}
	}{
		{
			name:  "Complex case with non-sequential parameters",
			query: `INSERT INTO "test_table" ("key","value") VALUES ($2,$1),($4,$3),($6,$5)`,
			args:  []interface{}{123, "key1", 456, "key2", 789, "key3"},
		},
		{
			name:  "Mixed parameter indexes",
			query: `INSERT INTO "test_table" ("key","value") VALUES ($1,$3),($2,$3),($4,$3)`,
			args:  []interface{}{"key1", "key2", 123, "key3"},
		},
		{
			name:  "Large parameter gap",
			query: `INSERT INTO "test_table" ("key","value") VALUES ($1,$2),($30,$31)`,
			args: func() []interface{} {
				// Create args array with 31 elements where most aren't used
				a := make([]interface{}, 31)
				a[0] = "key1"
				a[1] = "value1"
				a[29] = "key2"
				a[30] = "value2"
				return a
			}(),
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			// Try to split the batch insert
			queries, queryParams, err := s.SplitBatchInsertByShards(tc.query, tc.args)

			// We expect all of these test cases to fail
			assert.Error(t, err, "Expected error due to parameter tracking issues")
			assert.Contains(t, err.Error(), "parameter", "Expected parameter-related error")
			assert.Nil(t, queries, "Expected nil queries due to error")
			assert.Nil(t, queryParams, "Expected nil query parameters due to error")
		})
	}
}
