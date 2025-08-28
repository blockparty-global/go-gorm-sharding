package sharding

import (
	"fmt"
	"hash/crc32"
	"hash/fnv"
	"os"
	"reflect"
	"regexp"
	"strconv"
	"strings"
	"testing"

	"gorm.io/gorm"
)

func truncateTables(db *gorm.DB, tables ...string) {
	for _, table := range tables {
		db.Exec(fmt.Sprintf("TRUNCATE TABLE %s RESTART IDENTITY CASCADE", table))
	}
}

func toDialect(sql string) string {
	if os.Getenv("DIALECTOR") == "mysql" {
		sql = strings.ReplaceAll(sql, `"`, "`")
		r := regexp.MustCompile(`\$([0-9]+)`)
		sql = r.ReplaceAllString(sql, "?")
		sql = strings.ReplaceAll(sql, " RETURNING `id`", "")
	} else if os.Getenv("DIALECTOR") == "mariadb" {
		sql = strings.ReplaceAll(sql, `"`, "`")
		r := regexp.MustCompile(`\$([0-9]+)`)
		sql = r.ReplaceAllString(sql, "?")
	}
	return sql
}

// assertSfidQueryResult verifies that unique IDs were generated in a batch insert query.
func assertSfidQueryResult(t *testing.T, expectedStructureHint, lastQuery string) {
	t.Helper()

	// Regex to find VALUES clauses and extract the last number (assumed ID)
	// This regex looks for VALUES followed by groups of values in parentheses.
	// It captures the last number before the closing parenthesis of each group.
	// Example: VALUES (1, 'a', 100), (2, 'b', 101) -> extracts 100 and 101
	re := regexp.MustCompile(`\((?:[^()]*,\s*)+(-?\d+)\)`) // Capture last number in each VALUES group

	matches := re.FindAllStringSubmatch(lastQuery, -1)

	if len(matches) == 0 {
		t.Errorf("assertSfidQueryResult: Could not find any VALUES clauses with IDs in query: %s", lastQuery)
		return
	}

	generatedIDs := make(map[int64]bool)
	// var firstID int64 = -1 // Removed unused variable

	t.Logf("assertSfidQueryResult: Extracted IDs from query '%s':", lastQuery)
	for i, match := range matches {
		if len(match) < 2 {
			t.Errorf("assertSfidQueryResult: Regex failed to capture ID in match %d for query: %s", i, lastQuery)
			continue
		}
		idStr := match[1]
		id, err := strconv.ParseInt(idStr, 10, 64)
		if err != nil {
			t.Errorf("assertSfidQueryResult: Failed to parse extracted ID '%s' as int64: %v", idStr, err)
			continue
		}

		t.Logf("  - Row %d: ID = %d", i, id)

		// if i == 0 { // Removed unused variable assignment
		// 	firstID = id
		// }

		// Check for uniqueness
		if _, exists := generatedIDs[id]; exists {
			t.Errorf("assertSfidQueryResult: Duplicate generated ID found: %d in query: %s", id, lastQuery)
		}
		generatedIDs[id] = true
	}

	// Optional: Add a check for the number of rows if needed, e.g., based on expectedStructureHint
	// expectedRowCount := strings.Count(expectedStructureHint, "$sfid")
	// assert.Equal(t, expectedRowCount, len(matches), "Number of generated IDs does not match expected rows")

	// The main test already checks db.Create().Error, so we don't need to compare the full SQL string here.
	// This helper now focuses solely on verifying the uniqueness of generated IDs.
}

func mysqlDialector() bool {
	return os.Getenv("DIALECTOR") == "mysql" || os.Getenv("DIALECTOR") == "mariadb"
}

func mariadbDialector() bool {
	return os.Getenv("DIALECTOR") == "mariadb"
}

func shardingHasher4Algorithm(columnValue any) (suffix string, err error) {
	str, ok := columnValue.(string)
	if !ok {
		return "", fmt.Errorf("expected string, got %T", columnValue)
	}

	// Use a default value if name is empty
	if str == "" {
		str = "default"
	}

	// Create a new FNV-1a 32-bit hash.
	hasher := fnv.New32a()
	_, err = hasher.Write([]byte(str))
	if err != nil {
		return "", fmt.Errorf("failed to write to hasher: %v", err)
	}
	hashValue := hasher.Sum32()

	// Assume we have 4 shards; adjust as needed.
	suffix = fmt.Sprintf("_%d", hashValue%4)
	return suffix, nil
}

// shardingHasher32Algorithm returns a shard suffix (_0 through _31)
// based on the CRC-32 hash of the input string.
func shardingHasher32Algorithm(columnValue any) (suffix string, err error) {
	// Handle nil case
	if columnValue == nil {
		return "_0", nil // Default shard for nil values
	}

	// Convert to string and handle different types
	var str string
	switch v := columnValue.(type) {
	case int:
		str = fmt.Sprintf("%d", v)
	case *int:
		if v == nil {
			return "_0", nil
		}
	case int32:
		str = fmt.Sprintf("%d", v)
	case *int32:
		if v == nil {
			return "_0", nil
		}
	case int64:
		str = fmt.Sprintf("%d", v)
	case *int64:
		if v == nil {
			return "_0", nil
		}
	case string:
		str = v
	case *string:
		if v == nil {
			return "_0", nil
		}
		str = *v
	default:
		return "", fmt.Errorf("expected string or *string, got %T", columnValue)
	}

	// Handle empty string case
	if str == "" {
		return "_0", nil // Default shard for empty strings
	}

	// Use standard CRC-32 implementation from Go's standard library
	// This is more reliable and well-tested than custom implementations
	crc := crc32.ChecksumIEEE([]byte(str))

	// Calculate shard number (modulo 32 to get a number from 0 to 31)
	shardNum := crc % 32

	// Return shard suffix (e.g., "_0", "_1", etc.)
	return fmt.Sprintf("_%d", shardNum), nil
}

// Function to safely get value from pointer types
func dereferenceValue(value interface{}) interface{} {
	if value == nil {
		return nil
	}

	v := reflect.ValueOf(value)
	if v.Kind() == reflect.Ptr {
		if v.IsNil() {
			return nil
		}
		return v.Elem().Interface()
	}
	return value
}

func assertQueryResult(t *testing.T, expected string, middleware *Sharding) {
	t.Helper()
	normalize := func(query string) string {
		// Remove quotes around identifiers
		re := regexp.MustCompile(`"(\w+)"`)
		query = re.ReplaceAllString(query, `$1`)
		// Replace parameter numbers with a placeholder
		query = regexp.MustCompile(`\$\d+`).ReplaceAllString(query, `$?`)
		// Normalize whitespace
		query = strings.TrimSpace(query)
		query = regexp.MustCompile(`\s+`).ReplaceAllString(query, ` `)
		return query
	}
	normalizedExpected := normalize(toDialect(expected))
	normalizedActual := normalize(middleware.LastQuery())
	if normalizedExpected != normalizedActual {
		t.Errorf("\nExpected:\n%s\nActual:\n%s", normalizedExpected, normalizedActual)
	}
}

// NameShardingConfig creates a Config with name-based sharding algorithm
func NameShardingConfig(numberOfShards uint) Config {
	config := Config{
		DoubleWrite:    true,
		ShardingKey:    "name",
		NumberOfShards: numberOfShards,
		ShardingAlgorithm: func(columnValue any) (suffix string, err error) {
			//fmt.Println("columnValue: ", columnValue)
			str, ok := columnValue.(string)
			if !ok {
				return "", fmt.Errorf("expected string, got %T", columnValue)
			}

			// Use a default value if name is empty
			if str == "" {
				str = "default"
			}

			// Create a new FNV-1a 32-bit hash.
			hasher := fnv.New32a()
			_, err = hasher.Write([]byte(str))
			if err != nil {
				return "", fmt.Errorf("failed to write to hasher: %v", err)
			}
			hashValue := hasher.Sum32()

			// Use modulo to determine the shard
			suffix = fmt.Sprintf("_%d", hashValue%uint32(numberOfShards))
			//fmt.Println("suffix: ", suffix)
			return suffix, nil
		},
	}
	return config
}
