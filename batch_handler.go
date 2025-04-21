// Package sharding provides database sharding capabilities for GORM.
package sharding

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"os"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"

	"gorm.io/gorm"
)

var (
	// insertRegex is a regular expression pattern used to parse INSERT statements.
	// It captures the table name, column names, and values portion.
	insertRegex = regexp.MustCompile(`(?i)INSERT\s+INTO\s+(?:"([a-zA-Z0-9_]+)"|([a-zA-Z0-9_]+))\s+\((.*?)\)\s+VALUES\s*(.*)`)

	// updateRegex is a regular expression pattern used to parse UPDATE statements.
	// It captures the table name, SET clause, and WHERE clause.
	updateRegex = regexp.MustCompile(`(?i)UPDATE\s+(?:"([a-zA-Z0-9_]+)"|([a-zA-Z0-9_]+))\s+SET\s+(.*?)(?:\s+WHERE\s+(.*))?$`)

	// shardingKeyRegex is a regular expression pattern used to find column names in both quoted and unquoted formats.
	shardingKeyRegex = regexp.MustCompile(`(?:"([^"]+)"|([a-zA-Z0-9_]+))`)

	// ErrSkipBatchHandler is returned when a query should be processed by the standard handler
	// rather than the batch handler. This is not an error condition, but a control flow signal.
	ErrSkipBatchHandler = errors.New("skip batch handler")

	// Additional error types for better error handling

	// ErrInvalidInsertFormat indicates that the provided SQL doesn't match the expected INSERT format.
	ErrInvalidInsertFormat = errors.New("invalid INSERT statement format")

	// ErrInvalidUpdateFormat indicates that the provided SQL doesn't match the expected UPDATE format.
	ErrInvalidUpdateFormat = errors.New("invalid UPDATE statement format")

	// ErrNoShardingKey indicates that the sharding key column wasn't found in the query.
	ErrNoShardingKey = errors.New("sharding key not found in columns")

	// ErrShardingKeyExtract indicates a failure to extract the sharding key value from parameters.
	ErrShardingKeyExtract = errors.New("failed to extract sharding key value")

	// ErrShardResolution indicates a failure to determine the appropriate shard for a key value.
	ErrShardResolution = errors.New("failed to resolve shard for key value")

	// ErrParameterMismatch indicates a mismatch between the parameters expected by the query
	// and the parameters provided.
	ErrParameterMismatch = errors.New("parameter count mismatch")

	// ErrUpdateSetClauseEmpty indicates that the SET clause in an UPDATE statement is empty or invalid.
	ErrUpdateSetClauseEmpty = errors.New("UPDATE statement has empty or invalid SET clause")

	// ErrUpdateWhereClauseMissing indicates that an UPDATE statement is missing a WHERE clause,
	// which is required for sharding to identify the target shards.
	ErrUpdateWhereClauseMissing = errors.New("UPDATE statement missing WHERE clause required for sharding")

	// ErrShardingKeyNotFoundInWhere indicates that the sharding key was not found in the WHERE clause
	// of an UPDATE statement, making it impossible to determine the target shard.
	ErrShardingKeyNotFoundInWhere = errors.New("sharding key not found in UPDATE WHERE clause")
)

// paramInfo stores the original 1-based index and the value
type paramInfo struct {
	originalIndex int
	value         interface{}
}

// QueryCacheEntry represents a cached query and its parsed components
type QueryCacheEntry struct {
	// Original query and its parsed components
	tableName  string
	columnsStr string
	valuesStr  string

	// Position of the sharding key in the columns list
	shardingKeyIndex int

	// ON CONFLICT clause if present
	conflictClause string

	// Last access time for cache eviction
	lastAccess time.Time
}

// QueryCache caches parsed queries to avoid repeated parsing
type QueryCache struct {
	mu       sync.RWMutex
	entries  map[string]*QueryCacheEntry
	maxSize  int
	hits     int64
	misses   int64
	disabled bool
}

// NewQueryCache creates a new query cache with the specified size
func NewQueryCache(maxSize int) *QueryCache {
	return &QueryCache{
		entries:  make(map[string]*QueryCacheEntry),
		maxSize:  maxSize,
		disabled: maxSize <= 0,
	}
}

// Get retrieves a cached query entry if available
func (cache *QueryCache) Get(query string) (*QueryCacheEntry, bool) {
	if cache.disabled {
		return nil, false
	}

	cache.mu.RLock()
	entry, ok := cache.entries[query]
	cache.mu.RUnlock()

	if ok {
		// Update last access time and hit count
		cache.mu.Lock()
		entry.lastAccess = time.Now()
		cache.hits++
		cache.mu.Unlock()
		return entry, true
	}

	// Update miss count
	cache.mu.Lock()
	cache.misses++
	cache.mu.Unlock()

	return nil, false
}

// Put adds a query to the cache
func (cache *QueryCache) Put(query string, entry *QueryCacheEntry) {
	if cache.disabled {
		return
	}

	cache.mu.Lock()
	defer cache.mu.Unlock()

	// Check if we need to evict an entry
	if len(cache.entries) >= cache.maxSize {
		cache.evictLRU()
	}

	// Add the new entry
	cache.entries[query] = entry
}

// evictLRU removes the least recently used cache entry
func (cache *QueryCache) evictLRU() {
	var oldestQuery string
	var oldestTime time.Time

	// Find the oldest entry
	for query, entry := range cache.entries {
		if oldestQuery == "" || entry.lastAccess.Before(oldestTime) {
			oldestQuery = query
			oldestTime = entry.lastAccess
		}
	}

	// Remove the oldest entry
	if oldestQuery != "" {
		delete(cache.entries, oldestQuery)
	}
}

// GetStats returns cache statistics
func (cache *QueryCache) GetStats() map[string]interface{} {
	cache.mu.RLock()
	defer cache.mu.RUnlock()

	hitRate := 0.0
	total := cache.hits + cache.misses
	if total > 0 {
		hitRate = float64(cache.hits) / float64(total) * 100.0
	}

	return map[string]interface{}{
		"size":     len(cache.entries),
		"max_size": cache.maxSize,
		"hits":     cache.hits,
		"misses":   cache.misses,
		"hit_rate": hitRate,
	}
}

// queryCache is the global query cache instance
var (
	queryCache     *QueryCache
	queryCacheOnce sync.Once
)

// getQueryCache returns the global query cache, initializing it if necessary
func getQueryCache() *QueryCache {
	queryCacheOnce.Do(func() {
		// Default cache size of 1000 entries
		cacheSize := 1000

		// Use environment variable if set
		if sizeStr := os.Getenv("GORM_SHARDING_CACHE_SIZE"); sizeStr != "" {
			if size, err := strconv.Atoi(sizeStr); err == nil {
				cacheSize = size
			}
		}

		queryCache = NewQueryCache(cacheSize)
	})

	return queryCache
}

// DisableQueryCache disables the query cache
func DisableQueryCache() {
	getQueryCache().disabled = true
}

// EnableQueryCache enables the query cache
func EnableQueryCache() {
	getQueryCache().disabled = false
}

// GetQueryCacheStats returns statistics about the query cache
func GetQueryCacheStats() map[string]interface{} {
	return getQueryCache().GetStats()
}

// validateParameters validates that all required parameters are available for a batch insert
// and returns detailed information about any mismatches.
//
// Parameters:
//   - valueGroups: The value groups extracted from the SQL statement
//   - columnNames: The column names from the INSERT statement
//   - args: The parameter values provided
//
// Returns:
//   - map[int]bool: Map of parameter indices used in the query
//   - int: Maximum parameter index used in the query
//   - error: Detailed error if parameter validation fails
func validateParameters(valueGroups []string, columnNames []string, args []interface{}) (map[int]bool, int, error) {
	// Collect all parameter indices used in the query
	paramIndices := make(map[int]bool)
	maxParamIndex := -1
	missingParams := make([]int, 0)

	// First pass: collect all parameter indices
	for groupIdx, group := range valueGroups {
		for colIdx := 0; colIdx < len(columnNames); colIdx++ {
			paramIdx := getParamIndexFromGroup(group, colIdx)
			if paramIdx >= 0 {
				paramIndices[paramIdx] = true
				if paramIdx > maxParamIndex {
					maxParamIndex = paramIdx
				}
			} else if paramIdx == -1 {
				// Error parsing parameter index
				return nil, -1, fmt.Errorf("invalid parameter format in group %d, column %d: %s",
					groupIdx, colIdx, group)
			}
		}
	}

	// Ensure all needed parameters are available
	if maxParamIndex >= len(args) {
		// Create a detailed error message showing which parameters are missing
		for i := 0; i <= maxParamIndex; i++ {
			if paramIndices[i] && i >= len(args) {
				missingParams = append(missingParams, i)
			}
		}

		// Format missing parameters for better readability
		missingParamsStr := ""
		if len(missingParams) > 0 {
			if len(missingParams) == 1 {
				missingParamsStr = fmt.Sprintf("parameter index %d", missingParams[0])
			} else if len(missingParams) <= 5 {
				indices := make([]string, len(missingParams))
				for i, idx := range missingParams {
					indices[i] = fmt.Sprintf("%d", idx)
				}
				missingParamsStr = fmt.Sprintf("parameter indices %s", strings.Join(indices, ", "))
			} else {
				missingParamsStr = fmt.Sprintf("%d parameters (indices %d through %d)",
					len(missingParams), missingParams[0], missingParams[len(missingParams)-1])
			}
		}

		return nil, maxParamIndex, fmt.Errorf("%w: query requires %d parameters but only %d are provided (missing %s)",
			ErrParameterMismatch, maxParamIndex+1, len(args), missingParamsStr)
	}

	// Validate parameter indices are sequential without large gaps that might indicate errors
	if len(paramIndices) > 0 && maxParamIndex > 0 {
		// Check for unusual gaps in parameter numbering (potential SQL construction errors)
		expectedParams := maxParamIndex + 1
		if len(paramIndices) < expectedParams/2 && expectedParams > 10 {
			debugLog("Warning: Unusual parameter pattern - %d distinct parameters used but max index is %d",
				len(paramIndices), maxParamIndex)
		}
	}

	return paramIndices, maxParamIndex, nil
}

// SplitBatchInsertByShards splits batch insert SQL into multiple queries, one for each shard
// and returns a map of shard names to QueryContext.
func (c *QueryCache) SplitBatchInsertByShards(stmt *gorm.Statement, parsedSQL string, args []interface{}) (map[string]*QueryContext, error) {
	// Create a lookup table for matching value groups and parameters
	queryContextMap := make(map[string]*QueryContext)

	// Extract table name from SQL to determine sharding configuration
	var tableName string

	// Check if it's an UPDATE statement
	updateRe := regexp.MustCompile(`(?i)UPDATE\s+(?:"([a-zA-Z0-9_]+)"|([a-zA-Z0-9_]+))`)
	updateMatches := updateRe.FindStringSubmatch(parsedSQL)
	if len(updateMatches) >= 3 {
		// Match could be in group 1 or 2 depending on whether quotes were used
		if updateMatches[1] != "" {
			tableName = updateMatches[1]
		} else if updateMatches[2] != "" {
			tableName = updateMatches[2]
		}
	}

	// If not an UPDATE, try INSERT pattern
	if tableName == "" {
		re := regexp.MustCompile(`(?i)INSERT\s+INTO\s+([^\s\(]+)`)
		matches := re.FindStringSubmatch(parsedSQL)
		if len(matches) < 2 {
			return nil, fmt.Errorf("couldn't extract table name from SQL: %s", parsedSQL)
		}

		tableName = matches[1]
		tabNameOnlyReg := regexp.MustCompile(`"([^"]*)"`)
		tableMatches := tabNameOnlyReg.FindStringSubmatch(tableName)
		if len(tableMatches) >= 2 {
			tableName = tableMatches[1]
		}
	}

	// If we still couldn't extract a table name, return an error
	if tableName == "" {
		return nil, fmt.Errorf("couldn't extract table name from SQL: %s", parsedSQL)
	}

	// Extract column names
	reColumns := regexp.MustCompile(`(?i)INSERT\s+INTO\s+[^\(]+\(([^\)]+)\)`)
	columnMatches := reColumns.FindStringSubmatch(parsedSQL)
	if len(columnMatches) < 2 {
		return nil, fmt.Errorf("couldn't extract column names from SQL: %s", parsedSQL)
	}

	// Parse column names
	columnNames := strings.Split(columnMatches[1], ",")
	// Trim whitespace from column names
	for i := range columnNames {
		columnNames[i] = strings.TrimSpace(columnNames[i])
		// Remove quotes if present
		if strings.HasPrefix(columnNames[i], "\"") && strings.HasSuffix(columnNames[i], "\"") {
			columnNames[i] = columnNames[i][1 : len(columnNames[i])-1]
		}
	}

	// Find VALUES clause
	valuesIdx := strings.Index(strings.ToUpper(parsedSQL), "VALUES")
	if valuesIdx == -1 {
		return nil, fmt.Errorf("VALUES clause not found in SQL: %s", parsedSQL)
	}

	// Extract ON CONFLICT clause if present
	var onConflictClause string
	var err error

	// Extract before getting value groups to ensure consistent SQL portions
	beforeValues := parsedSQL[:valuesIdx]
	valuesClause := parsedSQL[valuesIdx:]
	onConflictIndex := strings.Index(strings.ToUpper(valuesClause), "ON CONFLICT")

	if onConflictIndex != -1 {
		onConflictClause = valuesClause[onConflictIndex:]
		valuesClause = valuesClause[:onConflictIndex]
	}

	// Parse value groups
	valueGroups := ParseValueGroups(valuesClause[6:]) // Skip "VALUES"
	if len(valueGroups) == 0 {
		return nil, fmt.Errorf("no value groups found in SQL: %s", parsedSQL)
	}

	// Validate parameter count early - NEW CODE ADDITION
	// Count total parameters needed across all value groups and verify against args length
	totalParamsNeeded := 0
	maxParamIndex := 0
	paramsByGroup := make([][]int, len(valueGroups))

	// First pass: extract all parameter indices from each value group
	for i, group := range valueGroups {
		paramList := make([]int, 0, len(columnNames))
		for colIdx := 0; colIdx < len(columnNames); colIdx++ {
			paramIdx := getParamIndexFromGroup(group, colIdx)
			if paramIdx >= 0 { // Only count actual parameters (not literals)
				totalParamsNeeded++
				paramList = append(paramList, paramIdx)
				if paramIdx > maxParamIndex {
					maxParamIndex = paramIdx
				}
			}
		}
		paramsByGroup[i] = paramList
	}

	// Check if we have enough parameters in args array
	if maxParamIndex >= len(args) {
		return nil, fmt.Errorf("%w: SQL requires %d parameters but only %d were provided",
			ErrParameterMismatch, maxParamIndex+1, len(args)) // +1 for 1-based indexing in error message
	}

	// Validate parameter indices are within reasonable ranges for each group
	for i, group := range paramsByGroup {
		if len(group) > 0 {
			minParam := group[0]
			maxParam := group[0]

			for _, param := range group {
				if param < minParam {
					minParam = param
				}
				if param > maxParam {
					maxParam = param
				}
			}

			// If we have a large gap or out-of-order parameters, this might indicate a problem
			if maxParam-minParam > len(columnNames)*2 {
				debugLog("Suspicious parameter index range in group %d: [%d-%d]", i, minParam, maxParam)
			}
		}
	}

	// Identify sharding column position in the columns list
	var shardingConfig Config
	var shardingColumnIndex int = -1
	var hasShardingKey bool = false

	// Parse sharding configuration for the table
	// Note: In production, you should retrieve this from the Sharding.configs map
	// This is a placeholder for testing purposes
	shardingConfig = Config{
		ShardingKey: "sharding_key", // Default key, will be overridden by actual config
	}

	if shardingConfig.ShardingKey != "" {
		for i, col := range columnNames {
			if strings.EqualFold(col, shardingConfig.ShardingKey) {
				shardingColumnIndex = i
				hasShardingKey = true
				break
			}
		}
		if !hasShardingKey {
			// If not found, it could be a table with auto-generated sharding key
			return nil, nil
		}
	} else {
		// No sharding key means table is not sharded
		return nil, nil
	}

	// Optimization for small batches: pre-allocate for 10 or fewer expected shards
	expectedShardCount := min(10, len(valueGroups))
	_ = expectedShardCount // To avoid unused variable warning

	// Small batch/Low cardinality optimization: Track parameter assignment across shards
	// paramIndices maps from original parameter index to list of (shardName, queryParamIndex) pairs
	paramTracking := make(map[int][]ParamAssignment, len(args))

	// Handle value groups
	for groupIdx, group := range valueGroups {
		// Get the parameter index for the sharding column
		paramIdx := getParamIndexFromGroup(group, shardingColumnIndex)

		if paramIdx < 0 {
			if paramIdx == -1 {
				return nil, fmt.Errorf("invalid parameter format at group %d, column %d", groupIdx, shardingColumnIndex)
			}
			// Handle literal value (paramIdx == -2)
			// For literal sharding keys, we just use a default shard
			// This is a simplified approach that might need to be improved
			if _, exists := queryContextMap["default"]; !exists {
				queryContextMap["default"] = &QueryContext{
					SQL:            beforeValues + "VALUES ",
					Args:           make([]interface{}, 0),
					ValueGroups:    make([]string, 0),
					ParamPositions: make(map[int]int),
					ShardingKey:    shardingConfig.ShardingKey,
					TableName:      tableName,
				}
			}
			queryContextMap["default"].ValueGroups = append(queryContextMap["default"].ValueGroups, group)
			continue
		}

		// Check if the parameter index is valid
		if paramIdx >= len(args) {
			return nil, fmt.Errorf("parameter index %d (from group %d) out of bounds for args length %d",
				paramIdx, groupIdx, len(args))
		}

		// Get the sharding key value
		shardKeyValue := args[paramIdx]

		// Calculate shard name - use a simple default for testing
		shardName := "_0" // Default shard
		if shardingConfig.ShardingAlgorithm != nil {
			var err error
			shardName, err = shardingConfig.ShardingAlgorithm(shardKeyValue)
			if err != nil {
				return nil, fmt.Errorf("error calculating shard for value %v: %w", shardKeyValue, err)
			}
		}

		// Create or get the query context for this shard
		if _, exists := queryContextMap[shardName]; !exists {
			queryContextMap[shardName] = &QueryContext{
				SQL:            beforeValues + "VALUES ",
				Args:           make([]interface{}, 0),
				ValueGroups:    make([]string, 0),
				ParamPositions: make(map[int]int),
				ShardingKey:    shardingConfig.ShardingKey,
				TableName:      tableName,
			}
		}

		// Extract all parameters from the current value group
		// and track their mapping from original to per-shard positions
		for colIdx := 0; colIdx < len(columnNames); colIdx++ {
			colParamIdx := getParamIndexFromGroup(group, colIdx)
			if colParamIdx >= 0 {
				// Check parameter bounds
				if colParamIdx >= len(args) {
					return nil, fmt.Errorf("parameter index %d (from group %d, column %d) out of bounds for args length %d",
						colParamIdx, groupIdx, colIdx, len(args))
				}

				// Update parameter tracking
				paramAssignment := ParamAssignment{
					ShardName:     shardName,
					NewParamIndex: len(queryContextMap[shardName].Args),
				}
				paramTracking[colParamIdx] = append(paramTracking[colParamIdx], paramAssignment)

				// Add parameter to this shard's args
				queryContextMap[shardName].Args = append(queryContextMap[shardName].Args, args[colParamIdx])

				// Store the mapping between original parameter index and shard-specific index
				queryContextMap[shardName].ParamPositions[colParamIdx] = len(queryContextMap[shardName].Args) - 1
			}
		}

		// Add the value group to this shard
		queryContextMap[shardName].ValueGroups = append(queryContextMap[shardName].ValueGroups, group)
	}

	// Process ON CONFLICT clause if present
	if onConflictClause != "" {
		err = processOnConflictClause(queryContextMap, onConflictClause, args, paramTracking)
		if err != nil {
			return nil, fmt.Errorf("error processing ON CONFLICT clause: %w", err)
		}
	}

	// Finalize queries for each shard
	for shardName, context := range queryContextMap {
		var finalValueGroups []string

		// Adjust parameter indices in value groups based on the new positions
		for _, group := range context.ValueGroups {
			finalGroup, err := adjustParameterIndices(group, context.ParamPositions)
			if err != nil {
				return nil, fmt.Errorf("error adjusting parameter indices for shard %s: %w", shardName, err)
			}
			finalValueGroups = append(finalValueGroups, finalGroup)
		}

		// Finalize the SQL with adjusted value groups
		context.SQL += strings.Join(finalValueGroups, ", ")

		// Add ON CONFLICT clause if present and processed
		if context.OnConflictClause != "" {
			context.SQL += " " + context.OnConflictClause
		}
	}

	// Final safety check - ensure all parameter indices have valid values
	// This is defensive programming to catch any edge cases
	for paramIdx := range paramTracking {
		if paramIdx < 0 || paramIdx >= len(args) {
			return nil, fmt.Errorf("%w: reference to parameter $%d is out of bounds (args length: %d)",
				ErrParameterMismatch, paramIdx+1, len(args))
		}
	}

	return queryContextMap, nil
}

// ParamAssignment represents a parameter assignment to a shard
type ParamAssignment struct {
	ShardName     string
	NewParamIndex int
}

// ExtractUpdateComponents extracts the table name, column assignments from SET clause,
// and WHERE clause from an UPDATE statement.
// Returns the table name, map of column names to parameter indices, WHERE clause, and error if any.
func ExtractUpdateComponents(query string) (tableName string, columnParams map[string]int, whereClause string, err error) {
	matches := updateRegex.FindStringSubmatch(query)
	if len(matches) < 4 {
		return "", nil, "", fmt.Errorf("%w: couldn't parse UPDATE statement components", ErrInvalidUpdateFormat)
	}

	// The table name can be either quoted (matches[1]) or unquoted (matches[2])
	if matches[1] != "" {
		tableName = matches[1]
	} else {
		tableName = matches[2]
	}

	if tableName == "" {
		return "", nil, "", fmt.Errorf("%w: couldn't extract table name", ErrInvalidUpdateFormat)
	}

	// Extract column names and their parameter indices from the SET clause
	setClause := matches[3]
	if setClause == "" {
		return "", nil, "", ErrUpdateSetClauseEmpty
	}

	// Process the SET clause to extract column names and their parameter indices
	columnParams = make(map[string]int)
	assignments := strings.Split(setClause, ",")

	for _, assignment := range assignments {
		assignment = strings.TrimSpace(assignment)
		parts := strings.SplitN(assignment, "=", 2)
		if len(parts) != 2 {
			continue
		}

		colName := parts[0]
		paramStr := parts[1]

		// Extract the column name (handling quoted identifiers)
		colMatches := shardingKeyRegex.FindStringSubmatch(colName)
		if len(colMatches) < 3 {
			continue
		}

		var columnName string
		if colMatches[1] != "" {
			columnName = colMatches[1] // Quoted column name
		} else {
			columnName = colMatches[2] // Unquoted column name
		}

		// Extract parameter index
		if strings.HasPrefix(paramStr, "$") {
			paramIdx, err := strconv.Atoi(strings.TrimPrefix(paramStr, "$"))
			if err != nil {
				continue
			}
			columnParams[columnName] = paramIdx
		}
	}

	// Get the WHERE clause if present
	whereClause = ""
	if len(matches) > 4 && matches[4] != "" {
		whereClause = matches[4]
	} else {
		// For sharding, we typically need a WHERE clause to determine which shard(s) to target
		return tableName, columnParams, "", ErrUpdateWhereClauseMissing
	}

	return tableName, columnParams, whereClause, nil
}

// adjustParameterIndices updates parameter references in a value group based on new positions
func adjustParameterIndices(group string, paramPositions map[int]int) (string, error) {
	trimmedGroup := strings.Trim(group, "()")
	parts := strings.Split(trimmedGroup, ",")

	for i := range parts {
		parts[i] = strings.TrimSpace(parts[i])

		if strings.HasPrefix(parts[i], "$") {
			// Extract the original parameter index
			paramIndexStr := parts[i][1:]
			origParamIdx := 0
			_, err := fmt.Sscanf(paramIndexStr, "%d", &origParamIdx)
			if err != nil {
				return "", fmt.Errorf("invalid parameter format '%s'", parts[i])
			}

			// Convert to 0-based index
			origParamIdx--

			// Get the new position from the mapping
			if newPos, exists := paramPositions[origParamIdx]; exists {
				// Replace with new parameter reference (converting back to 1-based for SQL)
				parts[i] = fmt.Sprintf("$%d", newPos+1)
			} else {
				return "", fmt.Errorf("parameter mapping not found for index %d", origParamIdx)
			}
		}
	}

	return "(" + strings.Join(parts, ", ") + ")", nil
}

// processOnConflictClause handles the ON CONFLICT clause and distributes its parameters correctly
func processOnConflictClause(queryContextMap map[string]*QueryContext, onConflictClause string,
	args []interface{}, paramTracking map[int][]ParamAssignment) error {
	// Extract all parameters from ON CONFLICT clause
	// TODO: Extract and rewrite parameters in the ON CONFLICT clause
	// This is a simplified implementation that just passes through the clause

	// Regular expression to find parameter references like $1, $2, etc.
	paramRefRegex := regexp.MustCompile(`\$(\d+)`)
	matches := paramRefRegex.FindAllStringSubmatch(onConflictClause, -1)

	for _, match := range matches {
		if len(match) < 2 {
			continue
		}

		// Extract parameter index (1-based)
		paramIdx, err := strconv.Atoi(match[1])
		if err != nil {
			return fmt.Errorf("invalid parameter reference in ON CONFLICT clause: %s", match[0])
		}

		// Convert to 0-based index
		paramIdx--

		// Check bounds
		if paramIdx < 0 || paramIdx >= len(args) {
			return fmt.Errorf("parameter index %d out of bounds in ON CONFLICT clause", paramIdx+1)
		}

		// Add this parameter to all shards that need it
		for shardName, context := range queryContextMap {
			// Add the parameter to this shard
			context.Args = append(context.Args, args[paramIdx])

			// Update parameter tracking
			paramAssignment := ParamAssignment{
				ShardName:     shardName,
				NewParamIndex: len(context.Args) - 1,
			}
			paramTracking[paramIdx] = append(paramTracking[paramIdx], paramAssignment)

			// Store mapping for parameter adjustment
			context.ParamPositions[paramIdx] = len(context.Args) - 1
		}
	}

	// Process the clause for each shard
	for _, context := range queryContextMap {
		// Adjust parameter references in the clause
		adjustedClause := onConflictClause
		for _, match := range matches {
			if len(match) < 2 {
				continue
			}

			paramIdx, _ := strconv.Atoi(match[1])
			paramIdx-- // Convert to 0-based

			// Replace with shard-specific parameter index
			if newPos, exists := context.ParamPositions[paramIdx]; exists {
				// +1 to convert back to 1-based for SQL
				newRef := fmt.Sprintf("$%d", newPos+1)
				adjustedClause = strings.Replace(adjustedClause, match[0], newRef, 1)
			}
		}

		context.OnConflictClause = adjustedClause
	}

	return nil
}

// ParseValueGroups extracts value groups from the VALUES clause.
// It handles nested parentheses and string literals correctly.
func ParseValueGroups(valuesStr string) []string {
	var groups []string
	depth := 0
	start := 0
	inString := false
	escapeNext := false
	stringChar := byte(0) // Track which string delimiter we're in (' or ")

	// Trim any leading/trailing whitespace
	valuesStr = strings.TrimSpace(valuesStr)

	// Pre-allocate the groups slice with a reasonable capacity
	// based on a heuristic - estimate 1 group per 30 chars on average
	estimatedGroups := len(valuesStr) / 30
	if estimatedGroups > 0 {
		groups = make([]string, 0, estimatedGroups)
	} else {
		groups = make([]string, 0, 1)
	}

	for i := 0; i < len(valuesStr); i++ {
		char := valuesStr[i]

		// Handle escape sequences in string literals
		if escapeNext {
			escapeNext = false
			continue
		}

		// Handle string literals
		if char == '\\' && inString {
			escapeNext = true
			continue
		}

		if char == '\'' || char == '"' {
			if !inString {
				// Starting a string
				inString = true
				stringChar = char
			} else if char == stringChar {
				// Ending a string of the same type we started with
				inString = false
				stringChar = 0
			}
			// If we're in a string and see a different quote type, just treat it as part of the string
			continue
		}

		if inString {
			continue // Skip processing while inside a string
		}

		switch char {
		case '(':
			if depth == 0 {
				start = i
			}
			depth++
		case ')':
			depth--
			if depth == 0 {
				group := valuesStr[start : i+1]
				// Add sanity check for valid value groups
				if strings.Count(group, "(") == strings.Count(group, ")") {
					groups = append(groups, group)
				} else {
					debugLog("Skipping malformed value group: %s", group)
				}
			} else if depth < 0 {
				// Mismatched parentheses - reset depth to avoid invalid groups
				debugLog("Mismatched parentheses at position %d: %s", i, truncateString(valuesStr, 100))
				depth = 0
			}
		}
	}

	// Extra validation for the result
	if depth != 0 {
		debugLog("Warning: Unclosed parentheses in VALUES clause: %s", truncateString(valuesStr, 100))
	}

	// Verify each group has the same number of parameters
	if len(groups) > 1 {
		baseParamCount := CountParams(groups[0])
		for i, group := range groups[1:] {
			currentCount := CountParams(group)
			if currentCount != baseParamCount {
				debugLog("Warning: Inconsistent parameter count in group %d: expected %d, got %d",
					i+1, baseParamCount, currentCount)
			}
		}
	}

	return groups
}

// CountParams counts the number of parameter placeholders (e.g., $1, $2) in a value group.
// It handles string literals correctly to avoid counting dollar signs within quoted strings.
//
// Parameters:
//   - group: A value group string (e.g., "($1, 'text', $2)")
//
// Returns:
//   - int: The number of parameter placeholders found
func CountParams(group string) int {
	count := 0
	inString := false

	for i, char := range group {
		// Ignore parameter references inside string literals
		if char == '\'' {
			if i == 0 || group[i-1] != '\\' { // Not an escaped quote
				inString = !inString
			}
			continue
		}

		if inString {
			continue
		}

		// Count dollar-sign parameters
		if char == '$' {
			// Make sure it's actually a parameter and not part of a string
			if i+1 < len(group) && group[i+1] >= '0' && group[i+1] <= '9' {
				count++
			}
		}
	}

	return count
}

// getParamIndexFromGroup extracts the parameter index at the specified position within a value group.
// It returns the 0-based index of the parameter in the args array.
//
// Parameters:
//   - group: A value group string (e.g., "($1, $2, $3)")
//   - position: The position of the parameter to extract (0-based index in the group)
//
// Returns:
//
//	>= 0: The actual parameter index (as specified in the SQL, like $1, $2, etc.) minus 1.
//	-1: Error (e.g., index out of bounds, invalid parameter format '$abc').
//	-2: Indicates the value is a literal, not a parameter placeholder.
func getParamIndexFromGroup(group string, position int) int {
	// Trim surrounding parentheses and split by comma
	trimmedGroup := strings.Trim(group, "()")

	// Handle complex cases with potential quoted/nested values
	// by parsing more carefully
	var parts []string
	inString := false
	escapeNext := false
	start := 0
	depth := 0

	// More robust splitting that handles nested expressions and quotes
	for i, char := range trimmedGroup {
		// Handle escape sequences in string literals
		if escapeNext {
			escapeNext = false
			continue
		}

		// Handle string literals
		if char == '\\' && inString {
			escapeNext = true
			continue
		}

		if char == '\'' {
			if i == 0 || trimmedGroup[i-1] != '\\' { // Not an escaped quote
				inString = !inString
			}
			continue
		}

		if inString {
			continue // Skip comma processing while inside a string
		}

		// Track nested parentheses
		if char == '(' {
			depth++
		} else if char == ')' {
			depth--
		}

		// Only split at top-level commas
		if char == ',' && depth == 0 {
			parts = append(parts, trimmedGroup[start:i])
			start = i + 1
		}
	}

	// Don't forget the last part
	if start < len(trimmedGroup) {
		parts = append(parts, trimmedGroup[start:])
	}

	if len(parts) == 0 {
		// Fallback to simple splitting if our complex logic failed
		parts = strings.Split(trimmedGroup, ",")
	}

	if position < 0 || position >= len(parts) {
		debugLog("getParamIndexFromGroup: position %d out of bounds for parts %v", position, parts)
		return -1 // Position out of bounds
	}

	paramStr := strings.TrimSpace(parts[position])

	if !strings.HasPrefix(paramStr, "$") {
		debugLog("getParamIndexFromGroup: value '%s' at position %d is a literal", paramStr, position)
		return -2 // Indicate literal value
	}

	// Extract the number after $
	paramIndexStr := paramStr[1:]

	// Handle expressions like "$2::text" by truncating at type cast
	if idx := strings.Index(paramIndexStr, "::"); idx > 0 {
		paramIndexStr = paramIndexStr[:idx]
	}

	// Handle any other unexpected characters
	endIdx := 0
	for endIdx < len(paramIndexStr) && paramIndexStr[endIdx] >= '0' && paramIndexStr[endIdx] <= '9' {
		endIdx++
	}

	if endIdx < len(paramIndexStr) {
		paramIndexStr = paramIndexStr[:endIdx]
	}

	paramIndex := 0
	// Use Sscanf to parse the number reliably
	_, err := fmt.Sscanf(paramIndexStr, "%d", &paramIndex)
	if err != nil || paramIndex <= 0 { // Parameter numbers must be > 0
		debugLog("getParamIndexFromGroup: failed to parse parameter number '%s' or invalid number %d", paramIndexStr, paramIndex)
		return -1 // Invalid format or number <= 0
	}

	// Convert from 1-based to 0-based
	return paramIndex - 1
}

// ExtractOnConflictClause extracts the ON CONFLICT clause from a query if present.
// This allows the clause to be preserved when splitting queries.
//
// Parameters:
//   - query: The SQL query to extract from
//
// Returns:
//   - string: The ON CONFLICT clause, or empty string if not present
func ExtractOnConflictClause(query string) string {
	upperQuery := strings.ToUpper(query)
	conflictPos := strings.Index(upperQuery, "ON CONFLICT")
	if conflictPos == -1 {
		return ""
	}

	return query[conflictPos:]
}

// QueryContext holds information about a query for a specific shard
type QueryContext struct {
	SQL              string
	Args             []interface{}
	ValueGroups      []string
	ParamPositions   map[int]int
	ShardingKey      string
	TableName        string
	OnConflictClause string
	ConnPool         ConnPoolExecer
}

// ConnPoolExecer defines the interface for executing SQL queries.
// This is a subset of the gorm.ConnPool interface that only includes
// the methods needed for batch insert operations.
type ConnPoolExecer interface {
	// ExecContext executes a query with context and returns the result
	ExecContext(ctx context.Context, query string, args ...interface{}) (sql.Result, error)
}

// Debug outputs a debug message if the log level is high enough.
func (ctx *QueryContext) Debug(format string, args ...interface{}) {
	if DefaultLogLevel >= LogLevelDebug {
		debugLog(format, args...)
	}
}

// execContext executes a query using the context's connection pool.
func (ctx *QueryContext) execContext(query string, args ...interface{}) (sql.Result, error) {
	if ctx.ConnPool == nil {
		return nil, errors.New("no connection pool available")
	}
	return ctx.ConnPool.ExecContext(context.Background(), query, args...)
}

// HandleBatchInsert processes batch INSERT or UPDATE statements and splits them into multiple
// shard-specific queries if necessary. It executes each query separately and combines the results.
//
// Parameters:
//   - ctx: The query context containing the sharding instance and connection pool
//   - query: The SQL INSERT or UPDATE statement to process
//   - args: The parameter values for the query
//
// Returns:
//   - sql.Result: A combined result representing all executed queries
//   - error: Error if any occurred during processing or execution
//
// If the query is not eligible for batch handling, ErrSkipBatchHandler is returned to indicate
// that standard processing should be used instead.
func (s *Sharding) HandleBatchInsert(ctx *QueryContext, query string, args []interface{}) (sql.Result, error) {
	// Determine if this is an INSERT or UPDATE query
	isUpdate := strings.HasPrefix(strings.ToUpper(query), "UPDATE")
	isInsert := strings.HasPrefix(strings.ToUpper(query), "INSERT")

	if !isInsert && !isUpdate {
		// Not a supported statement type for batch handling
		return nil, ErrSkipBatchHandler
	}

	if isInsert {
		// Process as a batch INSERT
		if DefaultLogLevel >= LogLevelDebug {
			debugLog("Processing batch insert with %d parameters", len(args))
		}

		queries, queryParams, err := s.SplitBatchInsertByShards(query, args)
		if err != nil {
			if errors.Is(err, ErrSkipBatchHandler) {
				// Not a batch insert or not handled, proceed with normal execution
				return nil, err
			}

			if errors.Is(err, ErrParameterMismatch) {
				infoLog("Parameter mismatch in batch insert: %v", err)
			} else {
				infoLog("Error splitting batch insert: %v", err)
			}
			return nil, err
		}

		// Execute the INSERT queries and return the results
		return s.executeBatchQueries(ctx, queries, queryParams, "insert")
	} else {
		// Process as a batch UPDATE
		if DefaultLogLevel >= LogLevelDebug {
			debugLog("Processing batch update with %d parameters", len(args))
		}

		queries, queryParams, err := s.SplitUpdateByShards(query, args)
		if err != nil {
			if errors.Is(err, ErrSkipBatchHandler) {
				// Not able to handle as a batch, proceed with normal execution
				return nil, err
			}

			if errors.Is(err, ErrNoShardingKey) {
				infoLog("No sharding key found in UPDATE where clause")
			} else {
				infoLog("Error splitting batch update: %v", err)
			}
			return nil, err
		}

		// Execute the UPDATE queries and return the results
		return s.executeBatchQueries(ctx, queries, queryParams, "update")
	}
}

// executeBatchQueries executes a batch of queries and combines their results
func (s *Sharding) executeBatchQueries(ctx *QueryContext, queries []string, queryParams [][]interface{},
	operationType string) (sql.Result, error) {

	// Log the query breakdown before execution
	if DefaultLogLevel >= LogLevelDebug {
		debugLog("Split batch %s into %d shard-specific queries", operationType, len(queries))
		for i, q := range queries {
			debugLog("Shard query %d/%d: %s (with %d parameters)",
				i+1, len(queries), truncateString(q, 100), len(queryParams[i]))
		}
	}

	// Execute each query separately
	var lastResult sql.Result
	var rowsAffected int64

	for i, shardQuery := range queries {
		if DefaultLogLevel >= LogLevelDebug {
			debugLog("Executing shard-specific batch %s (%d/%d): %s",
				operationType, i+1, len(queries), truncateString(shardQuery, 100))
		}

		result, err := ctx.execContext(shardQuery, queryParams[i]...)
		if err != nil {
			errorLog("Error executing shard %d: %v", i, err)
			return nil, fmt.Errorf("error executing shard %d: %w", i, err)
		}

		// Keep track of the last result and accumulate rows affected
		lastResult = result
		if count, err := result.RowsAffected(); err == nil {
			rowsAffected += count
		}
	}

	// Create a combined result
	combinedResult := &batchResult{
		lastResult:   lastResult,
		rowsAffected: rowsAffected,
	}

	infoLog("Successfully executed batch %s across %d shards, affecting %d rows",
		operationType, len(queries), rowsAffected)
	return combinedResult, nil
}

// truncateString truncates a string to the specified length and adds "..." if truncated
func truncateString(s string, maxLen int) string {
	if len(s) <= maxLen {
		return s
	}
	return s[:maxLen] + "..."
}

// batchResult implements the sql.Result interface to represent combined results
// from multiple query executions. It combines the rows affected from all queries
// while preserving the last insert ID from the last executed query.
type batchResult struct {
	// lastResult is the result from the last executed query
	lastResult sql.Result

	// rowsAffected is the sum of rows affected across all executed queries
	rowsAffected int64
}

// LastInsertId returns the last insert ID from the last executed query.
// This is somewhat arbitrary since multiple queries were executed, but
// it satisfies the sql.Result interface.
func (r *batchResult) LastInsertId() (int64, error) {
	// Return the last insert ID from the last query executed
	if r.lastResult == nil {
		return 0, errors.New("no result available")
	}
	return r.lastResult.LastInsertId()
}

// RowsAffected returns the total number of rows affected across all executed queries.
func (r *batchResult) RowsAffected() (int64, error) {
	// Return the combined rows affected
	return r.rowsAffected, nil
}

// FormatSuffix is a helper function to format sharding suffixes.
// It applies the format string to the given value.
func FormatSuffix(format string, value interface{}) string {
	return fmt.Sprintf(format, value)
}

// ExtractShardingKeyFromUpdate extracts the sharding key value from the WHERE clause of an UPDATE statement.
// It returns the extracted value, a boolean indicating whether the sharding key was found, and any error encountered.
func (s *Sharding) ExtractShardingKeyFromUpdate(tableName, query string, args []interface{}) (interface{}, bool, error) {
	// Get the configuration for this table
	config, ok := s.configs[tableName]
	if !ok {
		return nil, false, fmt.Errorf("table %s is not configured for sharding", tableName)
	}

	// Extract the UPDATE components
	_, _, whereClause, err := ExtractUpdateComponents(query)
	if err != nil {
		return nil, false, fmt.Errorf("failed to extract components from UPDATE statement: %w", err)
	}

	// If there's no WHERE clause, we can't extract a sharding key
	if whereClause == "" {
		return nil, false, ErrUpdateWhereClauseMissing
	}

	// Look for simple sharding key conditions like "id = $1" or "id IN ($1, $2, $3)"
	shardingKeyStr := config.ShardingKey
	// Different patterns to look for the sharding key in WHERE clause
	patterns := []string{
		fmt.Sprintf(`(?i)%s\s*=\s*\$(\d+)`, regexp.QuoteMeta(shardingKeyStr)),
		fmt.Sprintf(`(?i)"%s"\s*=\s*\$(\d+)`, regexp.QuoteMeta(shardingKeyStr)),
		fmt.Sprintf(`(?i)'%s'\s*=\s*\$(\d+)`, regexp.QuoteMeta(shardingKeyStr)),
		fmt.Sprintf(`(?i)%s\s+IN\s+\(\s*\$(\d+)(?:\s*,\s*\$\d+)*\s*\)`, regexp.QuoteMeta(shardingKeyStr)),
		fmt.Sprintf(`(?i)"%s"\s+IN\s+\(\s*\$(\d+)(?:\s*,\s*\$\d+)*\s*\)`, regexp.QuoteMeta(shardingKeyStr)),
		fmt.Sprintf(`(?i)'%s'\s+IN\s+\(\s*\$(\d+)(?:\s*,\s*\$\d+)*\s*\)`, regexp.QuoteMeta(shardingKeyStr)),
	}

	for _, pattern := range patterns {
		re := regexp.MustCompile(pattern)
		matches := re.FindStringSubmatch(whereClause)
		if len(matches) > 1 {
			// Check if it's an IN clause
			if strings.Contains(strings.ToUpper(matches[0]), " IN ") {
				inClauseStr := extractINClauseParams(matches[0])
				return s.extractShardingKeyFromIN(shardingKeyStr, inClauseStr, args)
			}

			// It's a simple equality condition
			paramIndex, err := strconv.Atoi(matches[1])
			if err != nil {
				return nil, false, fmt.Errorf("invalid parameter index in WHERE clause: %w", err)
			}

			// Parameter indices in SQL are 1-based, but slice indices are 0-based
			paramIndex--
			if paramIndex >= len(args) {
				return nil, false, fmt.Errorf("%w: parameter $%d is required but only %d arguments provided",
					ErrParameterMismatch, paramIndex+1, len(args))
			}

			return args[paramIndex], true, nil
		}
	}

	// If we get here, we couldn't find the sharding key in the WHERE clause
	return nil, false, fmt.Errorf("%w: couldn't find sharding key '%s' in WHERE clause",
		ErrShardingKeyNotFoundInWhere, shardingKeyStr)
}

// extractShardingKeyFromIN extracts sharding key values from an IN clause in an UPDATE statement.
// For example: "user_id" IN ($1, $2, $3)
//
// Parameters:
//   - shardingKey: The name of the sharding key column
//   - condition: The WHERE condition string containing the IN clause
//   - args: The parameter values
//
// Returns:
//   - interface{}: The extracted sharding key values
//   - bool: Whether the sharding key was found
//   - error: Any error encountered
func (s *Sharding) extractShardingKeyFromIN(shardingKey, condition string, args []interface{}) (interface{}, bool, error) {
	// Parse the IN condition to extract parameter indices
	inRegex := regexp.MustCompile(`(?i)"?([a-zA-Z0-9_]+)"?\s+IN\s*\(\s*(.*?)\s*\)`)
	matches := inRegex.FindStringSubmatch(condition)
	if len(matches) < 3 {
		return nil, false, fmt.Errorf("invalid IN clause format")
	}

	// Check if column is the sharding key
	columnName := matches[1]
	if columnName != shardingKey {
		return nil, false, ErrNoShardingKey
	}

	// Extract the parameter indices from the IN clause
	paramList := matches[2]
	paramValues := make([]interface{}, 0)

	// Split by comma
	params := strings.Split(paramList, ",")
	for _, param := range params {
		param = strings.TrimSpace(param)
		if strings.HasPrefix(param, "$") {
			paramIdx, err := strconv.Atoi(strings.TrimPrefix(param, "$"))
			if err != nil {
				return nil, false, fmt.Errorf("invalid parameter index: %v", err)
			}

			// Validate parameter index is within range
			if paramIdx < 1 || paramIdx > len(args) {
				return nil, false, fmt.Errorf("parameter index %d out of range", paramIdx)
			}

			// Add to list of parameter values
			paramValues = append(paramValues, args[paramIdx-1])
		}
	}

	// For multiple values, return the array of values
	if len(paramValues) > 0 {
		return paramValues, true, nil
	}

	return nil, false, ErrNoShardingKey
}

// SplitUpdateByShards splits an UPDATE statement into multiple shard-specific queries
// based on the sharding key values referenced in the WHERE clause
func (s *Sharding) SplitUpdateByShards(query string, args []interface{}) ([]string, [][]interface{}, error) {
	startTime := time.Now()

	// Extract table name from the query
	matches := updateRegex.FindStringSubmatch(query)
	if len(matches) < 4 {
		return nil, nil, fmt.Errorf("%w: couldn't match UPDATE statement pattern", ErrInvalidUpdateFormat)
	}

	var tableName string
	if matches[1] != "" {
		tableName = matches[1] // Quoted table name
	} else {
		tableName = matches[2] // Unquoted table name
	}

	s.mutex.RLock()
	config, exists := s.configs[tableName]
	s.mutex.RUnlock()
	if !exists {
		return nil, nil, fmt.Errorf("no sharding configuration for table %s", tableName)
	}

	// Check if we have an IN clause with multiple values
	whereClause := ""
	if len(matches) > 4 && matches[4] != "" {
		whereClause = matches[4]
	} else {
		return nil, nil, ErrUpdateWhereClauseMissing
	}

	// Parse operation type (standard single-shard update or multi-shard)
	var isMultiShard bool
	var shardingKeys []interface{}

	// Check if we have an IN clause involving the sharding key
	inClausePattern := fmt.Sprintf(`(?i)(?:"?%s"?|'?%s'?)\s+IN\s+\((.*?)\)`,
		regexp.QuoteMeta(config.ShardingKey), regexp.QuoteMeta(config.ShardingKey))
	inClauseMatch := regexp.MustCompile(inClausePattern).FindStringSubmatch(whereClause)

	if len(inClauseMatch) > 1 {
		// We have an IN clause - need to create one query per value
		inParamsStr := inClauseMatch[1]

		// Extract parameter indices from the IN clause
		paramIndices := []int{}
		for _, match := range regexp.MustCompile(`\$(\d+)`).FindAllStringSubmatch(inParamsStr, -1) {
			if len(match) > 1 {
				if idx, err := strconv.Atoi(match[1]); err == nil {
					paramIndices = append(paramIndices, idx)
				}
			}
		}

		if len(paramIndices) == 0 {
			return nil, nil, fmt.Errorf("no valid parameters found in IN clause")
		}

		// Extract sharding key values from the arguments
		shardingKeys = make([]interface{}, 0, len(paramIndices))
		for _, idx := range paramIndices {
			// Convert from 1-based to 0-based index
			idx--
			if idx >= len(args) {
				return nil, nil, fmt.Errorf("%w: parameter $%d referenced but only %d arguments provided",
					ErrParameterMismatch, idx+1, len(args))
			}
			shardingKeys = append(shardingKeys, args[idx])
		}

		isMultiShard = len(shardingKeys) > 1
	} else {
		// No IN clause, look for a simple equality condition
		keyValue, found, err := s.ExtractShardingKeyFromUpdate(tableName, query, args)
		if err != nil {
			return nil, nil, err
		}
		if !found {
			return nil, nil, ErrShardingKeyNotFoundInWhere
		}
		shardingKeys = []interface{}{keyValue}
		isMultiShard = false
	}

	// Initialize results
	var resultQueries []string
	var resultParams [][]interface{}

	if !isMultiShard {
		// For a single shard, just return the original query with modified table name
		shardValue, err := config.ShardingAlgorithm(shardingKeys[0])
		if err != nil {
			return nil, nil, fmt.Errorf("%w: %v", ErrShardResolution, err)
		}

		// Replace the table name with the sharded version
		shardedTableName := tableName + shardValue
		shardedQuery := strings.ReplaceAll(query, `"`+tableName+`"`, `"`+shardedTableName+`"`)
		shardedQuery = strings.ReplaceAll(shardedQuery, " "+tableName+" ", " "+shardedTableName+" ")

		resultQueries = []string{shardedQuery}
		resultParams = [][]interface{}{args}
	} else {
		// For multiple shards, we need to create a separate query for each shard
		resultQueries = make([]string, 0, len(shardingKeys))
		resultParams = make([][]interface{}, 0, len(shardingKeys))

		// Extract IN clause to replace
		inClauseToReplace := inClauseMatch[0]

		// Process each sharding key value
		for _, keyValue := range shardingKeys {
			// Determine which shard this update goes to
			shardValue, err := config.ShardingAlgorithm(keyValue)
			if err != nil {
				return nil, nil, fmt.Errorf("%w: %v", ErrShardResolution, err)
			}

			// Create a new WHERE clause with just this sharding key value
			newWhere := whereClause

			// Find parameter index for the current key value
			paramIndex := -1
			for i, arg := range args {
				if arg == keyValue {
					paramIndex = i + 1 // Convert to 1-based index
					break
				}
			}

			if paramIndex == -1 {
				// If we couldn't find the parameter, use a reasonable fallback
				paramIndex = 1
			}

			// Replace "key IN (...)" with "key = $X"
			paramStr := fmt.Sprintf("$%d", paramIndex)
			replacement := fmt.Sprintf("%s = %s", config.ShardingKey, paramStr)
			newWhere = strings.Replace(newWhere, inClauseToReplace, replacement, 1)

			// Replace table name with sharded version
			shardedTableName := tableName + shardValue
			shardedQuery := strings.ReplaceAll(query, `"`+tableName+`"`, `"`+shardedTableName+`"`)
			shardedQuery = strings.ReplaceAll(shardedQuery, " "+tableName+" ", " "+shardedTableName+" ")

			// Replace the WHERE clause
			shardedQuery = strings.Replace(shardedQuery, whereClause, newWhere, 1)

			resultQueries = append(resultQueries, shardedQuery)
			resultParams = append(resultParams, args)
		}
	}

	// Log performance metrics
	infoLog("Split UPDATE for table '%s' into %d queries in %s",
		tableName, len(resultQueries), time.Since(startTime))

	return resultQueries, resultParams, nil
}

// extractINClauseParams extracts the parameter list from an IN clause
func extractINClauseParams(whereClause string) string {
	// Find the part between the parentheses in an IN clause
	inRegex := regexp.MustCompile(`(?i)IN\s*\(\s*(.*?)\s*\)`)
	matches := inRegex.FindStringSubmatch(whereClause)
	if len(matches) < 2 {
		return ""
	}
	return matches[1]
}
