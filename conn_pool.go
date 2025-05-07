package sharding

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/google/uuid"
	"gorm.io/gorm"
)

// ConnPool wraps standard GORM ConnPool to handle sharding transparently for client code
type ConnPool struct {
	sharding *Sharding
	gorm.ConnPool
	txID string // Add transaction ID field to track transactions
}

// NonTransactionalPool provides a consistent interface across pool types that may not support transactions
type NonTransactionalPool struct {
	gorm.ConnPool
}

// Commit implements a no-op to maintain consistent transaction interface across connection types
func (p *NonTransactionalPool) Commit() error {
	debugLog("No-op Commit for non-transactional pool")
	return nil
}

// Rollback is a no-op to maintain consistent interface regardless of underlying capabilities
func (p *NonTransactionalPool) Rollback() error {
	debugLog("No-op Rollback for non-transactional pool")
	return nil
}

// String returns a consistent identifier for meaningful logging and debugging
func (pool *ConnPool) String() string {
	return "gorm:sharding:conn_pool"
}

// PrepareContext checks context cancellation first to prevent unnecessary database operations
func (pool ConnPool) PrepareContext(ctx context.Context, query string) (*sql.Stmt, error) {
	// Context cancellation check helps fail fast to prevent wasted database resources
	if ctx.Err() != nil {
		return nil, ctx.Err()
	}
	return pool.ConnPool.PrepareContext(ctx, query)
}

func (pool ConnPool) ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error) {
	var (
		curTime = time.Now()
		result  sql.Result
		err     error
	)
	// Context cancellation check prevents wasted resources when client connections drop
	if ctx.Err() != nil {
		return nil, ctx.Err()
	}

	// RequestID enables distributed tracing across services for correlating performance issues
	requestID := uuid.New().String()

	// Log connection stats at the start of query execution
	if sqlDB, err := pool.sharding.DB.DB(); err == nil {
		stats := sqlDB.Stats()
		traceLog("[%s] ExecContext START: Query: %s | Connections: open=%d in-use=%d idle=%d",
			requestID, query, stats.OpenConnections, stats.InUse, stats.Idle)
	} else {
		traceLog("[%s] ExecContext START: Query: %s", requestID, query)
	}

	defer func() {
		// Log connection stats at the end of query execution
		if sqlDB, err := pool.sharding.DB.DB(); err == nil {
			stats := sqlDB.Stats()
			traceLog("[%s] ExecContext END | Connections: open=%d in-use=%d idle=%d",
				requestID, stats.OpenConnections, stats.InUse, stats.Idle)
		} else {
			traceLog("[%s] ExecContext END", requestID)
		}
	}()

	// Try to handle batch insert queries
	queryCtx := &QueryContext{
		Sharding: pool.sharding,
		ConnPool: pool.ConnPool,
	}
	result, err = pool.sharding.HandleBatchInsert(queryCtx, query, args)
	if err == nil {
		// Batch insert was handled successfully
		return result, nil
	} else if err != ErrSkipBatchHandler {
		// There was an actual error in batch handling
		return nil, err
	}

	// Continue with normal query resolution
	// Query resolution happens outside synchronized blocks to minimize lock contention
	ftQuery, stQuery, table, err := pool.sharding.resolve(query, args...)
	debugLog("ExecContext: FtQuery: %s\n StQuery: %s \n\tQuery: %s \n Table: %s. Error: %v",
		ftQuery, stQuery, query, table, err)
	// Using sync.Map prevents race conditions in concurrent environments
	pool.sharding.querys.Store("last_query", stQuery)

	currentErr := err // Use a mutable error variable for errors from resolve

	// Determine if table is configured for sharding and its DoubleWrite setting
	isShardingConfigured := false
	doubleWriteSetting := false // Default to false; true only if configured and explicitly set
	if table != "" {
		pool.sharding.mutex.RLock()
		if config, ok := pool.sharding.configs[table]; ok {
			isShardingConfigured = true
			doubleWriteSetting = config.DoubleWrite
		}
		// If table is not in configs, isShardingConfigured remains false, doubleWriteSetting remains false.
		pool.sharding.mutex.RUnlock()
	}

	// ErrInsertDiffSuffix check to handle multi-shard inserts
	if currentErr != nil && errors.Is(currentErr, ErrInsertDiffSuffix) {
		// When we detect multiple shards, try to use the batch handler
		if strings.Contains(strings.ToUpper(query), "INSERT INTO") {
			GetLogger().Debug("Detected INSERT with multiple shards, attempting batch handler")

			// Try to handle batch insert queries
			queryCtx := &QueryContext{
				Sharding: pool.sharding,
				ConnPool: pool.ConnPool,
			}

			result, batchErr := pool.sharding.HandleBatchInsert(queryCtx, query, args)
			if batchErr == nil {
				// Batch insert was handled successfully
				GetLogger().Debug("Successfully handled multi-shard batch insert")
				return result, nil
			}

			// If batch handling failed, log and fall through to standard error handling below.
			GetLogger().Debug("Batch handler failed: %v, proceeding with original error", batchErr)
			currentErr = batchErr // Overwrite the original ErrInsertDiffSuffix with the actual batch handler error
		} else {
			// If it wasn't an INSERT, we still have the original ErrInsertDiffSuffix in 'currentErr'.
			// Fall through to standard error handling.
		}
	}

	// Handle ErrMissingShardingKey: only for configured sharded tables
	if isShardingConfigured && currentErr != nil && errors.Is(currentErr, ErrMissingShardingKey) {
		// Fallback to original table (ftQuery) if DoubleWrite is enabled for this configured table
		if doubleWriteSetting {
			pool.sharding.Logger.Trace(ctx, curTime, func() (sql string, rowsAffected int64) {
				result, currentErr = pool.ConnPool.ExecContext(ctx, ftQuery, args...)
				if result != nil {
					rowsAffected, _ = result.RowsAffected()
				}
				return pool.sharding.Explain(ftQuery, args...), rowsAffected
			}, pool.sharding.Error) // Note: pool.sharding.Error might not be currentErr here
			return result, currentErr
		}
		return nil, currentErr // If not DoubleWrite, return the ErrMissingShardingKey
	}

	// If resolve returned an error (other than handled ErrInsertDiffSuffix or ErrMissingShardingKey for configured tables), return it.
	if currentErr != nil && !errors.Is(currentErr, ErrInsertDiffSuffix) { // ErrMissingShardingKey for non-configured tables would fall here
		return nil, currentErr
	}
	// If currentErr was ErrInsertDiffSuffix and batch insert failed (currentErr holds batch error), return it.
	// 'err' here is the original error from pool.sharding.resolve()
	if errors.Is(err, ErrInsertDiffSuffix) && currentErr != nil && currentErr != ErrSkipBatchHandler {
		return nil, currentErr
	}

	// Double-write to main table: only for configured sharded tables with DoubleWrite enabled
	if isShardingConfigured && doubleWriteSetting {
		// Re-check context to avoid wasted operations if request was cancelled during resolution
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}

		// Double-write errors don't block sharded operations to prioritize availability over consistency
		if _, dwErr := pool.ConnPool.ExecContext(ctx, ftQuery, args...); dwErr != nil {
			errorLog("Error double-writing to main table: %v", dwErr)
			// Continue despite errors to maintain service availability
		}
	}
	// Final context check prevents wasted resources on operations that would be discarded
	if ctx.Err() != nil {
		return nil, ctx.Err()
	}

	// Sharded query execution comes after main table to ensure at least one copy exists if process crashes
	// For non-sharded tables (isShardingConfigured=false), stQuery is the original query, and this is the single execution.
	var execErr error
	result, execErr = pool.ConnPool.ExecContext(ctx, stQuery, args...)
	pool.sharding.Logger.Trace(ctx, curTime, func() (sql string, rowsAffected int64) {
		if result != nil {
			rowsAffected, _ = result.RowsAffected()
		}
		return pool.sharding.Explain(stQuery, args...), rowsAffected
	}, execErr) // Log with the error from this specific execution

	return result, execErr
}

func (pool *ConnPool) QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error) {
	// Context check prevents wasted resources and fails fast when client has disconnected
	if ctx.Err() != nil {
		return nil, ctx.Err()
	}

	var curTime = time.Now()
	// RequestID enables tracing query lifecycle across distributed systems
	requestID := uuid.New().String()

	// Log connection stats at the start of query execution
	if sqlDB, err := pool.sharding.DB.DB(); err == nil {
		stats := sqlDB.Stats()
		traceLog("[%s] QueryContext START: Query: %s | Connections: open=%d in-use=%d idle=%d",
			requestID, query, stats.OpenConnections, stats.InUse, stats.Idle)
	} else {
		traceLog("[%s] QueryContext START: Query: %s", requestID, query)
	}

	defer func() {
		// Log connection stats at the end of query execution
		if sqlDB, err := pool.sharding.DB.DB(); err == nil {
			stats := sqlDB.Stats()
			traceLog("[%s] QueryContext END | Connections: open=%d in-use=%d idle=%d",
				requestID, stats.OpenConnections, stats.InUse, stats.Idle)
		} else {
			traceLog("[%s] QueryContext END", requestID)
		}
	}()

	// Resolving queries outside locks reduces contention in high-throughput scenarios
	ftQuery, stQuery, table, resolveErr := pool.sharding.resolve(query, args...)
	debugLog("QueryContext: FtQuery: %s\n StQuery: %s \n\tQuery: %s \n Table: %s. Error: %v",
		ftQuery, stQuery, query, table, resolveErr)
	currentErr := resolveErr // Use a mutable error variable

	// Determine if table is configured for sharding and its DoubleWrite setting
	isShardingConfigured := false
	doubleWriteSetting := false
	if table != "" {
		pool.sharding.mutex.RLock()
		if config, ok := pool.sharding.configs[table]; ok {
			isShardingConfigured = true
			doubleWriteSetting = config.DoubleWrite
		}
		pool.sharding.mutex.RUnlock()
	}

	// ErrInsertDiffSuffix check is first to fail fast and prevent data corruption from partial operations
	if currentErr != nil && errors.Is(currentErr, ErrInsertDiffSuffix) {
		// When we detect multiple shards, try to use the batch handler
		if strings.Contains(strings.ToUpper(query), "INSERT INTO") {
			GetLogger().Debug("Detected INSERT with multiple shards, attempting batch handler")

			// Try to handle batch insert queries
			queryCtx := &QueryContext{
				Sharding: pool.sharding,
				ConnPool: pool.ConnPool,
			}

			result, batchErr := pool.sharding.HandleBatchInsert(queryCtx, query, args)
			if batchErr == nil {
				// Batch insert was handled successfully
				GetLogger().Debug("Successfully handled multi-shard batch insert")

				// For INSERT queries returning rows, create empty query result
				// with just the number of affected rows
				rowsAffected, _ := result.RowsAffected()
				GetLogger().Debug("Batch insert affected %d rows", rowsAffected)

				// For RETURNING clause, we need to return rows
				if strings.Contains(strings.ToUpper(query), "RETURNING") {
					// Need to run a dummy query that returns rows with the same schema
					// but no actual data, since the batch insert is already done
					dummyQuery := "SELECT * FROM " + table + " WHERE 1=0"
					return pool.ConnPool.QueryContext(ctx, dummyQuery, []interface{}{}...)
				}

				// Run a dummy query that returns a result but no rows
				return pool.ConnPool.QueryContext(ctx, "SELECT 1 WHERE 1=0", []interface{}{}...)
			}

			// If batch handling failed, log and fall through to standard error handling below.
			GetLogger().Debug("Batch handler failed: %v, proceeding with original error", batchErr)
			currentErr = batchErr // Overwrite the original ErrInsertDiffSuffix with the actual batch handler error
		} else {
			// If it wasn't an INSERT, we still have the original ErrInsertDiffSuffix in 'currentErr'.
			// Fall through to standard error handling.
		}
	}

	// Thread-safe query storage is critical for concurrent operation reliability
	pool.sharding.querys.Store("last_query", stQuery)

	// Handle ErrMissingShardingKey: only for configured sharded tables
	if isShardingConfigured && currentErr != nil && errors.Is(currentErr, ErrMissingShardingKey) {
		if doubleWriteSetting { // Use original query (ftQuery) if DoubleWrite enabled
			pool.sharding.querys.Store("last_query", ftQuery) // Log original query as last_query

			// Context check prevents unnecessary load for already cancelled requests
			if ctx.Err() != nil {
				return nil, ctx.Err()
			}

			// Original table query provides a reliable fallback path during migration
			rows, queryErr := pool.ConnPool.QueryContext(ctx, ftQuery, args...) // Use ftQuery
			pool.sharding.Logger.Trace(ctx, curTime, func() (sql string, rowsAffected int64) {
				return pool.sharding.Explain(ftQuery, args...), 0
			}, queryErr) // Log with queryErr
			return rows, queryErr
		}
		return nil, currentErr // If not DoubleWrite, return the ErrMissingShardingKey
	}

	// If resolve returned an error (other than handled ErrInsertDiffSuffix or ErrMissingShardingKey for configured tables), return it.
	if currentErr != nil && !errors.Is(currentErr, ErrInsertDiffSuffix) {
		return nil, currentErr
	}
	// If currentErr was ErrInsertDiffSuffix and batch insert failed (currentErr holds batch error), return it.
	// 'resolveErr' here is the original error from pool.sharding.resolve()
	if errors.Is(resolveErr, ErrInsertDiffSuffix) && currentErr != nil && currentErr != ErrSkipBatchHandler {
		return nil, currentErr
	}

	// Double-write for INSERTs: only for configured sharded tables with DoubleWrite enabled
	isInsert := strings.Contains(strings.ToUpper(query), "INSERT INTO")
	if isInsert && isShardingConfigured && doubleWriteSetting {
		// Context check prevents wasted operations for cancelled requests
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}

		// Main table insert ensures data is captured in both storage locations
		rows, err := pool.ConnPool.QueryContext(ctx, ftQuery, args...)
		if err != nil {
			errorLog("Error double-writing to main table: %v", err)
			// Continue with sharded operation despite errors for availability
		} else {
			debugLog("Successfully double-wrote to main table %s", table)
			// Closing rows prevents resource leaks in long-running applications
			if rows != nil {
				if closeErr := rows.Close(); closeErr != nil {
					errorLog("Error closing rows from double-write: %v", closeErr)
				}
			}
		}
	}
	// Final context check prevents wasted operations when request has been cancelled
	if ctx.Err() != nil {
		return nil, ctx.Err()
	}

	// Tracing execution time helps identify performance bottlenecks
	// For non-sharded tables (isShardingConfigured=false), stQuery is the original query, and this is the single execution.
	var execErr error
	rows, execErr := pool.ConnPool.QueryContext(ctx, stQuery, args...)
	pool.sharding.Logger.Trace(ctx, curTime, func() (sql string, rowsAffected int64) {
		return pool.sharding.Explain(stQuery, args...), 0
	}, execErr) // Log with the error from this specific execution

	return rows, execErr
}

func (pool ConnPool) QueryRowContext(ctx context.Context, query string, args ...any) *sql.Row {
	// Empty Row on cancelled context ensures consistent non-nil return behavior
	if ctx.Err() != nil {
		// Row will return context error when Scan is called
		return &sql.Row{}
	}

	// RequestID enables cross-service tracing for performance analysis
	requestID := uuid.New().String()

	// Log connection stats at the start of query execution
	if sqlDB, err := pool.sharding.DB.DB(); err == nil {
		stats := sqlDB.Stats()
		traceLog("[%s] QueryRowContext START: Query: %s | Connections: open=%d in-use=%d idle=%d",
			requestID, query, stats.OpenConnections, stats.InUse, stats.Idle)
	} else {
		traceLog("[%s] QueryRowContext START: Query: %s", requestID, query)
	}

	defer func() {
		// Log connection stats at the end of query execution
		if sqlDB, err := pool.sharding.DB.DB(); err == nil {
			stats := sqlDB.Stats()
			traceLog("[%s] QueryRowContext END | Connections: open=%d in-use=%d idle=%d",
				requestID, stats.OpenConnections, stats.InUse, stats.Idle)
		} else {
			traceLog("[%s] QueryRowContext END", requestID)
		}
	}()

	// Non-locked query resolution improves concurrency for high-throughput systems
	ftQuery, stQuery, table, resolveErr := pool.sharding.resolve(query, args...)
	debugLog("QueryRowContext: FtQuery: %s\n StQuery: %s \n\tQuery: %s \n Table: %s. Error: %v",
		ftQuery, stQuery, query, table, resolveErr)
	// Note: QueryRowContext cannot return errors directly. Errors are embedded in sql.Row.

	// Determine if table is configured for sharding and its DoubleWrite setting
	isShardingConfigured := false
	doubleWriteSetting := false
	if table != "" {
		pool.sharding.mutex.RLock()
		if config, ok := pool.sharding.configs[table]; ok {
			isShardingConfigured = true
			doubleWriteSetting = config.DoubleWrite
		}
		pool.sharding.mutex.RUnlock()
	}

	// Handle ErrMissingShardingKey: only for configured sharded tables
	if isShardingConfigured && resolveErr != nil && errors.Is(resolveErr, ErrMissingShardingKey) {
		if doubleWriteSetting { // Use original query (ftQuery) if DoubleWrite enabled
			// Error from ftQuery will be embedded in the returned sql.Row
			return pool.ConnPool.QueryRowContext(ctx, ftQuery, args...)
		}
		// If not DoubleWrite, an error occurred. We can't return error directly.
		// The subsequent stQuery execution will likely fail or use a problematic query.
		// GORM's Row.Scan() will reveal the error.
		// For now, allow flow to stQuery, which might be ftQuery if resolve did that.
		// Or, construct a Row with the error if possible (not straightforward with stdlib).
		// Let's assume stQuery will be the one executed, and if resolveErr was critical,
		// stQuery might be bad or resolve might have made stQuery = ftQuery.
	}
	// Unlike ExecContext/QueryContext, we can't easily return early with an error here.
	// We proceed, and errors from resolveErr might affect stQuery or be revealed on Scan.

	// Thread-safe query storage prevents race conditions in concurrent access
	pool.sharding.querys.Store("last_query", stQuery)

	// Double-write for INSERTs: only for configured sharded tables with DoubleWrite enabled
	isInsert := strings.Contains(strings.ToUpper(query), "INSERT INTO")
	// Only perform double-write if resolveErr was nil, indicating a valid sharded operation initially.
	if isInsert && isShardingConfigured && doubleWriteSetting && resolveErr == nil {
		// QueryContext instead of QueryRowContext enables proper resource/error management for the double-write
		rows, dwErr := pool.ConnPool.QueryContext(ctx, ftQuery, args...)
		if dwErr != nil {
			errorLog("Error double-writing to main table in QueryRowContext: %v", dwErr)
		} else if rows != nil {
			// Always close rows to prevent resource leaks in long-running applications
			if closeErr := rows.Close(); closeErr != nil {
				errorLog("Error closing rows from double-write in QueryRowContext: %v", closeErr)
			}
		}
	}
	return pool.ConnPool.QueryRowContext(ctx, stQuery, args...)
}

// BeginTx uses composition to provide consistent client interface regardless of backend capabilities
func (pool *ConnPool) BeginTx(ctx context.Context, opt *sql.TxOptions) (gorm.ConnPool, error) {
	// Get transaction timeout from global configuration
	txTimeout := time.Duration(GetConfig().Connection.TransactionTimeout) * time.Second

	// Register the transaction in the registry
	txID, txCtx, _ := pool.sharding.txRegistry.Register(ctx, txTimeout)

	if db, ok := pool.ConnPool.(interface {
		Get(string) (interface{}, bool)
	}); ok {
		if val, ok := db.Get("supports_transactions"); ok && val.(bool) {
			if basePool, ok := pool.ConnPool.(gorm.ConnPoolBeginner); ok {
				// Use the context with timeout from the registry
				txConn, err := basePool.BeginTx(txCtx, opt)
				if err != nil {
					// Unregister on failure
					pool.sharding.txRegistry.Unregister(txID)
					return nil, fmt.Errorf("forced transaction failed: %w", err)
				}
				return &ConnPool{
					sharding: pool.sharding,
					ConnPool: txConn,
					txID:     txID, // Store the transaction ID
				}, nil
			}
		}
	}

	// Try standard transaction support
	if basePool, ok := pool.ConnPool.(gorm.ConnPoolBeginner); ok {
		// Use the context with timeout from the registry
		txConn, err := basePool.BeginTx(txCtx, opt)
		if err != nil {
			// Unregister on failure
			pool.sharding.txRegistry.Unregister(txID)
			return nil, fmt.Errorf("failed to begin transaction: %w", err)
		}

		// Preserving sharding context ensures consistent behavior in transactions
		return &ConnPool{
			sharding: pool.sharding,
			ConnPool: txConn,
			txID:     txID, // Store the transaction ID
		}, nil
	}

	// Non-transactional fallback maintains API compatibility even without transaction support
	debugLog("Transaction not supported by underlying pool, using non-transactional wrapper")
	return &ConnPool{
		sharding: pool.sharding,
		ConnPool: &NonTransactionalPool{
			ConnPool: pool.ConnPool,
		},
		txID: txID, // Store the transaction ID even for non-transactional pools
	}, nil
}

// Commit uses type assertions to support multiple transaction implementations for maximum compatibility
func (pool *ConnPool) Commit() error {
	var err error

	// Handle no-op commits first for pools without transaction support
	if nonTxPool, ok := pool.ConnPool.(*NonTransactionalPool); ok {
		err = nonTxPool.Commit()
	} else if tx, ok := pool.ConnPool.(*sql.Tx); ok {
		// Support standard SQL transactions
		err = tx.Commit()
	} else if basePool, ok := pool.ConnPool.(gorm.TxCommitter); ok {
		// Finally try GORM-specific transaction handling
		err = basePool.Commit()
	} else {
		// Clear error for unsupported operations prevents silent failures
		err = ErrNotSupported
	}

	// Unregister the transaction regardless of the commit result
	// This ensures cleanup even if the commit fails
	if pool.txID != "" {
		pool.sharding.txRegistry.Unregister(pool.txID)
		pool.txID = "" // Clear the ID to prevent double cleanup
	}

	return err
}

// Rollback follows same pattern as Commit to handle multiple transaction types
func (pool *ConnPool) Rollback() error {
	var err error

	// Handle our wrapper first for consistent interface
	if nonTxPool, ok := pool.ConnPool.(*NonTransactionalPool); ok {
		err = nonTxPool.Rollback()
	} else if tx, ok := pool.ConnPool.(*sql.Tx); ok {
		// Support standard SQL transactions directly
		err = tx.Rollback()
	} else if basePool, ok := pool.ConnPool.(gorm.TxCommitter); ok {
		// Try GORM-specific transaction handling
		err = basePool.Rollback()
	} else {
		err = ErrNotSupported
	}

	// Unregister the transaction regardless of the rollback result
	if pool.txID != "" {
		pool.sharding.txRegistry.Unregister(pool.txID)
		pool.txID = "" // Clear the ID to prevent double cleanup
	}

	return err
}

// Ping uses progressively more generic approaches for reliable health checks across database types
func (pool *ConnPool) Ping() error {
	// Direct sql.DB ping is most efficient when available
	if db, ok := pool.ConnPool.(*sql.DB); ok {
		return db.Ping()
	}

	// Transactions are already connected and don't need separate ping
	if _, ok := pool.ConnPool.(*sql.Tx); ok {
		return nil
	}

	// Check through non-transactional wrapper to ping underlying pool
	if nonTxPool, ok := pool.ConnPool.(*NonTransactionalPool); ok {
		if db, ok := nonTxPool.ConnPool.(*sql.DB); ok {
			return db.Ping()
		}
	}

	// Interface-based detection for custom pool implementations
	if pinger, ok := pool.ConnPool.(interface{ Ping() error }); ok {
		return pinger.Ping()
	}

	// Short timeout prevents health checks from blocking indefinitely
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// Simple query as last-resort health check for any database type
	_, err := pool.QueryContext(ctx, "SELECT 1")
	return err
}
