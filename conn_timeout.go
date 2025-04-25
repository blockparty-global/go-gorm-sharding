package sharding

import (
	"database/sql"
	"fmt"
	"time"
)

// ConfigureDatabaseTimeouts sets database-specific timeout parameters based on configuration.
// It should be called during connection initialization.
func (s *Sharding) ConfigureDatabaseTimeouts(db *sql.DB) error {
	config := GetConfig()

	// Apply connection pool settings
	db.SetConnMaxLifetime(time.Duration(config.Connection.MaxLifetime) * time.Second)
	db.SetMaxIdleConns(10)  // Can be configurable later
	db.SetMaxOpenConns(100) // Can be configurable later

	// Apply database-specific timeouts based on engine type
	switch s._config.engine {
	case EnginePostgreSQL:
		return configurePostgreSQLTimeouts(db, config.Connection)
	case EngineMySQL:
		return configureMySQLTimeouts(db, config.Connection)
	case EngineSQLite:
		// SQLite has limited timeout options
		return configureSQLiteTimeouts(db, config.Connection)
	default:
		return fmt.Errorf("unsupported database engine for timeout configuration")
	}
}

// configurePostgreSQLTimeouts sets PostgreSQL-specific timeout parameters.
func configurePostgreSQLTimeouts(db *sql.DB, config ConnectionConfig) error {
	// Convert seconds to milliseconds for PostgreSQL settings
	idleTimeout := config.TransactionTimeout * 1000
	stmtTimeout := config.TransactionTimeout * 1000
	lockTimeout := 10000 // 10 seconds default for lock timeout

	// Set idle transaction timeout to automatically terminate abandoned transactions
	if _, err := db.Exec(fmt.Sprintf("SET idle_in_transaction_session_timeout = %d", idleTimeout)); err != nil {
		return fmt.Errorf("failed to set idle_in_transaction_session_timeout: %w", err)
	}

	// Set statement timeout to prevent long-running queries
	if _, err := db.Exec(fmt.Sprintf("SET statement_timeout = %d", stmtTimeout)); err != nil {
		return fmt.Errorf("failed to set statement_timeout: %w", err)
	}

	// Set lock timeout to prevent deadlocks
	if _, err := db.Exec(fmt.Sprintf("SET lock_timeout = %d", lockTimeout)); err != nil {
		return fmt.Errorf("failed to set lock_timeout: %w", err)
	}

	return nil
}

// configureMySQLTimeouts sets MySQL-specific timeout parameters.
func configureMySQLTimeouts(db *sql.DB, config ConnectionConfig) error {
	// MySQL uses different timeout parameters
	if _, err := db.Exec(fmt.Sprintf("SET SESSION wait_timeout = %d", config.TransactionTimeout)); err != nil {
		return fmt.Errorf("failed to set wait_timeout: %w", err)
	}

	if _, err := db.Exec(fmt.Sprintf("SET SESSION innodb_lock_wait_timeout = %d", 50)); err != nil {
		return fmt.Errorf("failed to set innodb_lock_wait_timeout: %w", err)
	}

	return nil
}

// configureSQLiteTimeouts sets SQLite-specific timeout parameters.
func configureSQLiteTimeouts(db *sql.DB, config ConnectionConfig) error {
	// SQLite has limited configuration options
	if _, err := db.Exec(fmt.Sprintf("PRAGMA busy_timeout = %d", config.TransactionTimeout*1000)); err != nil {
		return fmt.Errorf("failed to set busy_timeout: %w", err)
	}

	return nil
}
