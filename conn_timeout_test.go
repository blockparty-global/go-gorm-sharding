package sharding

import (
	"database/sql"
	"fmt"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/stretchr/testify/assert"
)

func TestConfigureDatabaseTimeouts(t *testing.T) {
	// Create a mock database connection
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("Failed to create mock database: %v", err)
	}
	defer db.Close()

	// Create a sharding instance with PostgreSQL engine
	s := &Sharding{
		_config: Config{
			engine: EnginePostgreSQL,
		},
	}
	// Load the actual config to get the timeout value
	config := GetConfig()
	expectedIdleTimeout := config.Connection.TransactionTimeout * 1000
	expectedStmtTimeout := config.Connection.TransactionTimeout * 1000
	expectedLockTimeout := 10000 // Default lock timeout

	// Set up expectations for the PostgreSQL timeout settings based on config
	mock.ExpectExec(fmt.Sprintf("SET idle_in_transaction_session_timeout = %d", expectedIdleTimeout)).WillReturnResult(sqlmock.NewResult(0, 0))
	mock.ExpectExec(fmt.Sprintf("SET statement_timeout = %d", expectedStmtTimeout)).WillReturnResult(sqlmock.NewResult(0, 0))
	mock.ExpectExec(fmt.Sprintf("SET lock_timeout = %d", expectedLockTimeout)).WillReturnResult(sqlmock.NewResult(0, 0))

	// Call the function with the mock database
	err = s.ConfigureDatabaseTimeouts(db)
	assert.NoError(t, err)

	// Verify that all expectations were met
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("Unfulfilled expectations: %s", err)
	}
}

func TestConfigurePostgreSQLTimeouts(t *testing.T) {
	// Create a mock database connection
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("Failed to create mock database: %v", err)
	}
	defer db.Close()

	// Create connection config with custom values
	config := ConnectionConfig{
		TransactionTimeout: 600, // 10 minutes
	}

	// Set up expectations for the PostgreSQL timeout settings with custom values
	mock.ExpectExec("SET idle_in_transaction_session_timeout = 600000").WillReturnResult(sqlmock.NewResult(0, 0))
	mock.ExpectExec("SET statement_timeout = 600000").WillReturnResult(sqlmock.NewResult(0, 0))
	mock.ExpectExec("SET lock_timeout = 10000").WillReturnResult(sqlmock.NewResult(0, 0))

	// Call the function with the mock database
	err = configurePostgreSQLTimeouts(db, config)
	assert.NoError(t, err)

	// Verify that all expectations were met
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("Unfulfilled expectations: %s", err)
	}
}

func TestConfigureMySQLTimeouts(t *testing.T) {
	// Create a mock database connection
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("Failed to create mock database: %v", err)
	}
	defer db.Close()

	// Create connection config with custom values
	config := ConnectionConfig{
		TransactionTimeout: 600, // 10 minutes
	}

	// Set up expectations for the MySQL timeout settings
	mock.ExpectExec("SET SESSION wait_timeout = 600").WillReturnResult(sqlmock.NewResult(0, 0))
	mock.ExpectExec("SET SESSION innodb_lock_wait_timeout = 50").WillReturnResult(sqlmock.NewResult(0, 0))

	// Call the function with the mock database
	err = configureMySQLTimeouts(db, config)
	assert.NoError(t, err)

	// Verify that all expectations were met
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("Unfulfilled expectations: %s", err)
	}
}

func TestConfigureSQLiteTimeouts(t *testing.T) {
	// Create a mock database connection
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("Failed to create mock database: %v", err)
	}
	defer db.Close()

	// Create connection config with custom values
	config := ConnectionConfig{
		TransactionTimeout: 600, // 10 minutes
	}

	// Set up expectations for the SQLite timeout settings
	mock.ExpectExec("PRAGMA busy_timeout = 600000").WillReturnResult(sqlmock.NewResult(0, 0))

	// Call the function with the mock database
	err = configureSQLiteTimeouts(db, config)
	assert.NoError(t, err)

	// Verify that all expectations were met
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("Unfulfilled expectations: %s", err)
	}
}

func TestConfigureDatabaseTimeoutsError(t *testing.T) {
	// Create a mock database connection
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("Failed to create mock database: %v", err)
	}
	defer db.Close()

	// Create a sharding instance with PostgreSQL engine
	s := &Sharding{
		_config: Config{
			engine: EnginePostgreSQL,
		},
	}
	// Load the actual config to get the timeout value
	config := GetConfig()
	expectedIdleTimeout := config.Connection.TransactionTimeout * 1000

	// Set up expectations with an error, using the actual config value
	mock.ExpectExec(fmt.Sprintf("SET idle_in_transaction_session_timeout = %d", expectedIdleTimeout)).WillReturnError(sql.ErrConnDone)

	// Call the function with the mock database
	err = s.ConfigureDatabaseTimeouts(db)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "failed to set idle_in_transaction_session_timeout")

	// Verify that all expectations were met
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("Unfulfilled expectations: %s", err)
	}
}
