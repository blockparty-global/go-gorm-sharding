package sharding

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestConnectionConfigDefaults(t *testing.T) {
	// Get default configuration
	config := DefaultConfig()

	// Verify connection configuration has correct defaults
	assert.Equal(t, 3600, config.Connection.MaxLifetime, "Default MaxLifetime should be 3 seconds")
	assert.Equal(t, 900, config.Connection.TransactionTimeout, "Default TransactionTimeout should be 3 seconds")
	assert.Equal(t, 60, config.Connection.HealthCheckInterval, "Default HealthCheckInterval should be 60 seconds (1 minute)")
	assert.Equal(t, 3, config.Connection.MaxRetries, "Default MaxRetries should be 3")
	assert.True(t, config.Connection.EnableAutoCleanup, "Default EnableAutoCleanup should be true")
}
