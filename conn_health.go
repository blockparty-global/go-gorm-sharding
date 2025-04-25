package sharding

import (
	"context"
	"database/sql"
	"sync"
	"time"
)

// ConnectionHealthChecker monitors and maintains database connections.
type ConnectionHealthChecker struct {
	sharding      *Sharding
	ticker        *time.Ticker
	shutdown      chan struct{}
	wg            sync.WaitGroup
	lastCheckTime time.Time
	statsEnabled  bool
	cleanupStats  struct {
		totalCleanups     int
		totalTransactions int
		lastCleanupTime   time.Time
	}
}

// NewConnectionHealthChecker creates a new connection health checker.
func NewConnectionHealthChecker(s *Sharding, interval time.Duration) *ConnectionHealthChecker {
	return &ConnectionHealthChecker{
		sharding:      s,
		ticker:        time.NewTicker(interval),
		shutdown:      make(chan struct{}),
		lastCheckTime: time.Now(),
		statsEnabled:  true,
	}
}

// Start begins the health checker background routine.
func (c *ConnectionHealthChecker) Start() {
	c.wg.Add(1)
	go func() {
		defer c.wg.Done()

		for {
			select {
			case <-c.ticker.C:
				c.performHealthCheck()
			case <-c.shutdown:
				c.ticker.Stop()
				return
			}
		}
	}()

	infoLog("Connection health checker started with interval %v", c.ticker.C)
}

// Stop gracefully shuts down the health checker.
func (c *ConnectionHealthChecker) Stop() {
	close(c.shutdown)
	c.wg.Wait()
	infoLog("Connection health checker stopped")
}

// performHealthCheck executes a health check cycle.
func (c *ConnectionHealthChecker) performHealthCheck() {
	if !GetConfig().Connection.EnableAutoCleanup {
		return
	}

	debugLog("Performing connection health check")
	c.lastCheckTime = time.Now()

	// Cleanup expired transactions
	count := c.sharding.txRegistry.CleanupExpiredTransactions()
	if count > 0 {
		infoLog("Cleaned up %d expired transactions", count)

		// Update stats
		if c.statsEnabled {
			c.cleanupStats.totalCleanups++
			c.cleanupStats.totalTransactions += count
			c.cleanupStats.lastCleanupTime = time.Now()
		}
	}

	// Get database connection for health check
	db, err := c.sharding.DB.DB()
	if err != nil {
		errorLog("Failed to get database connection for health check: %v", err)
		return
	}

	// Perform a simple query to check database health
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := db.PingContext(ctx); err != nil {
		errorLog("Database health check failed: %v", err)
	}

	// Check connection pool stats
	c.logConnectionStats(db)
}

// logConnectionStats logs information about the connection pool.
func (c *ConnectionHealthChecker) logConnectionStats(db *sql.DB) {
	stats := db.Stats()

	debugLog("Connection pool stats: open=%d in-use=%d idle=%d wait-count=%d wait-duration=%v max-idle-closed=%d max-lifetime-closed=%d",
		stats.OpenConnections, stats.InUse, stats.Idle, stats.WaitCount,
		stats.WaitDuration, stats.MaxIdleClosed, stats.MaxLifetimeClosed)
}

// GetStats returns statistics about the health checker.
func (c *ConnectionHealthChecker) GetStats() map[string]interface{} {
	return map[string]interface{}{
		"last_check_time":    c.lastCheckTime,
		"total_cleanups":     c.cleanupStats.totalCleanups,
		"total_transactions": c.cleanupStats.totalTransactions,
		"last_cleanup_time":  c.cleanupStats.lastCleanupTime,
	}
}
