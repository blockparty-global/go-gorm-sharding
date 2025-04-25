package sharding

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestTransactionRegistry(t *testing.T) {
	registry := NewTransactionRegistry()

	// Test initial state
	assert.Equal(t, 0, registry.Count(), "Registry should start empty")
	assert.Empty(t, registry.ListExpiredTransactions(), "No expired transactions in empty registry")

	// Test register transaction
	txID, _, cancel := registry.Register(context.Background(), 5*time.Second)
	defer cancel()

	assert.NotEmpty(t, txID, "Transaction ID should not be empty")
	assert.Equal(t, 1, registry.Count(), "Registry should have one transaction")

	// Test get transaction
	txInfo, exists := registry.GetTransaction(txID)
	assert.True(t, exists, "Transaction should exist")
	assert.Equal(t, txID, txInfo.ID, "Transaction ID should match")
	assert.Equal(t, 5*time.Second, txInfo.Timeout, "Timeout should match")

	// Test unregister transaction
	registry.Unregister(txID)
	assert.Equal(t, 0, registry.Count(), "Registry should be empty after unregister")
	_, exists = registry.GetTransaction(txID)
	assert.False(t, exists, "Transaction should not exist after unregister")
}

func TestTransactionRegistryExpired(t *testing.T) {
	registry := NewTransactionRegistry()

	// Register a transaction with a very short timeout
	txID, _, cancel := registry.Register(context.Background(), 1*time.Millisecond)
	defer cancel()

	// Wait for it to expire
	time.Sleep(5 * time.Millisecond)

	// Check expired transactions
	expired := registry.ListExpiredTransactions()
	assert.Contains(t, expired, txID, "Transaction should be expired")

	// Test cleanup
	count := registry.CleanupExpiredTransactions()
	assert.Equal(t, 1, count, "One transaction should have been cleaned up")
	assert.Equal(t, 0, registry.Count(), "Registry should be empty after cleanup")
}

func TestTransactionRegistryConcurrency(t *testing.T) {
	registry := NewTransactionRegistry()
	numGoroutines := 100
	var wg sync.WaitGroup
	wg.Add(numGoroutines)

	// Register and unregister transactions concurrently
	for i := 0; i < numGoroutines; i++ {
		go func() {
			defer wg.Done()
			txID, _, cancel := registry.Register(context.Background(), 5*time.Second)
			// Small sleep to simulate some work
			time.Sleep(1 * time.Millisecond)
			registry.Unregister(txID)
			cancel() // Ensure context is cancelled
		}()
	}

	wg.Wait()
	assert.Equal(t, 0, registry.Count(), "Registry should be empty after all transactions are unregistered")
}

func TestTransactionRegistrySetConnID(t *testing.T) {
	registry := NewTransactionRegistry()

	// Register a transaction
	txID, _, cancel := registry.Register(context.Background(), 5*time.Second)
	defer cancel()

	// Set connection ID
	success := registry.SetConnID(txID, "conn-123")
	assert.True(t, success, "Setting connection ID should succeed")

	// Get transaction and verify connection ID
	txInfo, exists := registry.GetTransaction(txID)
	assert.True(t, exists, "Transaction should exist")
	assert.Equal(t, "conn-123", txInfo.ConnID, "Connection ID should be set")

	// Test getting transactions by connection ID
	txIDs := registry.GetTransactionsByConnID("conn-123")
	assert.Contains(t, txIDs, txID, "Transaction should be associated with connection ID")

	// Clean up by connection ID
	cleaned := registry.CleanupTransactionsByConnID("conn-123")
	assert.Equal(t, 1, cleaned, "One transaction should be cleaned up")
	assert.Equal(t, 0, registry.Count(), "Registry should be empty after cleanup")
}

func TestTransactionRegistryWithoutCancel(t *testing.T) {
	registry := NewTransactionRegistry()

	// Register two transactions
	txID1, ctx1, cancel1 := registry.Register(context.Background(), 5*time.Second)
	defer cancel1()
	txID2, ctx2, cancel2 := registry.Register(context.Background(), 5*time.Second)
	defer cancel2()

	// Unregister one normally, one without cancelling
	registry.Unregister(txID1)
	registry.UnregisterWithoutCancel(txID2)

	// Both transactions should be removed from registry
	assert.Equal(t, 0, registry.Count(), "Registry should be empty after unregistering")

	// The context from the first transaction should be cancelled (ctx.Done() should be closed)
	select {
	case <-ctx1.Done():
		// This is expected
	default:
		t.Error("Context should be cancelled after Unregister")
	}

	// The context from the second transaction should not be cancelled
	select {
	case <-ctx2.Done():
		t.Error("Context should not be cancelled after UnregisterWithoutCancel")
	default:
		// This is expected
	}
}

func TestRegisterWithID(t *testing.T) {
	registry := NewTransactionRegistry()

	// Test registering with a specific ID
	customID := "custom-tx-id"
	_, cancel := registry.RegisterWithID(customID, context.Background(), 5*time.Second)
	defer cancel()

	// Verify the transaction exists with the custom ID
	txInfo, exists := registry.GetTransaction(customID)
	assert.True(t, exists, "Transaction should exist with custom ID")
	assert.Equal(t, customID, txInfo.ID, "Transaction ID should match custom ID")
	assert.Equal(t, 1, registry.Count(), "Registry should have one transaction")

	// Unregister and verify
	registry.Unregister(customID)
	assert.Equal(t, 0, registry.Count(), "Registry should be empty after unregister")
}
