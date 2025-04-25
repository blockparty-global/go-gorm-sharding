package sharding

import (
	"context"
	"sync"
	"time"

	"github.com/google/uuid"
)

// TransactionRegistry keeps track of all active transactions to enable monitoring and cleanup.
type TransactionRegistry struct {
	mu           sync.RWMutex
	transactions map[string]*TransactionInfo
}

// TransactionInfo stores information about an active transaction.
type TransactionInfo struct {
	ID        string
	StartTime time.Time
	ConnID    string
	Timeout   time.Duration
	Context   context.Context
	Cancel    context.CancelFunc
}

// NewTransactionRegistry creates a new transaction registry.
func NewTransactionRegistry() *TransactionRegistry {
	return &TransactionRegistry{
		transactions: make(map[string]*TransactionInfo),
	}
}

// Register adds a new transaction to the registry.
func (tr *TransactionRegistry) Register(ctx context.Context, timeout time.Duration) (string, context.Context, context.CancelFunc) {
	tr.mu.Lock()
	defer tr.mu.Unlock()

	// Generate a unique transaction ID
	txID := uuid.New().String()

	// Create a context with timeout
	txCtx, cancel := context.WithTimeout(ctx, timeout)

	// Store transaction information
	tr.transactions[txID] = &TransactionInfo{
		ID:        txID,
		StartTime: time.Now(),
		Timeout:   timeout,
		Context:   txCtx,
		Cancel:    cancel,
	}

	// Log transaction start
	debugLog("Started transaction %s with timeout %v", txID, timeout)

	return txID, txCtx, cancel
}

// RegisterWithID adds a new transaction to the registry with a specified ID.
func (tr *TransactionRegistry) RegisterWithID(txID string, ctx context.Context, timeout time.Duration) (context.Context, context.CancelFunc) {
	tr.mu.Lock()
	defer tr.mu.Unlock()

	// Create a context with timeout
	txCtx, cancel := context.WithTimeout(ctx, timeout)

	// Store transaction information
	tr.transactions[txID] = &TransactionInfo{
		ID:        txID,
		StartTime: time.Now(),
		Timeout:   timeout,
		Context:   txCtx,
		Cancel:    cancel,
	}

	// Log transaction start
	debugLog("Started transaction %s with timeout %v", txID, timeout)

	return txCtx, cancel
}

// Unregister removes a transaction from the registry.
func (tr *TransactionRegistry) Unregister(txID string) {
	tr.mu.Lock()
	defer tr.mu.Unlock()

	if info, exists := tr.transactions[txID]; exists {
		// Cancel the context to ensure resources are released
		info.Cancel()
		delete(tr.transactions, txID)
		debugLog("Completed transaction %s (duration: %v)", txID, time.Since(info.StartTime))
	}
}

// UnregisterWithoutCancel removes a transaction from the registry without canceling the context.
// This is used when the transaction has already been committed or rolled back.
func (tr *TransactionRegistry) UnregisterWithoutCancel(txID string) {
	tr.mu.Lock()
	defer tr.mu.Unlock()

	if info, exists := tr.transactions[txID]; exists {
		delete(tr.transactions, txID)
		debugLog("Completed transaction %s (duration: %v)", txID, time.Since(info.StartTime))
	}
}

// GetTransaction retrieves information about a transaction.
func (tr *TransactionRegistry) GetTransaction(txID string) (*TransactionInfo, bool) {
	tr.mu.RLock()
	defer tr.mu.RUnlock()

	info, exists := tr.transactions[txID]
	return info, exists
}

// ListExpiredTransactions returns a list of transactions that have exceeded their timeout.
func (tr *TransactionRegistry) ListExpiredTransactions() []string {
	tr.mu.RLock()
	defer tr.mu.RUnlock()

	var expired []string
	now := time.Now()

	for txID, info := range tr.transactions {
		// Check if the transaction has timed out
		if now.Sub(info.StartTime) > info.Timeout {
			expired = append(expired, txID)
		}
	}

	return expired
}

// CleanupExpiredTransactions cancels and removes expired transactions.
func (tr *TransactionRegistry) CleanupExpiredTransactions() int {
	expired := tr.ListExpiredTransactions()

	for _, txID := range expired {
		tr.mu.Lock()
		if info, exists := tr.transactions[txID]; exists {
			infoLog("Cleaning up expired transaction %s (duration: %v)", txID, time.Since(info.StartTime))
			info.Cancel()
			delete(tr.transactions, txID)
		}
		tr.mu.Unlock()
	}

	return len(expired)
}

// Count returns the number of active transactions.
func (tr *TransactionRegistry) Count() int {
	tr.mu.RLock()
	defer tr.mu.RUnlock()

	return len(tr.transactions)
}

// SetConnID associates a connection ID with a transaction.
func (tr *TransactionRegistry) SetConnID(txID string, connID string) bool {
	tr.mu.Lock()
	defer tr.mu.Unlock()

	if info, exists := tr.transactions[txID]; exists {
		info.ConnID = connID
		return true
	}

	return false
}

// GetTransactionsByConnID returns all transactions associated with a specific connection.
func (tr *TransactionRegistry) GetTransactionsByConnID(connID string) []string {
	tr.mu.RLock()
	defer tr.mu.RUnlock()

	var txIDs []string
	for txID, info := range tr.transactions {
		if info.ConnID == connID {
			txIDs = append(txIDs, txID)
		}
	}

	return txIDs
}

// CleanupTransactionsByConnID cancels and removes all transactions for a specific connection.
func (tr *TransactionRegistry) CleanupTransactionsByConnID(connID string) int {
	tr.mu.RLock()
	txIDs := tr.GetTransactionsByConnID(connID)
	tr.mu.RUnlock()

	for _, txID := range txIDs {
		tr.Unregister(txID)
	}

	return len(txIDs)
}
