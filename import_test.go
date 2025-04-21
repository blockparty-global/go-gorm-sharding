package sharding

import (
	"testing"

	"github.com/longbridge/assert"
	"gorm.io/hints"
	"gorm.io/plugin/dbresolver"
)

func TestImportsWorking(t *testing.T) {
	// Just a simple test to verify that the imports are working
	// No need to actually run anything here
	_ = assert.True
	_ = hints.UseIndex
	_ = dbresolver.Register
}
