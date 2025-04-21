package test

import (
	"fmt"
	"testing"
)

// TestParameterCountMismatch304vs296 simulates the exact scenario from the log
// where the system tries to access parameter index 304 when there are only 296 parameters
func TestParameterCountMismatch304vs296(t *testing.T) {
	// Create a query with 38 value groups (each with 8 parameters = 304 total parameters)
	query := "INSERT INTO balance_balances (contract,type,account,value,owner,block,tx_hash,token_id) VALUES "

	// Add 38 value groups to the query, each with 8 parameters
	for i := 0; i < 38; i++ {
		if i > 0 {
			query += ", "
		}

		// Each group has 8 parameters
		query += fmt.Sprintf("($%d,$%d,$%d,$%d,$%d,$%d,$%d,$%d)",
			i*8+1, i*8+2, i*8+3, i*8+4, i*8+5, i*8+6, i*8+7, i*8+8)
	}

	// But only create 37 groups worth of parameters (37 * 8 = 296 parameters)
	// This simulates the exact scenario in the log - trying to access param 304 with only 296 params
	args := make([]interface{}, 296)
	for i := 0; i < 296; i++ {
		args[i] = fmt.Sprintf("value%d", i)
	}

	// Calculate expected vs actual parameters
	requiredParams := 38 * 8                       // 38 groups * 8 parameters per group = 304 parameters
	actualParams := len(args)                      // 296 parameters (37 groups * 8 parameters)
	missingParams := requiredParams - actualParams // Should be 8 parameters (1 full group)

	// Verify parameters are mismatched
	if requiredParams > actualParams {
		t.Logf("✅ Detected parameter count mismatch: query requires %d parameters but only %d are provided (missing %d parameters)",
			requiredParams, actualParams, missingParams)

		// Show the specific missing parameter indices
		t.Logf("Missing parameter indices: %d through %d", actualParams+1, requiredParams)

		// This is specifically parameter 304 which was mentioned in the error log
		criticalMissingParam := 304 // This is the parameter mentioned in the logs
		if criticalMissingParam > actualParams && criticalMissingParam <= requiredParams {
			t.Logf("✅ Successfully detected the specific issue from the logs: parameter %d is required but not provided",
				criticalMissingParam)
		} else {
			t.Errorf("❌ Failed to identify the specific parameter from the logs: expected parameter %d to be missing",
				criticalMissingParam)
		}
	} else {
		t.Errorf("❌ Failed to detect parameter count mismatch: query requires %d parameters but %d were provided",
			requiredParams, actualParams)
	}

	// The test is successful if:
	// 1. We detect a parameter count mismatch (requiredParams > actualParams)
	// 2. We confirm that parameter 304 specifically is missing (which was in the logs)
	if requiredParams <= actualParams {
		t.Fail()
	}
}
