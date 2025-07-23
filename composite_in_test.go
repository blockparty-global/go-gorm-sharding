package sharding

import (
	"gorm.io/gorm/logger"
	"testing"

	"github.com/stretchr/testify/assert"
	"gorm.io/driver/postgres"
	"gorm.io/gorm"
)

func TestCompositeINClauseWithMultipleTuples(t *testing.T) {
	dbConfig := postgres.Config{
		DSN:                  dbURL(),
		PreferSimpleProtocol: true,
	}
	db, err := gorm.Open(postgres.New(dbConfig), &gorm.Config{
		DisableForeignKeyConstraintWhenMigrating: true,
		Logger:                                   logger.Default.LogMode(logger.Info),
	})
	assert.NoError(t, err)

	middleware := Register(Config{
		ShardingKey:         "contract",
		NumberOfShards:      4,
		PrimaryKeyGenerator: PKSnowflake,
	}, "tokens")

	db.Use(middleware)

	// Create test table
	type Token struct {
		ID       int64  `gorm:"primarykey"`
		Contract string `gorm:"index:idx_contract_token,composite"`
		TokenID  string `gorm:"column:token_id;index:idx_contract_token,composite"`
		Owner    string
	}

	// Migrate the table
	err = db.AutoMigrate(&Token{})
	assert.NoError(t, err)

	// Insert test data with different contracts and token IDs
	testTokens := []Token{
		{Contract: "0x76be3b62873462d2142405439777e971754e8e77", TokenID: "10699", Owner: "owner1"},
		{Contract: "0x76be3b62873462d2142405439777e971754e8e77", TokenID: "10502", Owner: "owner2"},
		{Contract: "0x76be3b62873462d2142405439777e971754e8e77", TokenID: "10719", Owner: "owner3"},
		{Contract: "0x76be3b62873462d2142405439777e971754e8e77", TokenID: "10712", Owner: "owner4"},
		// Add some tokens with same contract but different token_id that should NOT be returned
		{Contract: "0x76be3b62873462d2142405439777e971754e8e77", TokenID: "99999", Owner: "owner5"},
		{Contract: "0x76be3b62873462d2142405439777e971754e8e77", TokenID: "88888", Owner: "owner6"},
		// Add some tokens with different contract but same token_id that should NOT be returned
		{Contract: "0xdifferentcontract", TokenID: "10699", Owner: "owner7"},
		{Contract: "0xanothercontract", TokenID: "10502", Owner: "owner8"},
	}

	for _, token := range testTokens {
		err := db.Create(&token).Error
		assert.NoError(t, err)
	}

	t.Run("CompositeIN_ChecksBothParameters", func(t *testing.T) {
		var results []Token

		// Test the exact query format
		err := db.Raw(`SELECT * FROM "tokens" WHERE ((contract, token_id)) IN (('0x76be3b62873462d2142405439777e971754e8e77', '10699'),('0x76be3b62873462d2142405439777e971754e8e77', '10502'),('0x76be3b62873462d2142405439777e971754e8e77', '10719'),('0x76be3b62873462d2142405439777e971754e8e77', '10712'))`).
			Find(&results).Error

		assert.NoError(t, err)
		assert.Equal(t, 4, len(results), "Should return exactly 4 tokens matching both contract AND token_id")

		// Verify the correct tokens were returned
		expectedTokenIDs := map[string]bool{
			"10699": true,
			"10502": true,
			"10719": true,
			"10712": true,
		}

		for _, result := range results {
			assert.Equal(t, "0x76be3b62873462d2142405439777e971754e8e77", result.Contract)
			assert.True(t, expectedTokenIDs[result.TokenID], "TokenID %s should be in expected list", result.TokenID)
			delete(expectedTokenIDs, result.TokenID)
		}

		assert.Empty(t, expectedTokenIDs, "All expected token IDs should have been found")
	})

	t.Run("CompositeIN_WithParameterizedValues", func(t *testing.T) {
		var results []Token

		// Test with parameterized values to ensure both parameters are checked
		tuples := [][]interface{}{
			{"0x76be3b62873462d2142405439777e971754e8e77", "10699"},
			{"0x76be3b62873462d2142405439777e971754e8e77", "10502"},
			{"0x76be3b62873462d2142405439777e971754e8e77", "10719"},
			{"0x76be3b62873462d2142405439777e971754e8e77", "10712"},
		}

		err := db.Raw(`SELECT * FROM tokens WHERE ((contract, token_id)) IN (?)`, tuples).
			Find(&results).Error

		assert.NoError(t, err)
		assert.Equal(t, 4, len(results), "Should return exactly 4 tokens with parameterized query")
	})

	t.Run("CompositeIN_GORM_Style", func(t *testing.T) {
		var results []Token

		// Test using GORM's Where clause
		tuples := [][]interface{}{
			{"0x76be3b62873462d2142405439777e971754e8e77", "10699"},
			{"0x76be3b62873462d2142405439777e971754e8e77", "10502"},
			{"0x76be3b62873462d2142405439777e971754e8e77", "10719"},
			{"0x76be3b62873462d2142405439777e971754e8e77", "10712"},
		}

		err := db.Where("((contract, token_id)) IN ?", tuples).Find(&results).Error

		assert.NoError(t, err)
		assert.Equal(t, 4, len(results), "Should return exactly 4 tokens with GORM Where clause")
	})

	t.Run("CompositeIN_VerifyNotJustFirstParameter", func(t *testing.T) {
		var results []Token

		// Query that would return wrong results if only checking first parameter
		err := db.Raw(`SELECT * FROM "tokens" WHERE ((contract, token_id)) IN (('0xdifferentcontract', '10699'),('0xanothercontract', '10502'))`).
			Find(&results).Error

		assert.NoError(t, err)
		assert.Equal(t, 2, len(results), "Should return tokens with different contracts")

		// Verify none of them have the original contract
		for _, result := range results {
			assert.NotEqual(t, "0x76be3b62873462d2142405439777e971754e8e77", result.Contract)
		}
	})

	// Cleanup
	db.Exec("DROP TABLE IF EXISTS tokens_0, tokens_1, tokens_2, tokens_3")
}
