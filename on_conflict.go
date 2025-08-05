package sharding

import (
	"fmt"
	"reflect"
	"regexp"
	"strings"

	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

// fixOnConflictIDQuery modifies queries that have ON CONFLICT on ID but don't include ID in the INSERT
func (s *Sharding) fixOnConflictIDQuery(db *gorm.DB) {
	// Only process CREATE operations with ON CONFLICT
	if db.Statement.Schema == nil || db.Statement.Table == "" {
		return
	}
	
	// Check if there's an ON CONFLICT clause
	var hasOnConflict bool
	var onConflictOnID bool
	
	for _, c := range db.Statement.Clauses {
		if conflict, ok := c.Expression.(clause.OnConflict); ok {
			hasOnConflict = true
			// Check if the conflict is on ID
			for _, col := range conflict.Columns {
				if col.Name == "id" {
					onConflictOnID = true
					break
				}
			}
		}
	}
	
	if !hasOnConflict || !onConflictOnID {
		return
	}
	
	baseTableName := db.Statement.Table
	
	// Check if this table is configured for sharding
	s.mutex.RLock()
	config, exists := s.configs[baseTableName]
	s.mutex.RUnlock()
	
	if !exists {
		return // Not a sharded table
	}
	
	// Only fix if sharding key is "id" and we have a primary key generator
	if config.ShardingKey != "id" || config.PrimaryKeyGeneratorFn == nil {
		return
	}
	
	// Check if the model already has an ID set
	var idValue int64
	var hasID bool
	
	if db.Statement.ReflectValue.Kind() == reflect.Ptr {
		elem := db.Statement.ReflectValue.Elem()
		if elem.Kind() == reflect.Struct {
			idField := elem.FieldByName("ID")
			if idField.IsValid() && idField.CanSet() {
				if idField.Kind() == reflect.Int64 {
					idValue = idField.Int()
					hasID = idValue != 0
				}
			}
		}
	}
	
	// If we have an ID (either already set or generated), we need to ensure it's included in the INSERT
	if hasID {
		// Force GORM to include the ID field in the INSERT
		// This is done by setting the field as "changed" 
		if field := db.Statement.Schema.LookUpField("ID"); field != nil {
			// Mark the field as changed to force its inclusion
			db.Statement.SetColumn("id", idValue)
		}
		
		GetLogger().Debug("Forced ID %d inclusion for ON CONFLICT query on table %s", idValue, baseTableName)
	}
}

// interceptOnConflictQuery is a callback that runs after GORM builds the SQL but before execution
func (s *Sharding) interceptOnConflictQuery(db *gorm.DB) {
	if db.Error != nil {
		return
	}
	
	// Check if this is an INSERT with ON CONFLICT on ID
	sql := db.Statement.SQL.String()
	if !strings.Contains(strings.ToUpper(sql), "INSERT INTO") {
		return
	}
	
	if !strings.Contains(strings.ToUpper(sql), "ON CONFLICT") {
		return
	}
	
	// Check if ON CONFLICT is on ID
	onConflictIDRegex := regexp.MustCompile(`(?i)ON\s+CONFLICT\s*\(\s*"?id"?\s*\)`)
	if !onConflictIDRegex.MatchString(sql) {
		return
	}
	
	// Check if ID is already in the column list
	insertRegex := regexp.MustCompile(`(?i)INSERT\s+INTO\s+"?([^"]+)"?\s*\(([^)]+)\)\s+VALUES`)
	matches := insertRegex.FindStringSubmatch(sql)
	if len(matches) < 3 {
		return
	}
	
	tableName := matches[1]
	columnsStr := matches[2]
	
	// Check if ID is already in columns
	if strings.Contains(strings.ToLower(columnsStr), "id") {
		return // ID is already included
	}
	
	// Check if this table is configured for sharding
	s.mutex.RLock()
	config, exists := s.configs[tableName]
	s.mutex.RUnlock()
	
	if !exists || config.ShardingKey != "id" || config.PrimaryKeyGeneratorFn == nil {
		return
	}
	
	// Get the ID value from the model
	var idValue int64
	if db.Statement.ReflectValue.Kind() == reflect.Ptr {
		elem := db.Statement.ReflectValue.Elem()
		if elem.Kind() == reflect.Struct {
			idField := elem.FieldByName("ID")
			if idField.IsValid() && idField.Kind() == reflect.Int64 {
				idValue = idField.Int()
			}
		}
	}
	
	if idValue == 0 {
		return // No ID to insert
	}
	
	// Modify the SQL to include ID
	// Add "id" to the column list
	newColumns := `"id", ` + columnsStr
	
	// Count the number of placeholders in VALUES to add one more
	valuesRegex := regexp.MustCompile(`VALUES\s*\(([^)]+)\)`)
	valuesMatch := valuesRegex.FindStringSubmatch(sql)
	if len(valuesMatch) < 2 {
		return
	}
	
	// Count existing placeholders
	placeholders := strings.Split(valuesMatch[1], ",")
	nextPlaceholder := fmt.Sprintf("$%d", len(placeholders)+1)
	
	// Add the new placeholder
	newValues := nextPlaceholder + ", " + valuesMatch[1]
	
	// Build the new SQL
	newSQL := strings.Replace(sql, "("+columnsStr+")", "("+newColumns+")", 1)
	newSQL = strings.Replace(newSQL, "VALUES ("+valuesMatch[1]+")", "VALUES ("+newValues+")", 1)
	
	// Update the SQL
	db.Statement.SQL.Reset()
	db.Statement.SQL.WriteString(newSQL)
	
	// Add the ID value to vars
	db.Statement.Vars = append([]interface{}{idValue}, db.Statement.Vars...)
	
	GetLogger().Debug("Modified ON CONFLICT query to include ID %d: %s", idValue, newSQL)
}

// Register additional callbacks for ON CONFLICT handling
func (s *Sharding) registerOnConflictCallbacks(db *gorm.DB) {
	// This callback runs before create to mark ID field as changed
	db.Callback().Create().Before("gorm:before_create").Register("sharding:fix_on_conflict_id", func(db *gorm.DB) {
		if db.Error == nil {
			s.fixOnConflictIDQuery(db)
		}
	})
}