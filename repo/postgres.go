package repo

import (
	"strconv"

	"gorm.io/gorm"
)

func GetTables(db *gorm.DB) map[string]int64 {
	var tables []string
	if err := db.Table("information_schema.tables").Where("table_schema = ?", "public").Pluck("table_name", &tables).Error; err != nil {
		panic(err)
	}

	tableInfo := make(map[string]int64)
	var count int64
	for _, table := range tables {
		db.Table(table).Count(&count)
		tableInfo[table] = count
	}

	return tableInfo
}

func GetMigrationStatus(sourceDB *gorm.DB, targetDB *gorm.DB) map[string]string {
	var sourceTables []string
	if err := sourceDB.Table("information_schema.tables").Where("table_schema = ?", "public").Pluck("table_name", &sourceTables).Error; err != nil {
		panic(err)
	}

	sourceTableInfo := make(map[string]int64)
	var count int64
	for _, table := range sourceTables {
		sourceDB.Table(table).Count(&count)
		sourceTableInfo[table] = count
	}

	var targetTables []string
	if err := targetDB.Table("information_schema.tables").Where("table_schema = ?", "public").Pluck("table_name", &targetTables).Error; err != nil {
		panic(err)
	}

	targetTableInfo := make(map[string]int64)
	for _, table := range targetTables {
		targetDB.Table(table).Count(&count)
		targetTableInfo[table] = count
	}

	compareTables := make(map[string]string, len(targetTables))
	for _, k := range targetTables {
		compareTables[k] = strconv.FormatInt(sourceTableInfo[k], 10) + " / " + strconv.FormatInt(targetTableInfo[k], 10)
	}

	return compareTables
}
