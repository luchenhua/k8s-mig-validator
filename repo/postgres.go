package repo

import (
	"context"
	"database/sql"
	"fmt"
	"reflect"
	"strconv"
	"time"

	"github.com/go-redis/redis/v8"
	"gorm.io/gorm"
)

var limit int = 100

func GetTables(db *gorm.DB) (map[string]int64, error) {
	return getTableInfo(db)
}

func GetMigrationStatus(sourceDB *gorm.DB, targetDB *gorm.DB) (map[string]string, error) {
	sourceTableInfo, err := getTableInfo(sourceDB)
	if err != nil {
		return nil, err
	}

	targetTableInfo, err := getTableInfo(targetDB)
	if err != nil {
		return nil, err
	}

	compareTables := make(map[string]string)
	for k, v := range targetTableInfo {
		compareTables[k] = strconv.FormatInt(sourceTableInfo[k], 10) + " / " + strconv.FormatInt(v, 10)
	}

	return compareTables, nil
}

func StartMigration(sourceDB *gorm.DB, targetDB *gorm.DB, rdb *redis.Client, ctx context.Context) (bool, time.Time, error) {
	// record the start time of the migration task
	startTime := time.Now().UTC()
	err := rdb.Set(ctx, "StartTimeNano", startTime.Nanosecond(), 0).Err()
	if err != nil {
		return false, startTime, err
	}

	// get the name of the tables & the number of the rows in each table
	sourceTableInfo, err := getTableInfo(sourceDB)
	if err != nil {
		return false, startTime, err
	}

	// for each table, start a goroutine to do the migration
	for k, v := range sourceTableInfo {
		err = migrateData(sourceDB, targetDB, rdb, ctx, k, v)
		if err != nil {
			return false, startTime, err
		}
	}

	return true, startTime, nil
}

func getTableInfo(db *gorm.DB) (map[string]int64, error) {
	var tables []string
	err := db.Table("information_schema.tables").Where("table_schema = ?", "public").Pluck("table_name", &tables).Error
	if err != nil {
		return nil, err
	}

	tableInfo := make(map[string]int64)
	var count int64
	for _, table := range tables {
		db.Table(table).Count(&count)
		tableInfo[table] = count
	}

	return tableInfo, nil
}

func migrateData(sourceDB *gorm.DB, targetDB *gorm.DB, rdb *redis.Client, ctx context.Context, tableName string, rowCount int64) error {
	// get the offset from the Redis for the starting point
	// if not exist, store the initial index into the Redis as the starting point
	idx, err := rdb.Get(ctx, tableName).Int()
	if err == redis.Nil {
		fmt.Println("creating starting point for", tableName)

		err = rdb.Set(ctx, tableName, 0, 0).Err()
		if err != nil {
			return err
		}
	} else if err != nil {
		return err
	} else {
	}
	fmt.Println(tableName, "starting from", idx)

	for idx < int(rowCount) {
		// extract the same group of data from both DBs for comparison
		sourceData, targetData, err := prepareDataForComparison(sourceDB, targetDB, tableName, idx, limit)
		if err != nil {
			return err
		}

		// compare the data and update the target DB if necessary
		err = updateTargetDB(targetDB, tableName, sourceData, targetData)
		if err != nil {
			return err
		}

		// update the index and store it into the Redis as the new starting point
		idx += limit
		fmt.Println("new starting point", idx)
		err = rdb.Set(ctx, tableName, idx, 0).Err()
		if err != nil {
			return err
		}
	}

	fmt.Println("all done & reset starting point")
	err = rdb.Set(ctx, tableName, 0, 0).Err()
	if err != nil {
		return err
	}

	return nil
}

func sqlRowsToMap(rows *sql.Rows) []map[string]interface{} {
	cols, _ := rows.Columns()
	length := len(cols)
	cache := make([]interface{}, length)
	for i := range cache {
		var v interface{}
		cache[i] = &v
	}

	var list []map[string]interface{}
	for rows.Next() {
		_ = rows.Scan(cache...)

		item := make(map[string]interface{})
		for i, v := range cache {
			item[cols[i]] = *v.(*interface{})
		}
		list = append(list, item)
	}

	return list
}

func prepareDataForComparison(source *gorm.DB, target *gorm.DB, table string, idx int, limit int) ([]map[string]interface{}, []map[string]interface{}, error) {
	sourceRows, err := source.Table(table).Offset(idx).Limit(limit).Rows()
	defer sourceRows.Close()
	if err != nil {
		return nil, nil, err
	}
	sourceData := sqlRowsToMap(sourceRows)

	targetRows, err := target.Table(table).Offset(idx).Limit(limit).Rows()
	defer targetRows.Close()
	if err != nil {
		return nil, nil, err
	}
	targetData := sqlRowsToMap(targetRows)

	return sourceData, targetData, nil
}

func updateTargetDB(db *gorm.DB, tableName string, source []map[string]interface{}, target []map[string]interface{}) error {
	var bathInsertDatas []map[string]interface{}
	for i, v := range source {
		if i >= len(target) {
			bathInsertDatas = append(bathInsertDatas, v)
			continue
		}
		if !reflect.DeepEqual(v, target[i]) {
			err := db.Table(tableName).Where(target[i]).Updates(v).Error
			if err != nil {
				return err
			}
			continue
		}
	}

	err := db.Table(tableName).CreateInBatches(bathInsertDatas, len(bathInsertDatas)).Error
	if err != nil {
		return err
	}

	return nil
}
