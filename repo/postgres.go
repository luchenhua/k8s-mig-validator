package repo

import (
	"context"
	"database/sql"
	"fmt"
	"reflect"
	"strconv"
	"time"

	"github.com/go-redis/redis/v8"
	"golang.org/x/sync/errgroup"
	"gorm.io/gorm"
)

var batchSizeCreate int = 1000
var batchSizeUpdate int = 100
var batchSizeSearch int = 10000

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

func DataCopy(sourceDB *gorm.DB, targetDB *gorm.DB, rdb *redis.Client, ctx context.Context) (bool, time.Time, error) {
	// record the start time of the migration task
	startTime := time.Now().UTC()
	_, err := rdb.GetSet(ctx, "DataCopyTimeNano", startTime.Nanosecond()).Result()
	if err != redis.Nil && err != nil {
		return false, startTime, err
	}

	// get the name of the tables & the number of the rows in each table
	sourceTableInfo, err := getTableInfo(sourceDB)
	if err != nil {
		return false, startTime, err
	}

	// for each table, start a goroutine to do the migration
	grp := new(errgroup.Group)
	for k, v := range sourceTableInfo {
		tableName := k
		totalRows := v
		grp.Go(func() error {
			err := copyData(sourceDB, targetDB, rdb, ctx, tableName, totalRows)
			return err
		})
	}
	if err := grp.Wait(); err != nil {
		fmt.Println(err.Error())
		return false, startTime, err
	}

	rdb.GetSet(ctx, "DataCopyTimeNano", nil)

	return true, startTime, nil
}

func DataSync(sourceDB *gorm.DB, targetDB *gorm.DB, rdb *redis.Client, ctx context.Context) (bool, time.Time, error) {
	// record the start time of the migration task
	startTime := time.Now().UTC()
	_, err := rdb.GetSet(ctx, "StartTimeNano", startTime.Nanosecond()).Result()
	if err != redis.Nil && err != nil {
		return false, startTime, err
	}

	// get the name of the tables & the number of the rows in each table
	sourceTableInfo, err := getTableInfo(sourceDB)
	if err != nil {
		return false, startTime, err
	}

	// for each table, start a goroutine to do the migration
	grp := new(errgroup.Group)
	for k, v := range sourceTableInfo {
		tableName := k
		totalRows := v
		grp.Go(func() error {
			err := syncData(sourceDB, targetDB, rdb, ctx, tableName, totalRows)
			return err
		})
	}
	if err := grp.Wait(); err != nil {
		fmt.Println(err.Error())
		return false, startTime, err
	}

	rdb.GetSet(ctx, "StartTimeNano", nil)

	return true, startTime, nil
}

func copyData(source *gorm.DB, target *gorm.DB, rdb *redis.Client, ctx context.Context, table string, total int64) error {
	offset, err := rdb.Get(ctx, table).Int()
	if err == redis.Nil {
		err = rdb.Set(ctx, table, 0, 0).Err()
		if err != nil {
			return err
		}
		fmt.Println("data init starting point for", table)
	} else if err != nil {
		return err
	} else {
	}

	for offset < int(total) {
		fmt.Println(table, "starting from", offset)
		data := make([]map[string]interface{}, 0)
		// get data from the source database
		err := source.Table(table).
			Where("id >= (?)", source.Table(table).Select("id").Order("id asc").Limit(1).Offset(offset)).
			Order("id asc").
			Limit(batchSizeCreate).
			Find(&data).Error
		if err != nil {
			return err
		}

		// write data into the target database
		err = target.Table(table).Create(data).Error
		if err != nil {
			return err
		}

		// update the index and store it into the Redis as the new starting point
		offset += batchSizeCreate
		err = rdb.Set(ctx, table, offset, 0).Err()
		if err != nil {
			return err
		}
	}

	err = rdb.Set(ctx, table, 0, 0).Err()
	if err != nil {
		return err
	}
	fmt.Println("all done & reset starting point")

	return nil
}

func syncData(source *gorm.DB, target *gorm.DB, rdb *redis.Client, ctx context.Context, table string, total int64) error {
	// get the id list for all the rows with difference
	idList, err := getIdListForDiffData(source, target, rdb, ctx, table, total)
	if err != nil {
		return err
	}

	return updateDiffData(source, target, idList, table)
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

func getIdListForDiffData(source *gorm.DB, target *gorm.DB, rdb *redis.Client, ctx context.Context, table string, total int64) ([]int, error) {
	offset, err := rdb.Get(ctx, table).Int()
	if err == redis.Nil {
		err = rdb.Set(ctx, table, 0, 0).Err()
		if err != nil {
			return nil, err
		}
		fmt.Println("data sync starting point for", table)
	} else if err != nil {
		return nil, err
	} else {
	}

	idList := make([]int, 0)
	for offset < int(total) {
		fmt.Println(table, "starting from", offset)

		sourceList := make([]map[string]interface{}, 0)
		err := source.Table(table).
			Select("id", "md5(textin(record_out("+table+")))").
			Where("id >= (?)", source.Table(table).Select("id").Order("id asc").Limit(1).Offset(offset)).
			Order("id asc").
			Limit(batchSizeSearch).
			Find(&sourceList).Error
		if err != nil {
			return nil, err
		}

		targetList := make([]map[string]interface{}, 0)
		err = target.Table(table).
			Select("id", "md5(textin(record_out("+table+")))").
			Where("id >= (?)", target.Table(table).Select("id").Order("id asc").Limit(1).Offset(offset)).
			Order("id asc").
			Limit(batchSizeSearch).
			Find(&targetList).Error
		if err != nil {
			return nil, err
		}

		// compare the md5 and store the id for the row with difference
		for k, v := range sourceList {
			if !reflect.DeepEqual(v, targetList[k]) {
				idList = append(idList, int(v["id"].(int32)))
			}
		}

		// update the index and store it into the Redis as the new starting point
		offset += batchSizeSearch
		err = rdb.Set(ctx, table, offset, 0).Err()
		if err != nil {
			return nil, err
		}
	}
	fmt.Println(table, ":", len(idList), "rows need to be update")

	err = rdb.Set(ctx, table, 0, 0).Err()
	if err != nil {
		return nil, err
	}
	fmt.Println("all done & reset starting point")

	return idList, nil
}

func updateDiffData(source *gorm.DB, target *gorm.DB, ids []int, table string) error {
	for len(ids) > 0 {
		// get data from the source db
		dataList := make([]map[string]interface{}, 0)
		if batchSizeUpdate > len(ids) {
			err := source.Table(table).Where("id IN ?", ids).Find(&dataList).Error
			if err != nil {
				return err
			}
		} else {
			err := source.Table(table).Where("id IN ?", ids[:batchSizeUpdate]).Find(&dataList).Error
			if err != nil {
				return err
			}
		}

		// loop through and update the target db row by row
		grp := new(errgroup.Group)
		for _, v := range dataList {
			data := v
			grp.Go(func() error {
				err := target.Table(table).Where("id = ?", data["id"]).Updates(&data).Error
				return err
			})
		}
		if err := grp.Wait(); err != nil {
			return err
		}

		ids = ids[batchSizeUpdate:]
	}

	return nil
}
