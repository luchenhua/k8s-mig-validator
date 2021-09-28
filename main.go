package main

import (
	"context"
	"k8s-mig-validator/repo"
	"net/http"
	"strings"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/go-redis/redis/v8"
	"github.com/spf13/viper"
	"gorm.io/driver/postgres"
	"gorm.io/gorm"
)

var dbSource *gorm.DB
var dbTarget *gorm.DB
var rdb *redis.Client
var ctx = context.Background()

func main() {
	viper.AutomaticEnv()

	prepareDBConnection()

	prepareRedisConnection()

	router := gin.Default()
	baseURL := "/validator"

	base := router.Group(baseURL)
	{
		base.GET("/ping", func(c *gin.Context) {
			c.SecureJSON(http.StatusOK, "pong")
			return
		})
	}

	source := router.Group(baseURL + "/source")
	{
		ss, _ := dbSource.DB()

		source.GET("/config", func(c *gin.Context) {
			c.SecureJSON(http.StatusOK, dbSource.Config)
			return
		})
		source.GET("/dialector", func(c *gin.Context) {
			c.SecureJSON(http.StatusOK, dbSource.Dialector)
			return
		})
		source.GET("/status", func(c *gin.Context) {
			c.SecureJSON(http.StatusOK, ss.Stats())
			return
		})
		source.GET("/tables", func(c *gin.Context) {
			result, err := repo.GetTables(dbSource)
			if err != nil {
				c.SecureJSON(http.StatusBadRequest, err.Error())
				return
			}

			c.SecureJSON(http.StatusOK, result)
			return
		})
	}

	target := router.Group(baseURL + "/target")
	{
		ts, _ := dbTarget.DB()

		target.GET("/config", func(c *gin.Context) {
			c.SecureJSON(http.StatusOK, dbTarget.Config)
			return
		})
		target.GET("/dialector", func(c *gin.Context) {
			c.SecureJSON(http.StatusOK, dbTarget.Dialector)
			return
		})
		target.GET("/status", func(c *gin.Context) {
			c.SecureJSON(http.StatusOK, ts.Stats())
			return
		})
		target.GET("/tables", func(c *gin.Context) {
			result, err := repo.GetTables(dbTarget)
			if err != nil {
				c.SecureJSON(http.StatusBadRequest, err.Error())
				return
			}

			c.SecureJSON(http.StatusOK, result)
			return
		})
	}

	migration := router.Group(baseURL + "/migration")
	{
		migration.GET("/status", func(c *gin.Context) {
			result, err := repo.GetMigrationStatus(dbSource, dbTarget)
			if err != nil {
				c.SecureJSON(http.StatusBadRequest, err.Error())
				return
			}

			c.SecureJSON(http.StatusOK, result)
			return
		})
		migration.POST("/init", func(c *gin.Context) {
			isStarted, startTime, err := repo.DataCopy(dbSource, dbTarget, rdb, ctx)

			if err != nil || !isStarted {
				c.SecureJSON(http.StatusServiceUnavailable, err.Error())
				return
			}

			c.SecureJSON(http.StatusCreated, "DataCopy started at "+startTime.String())
			return
		})
		migration.POST("/sync", func(c *gin.Context) {
			isStarted, startTime, err := repo.DataSync(dbSource, dbTarget, rdb, ctx)

			if err != nil || !isStarted {
				c.SecureJSON(http.StatusServiceUnavailable, err.Error())
				return
			}

			c.SecureJSON(http.StatusCreated, "DataSync started at "+startTime.String())
			return
		})
	}

	router.Run(":3000")
}

func prepareDBConnection() {
	var builder strings.Builder

	builder.WriteString("host=")
	builder.WriteString(viper.GetString("DB_SOURCE_HOST"))
	builder.WriteString(" user=")
	builder.WriteString(viper.GetString("DB_SOURCE_USER"))
	builder.WriteString(" password=")
	builder.WriteString(viper.GetString("DB_SOURCE_PASSWORD"))
	builder.WriteString(" dbname=")
	builder.WriteString(viper.GetString("DB_SOURCE_NAME"))
	builder.WriteString(" port=")
	builder.WriteString(viper.GetString("DB_SOURCE_PORT"))
	builder.WriteString(" sslmode=disable")

	dbSource, _ = gorm.Open(postgres.Open(builder.String()), &gorm.Config{})
	sourceDB, _ := dbSource.DB()
	sourceDB.SetMaxIdleConns(10)
	sourceDB.SetMaxOpenConns(100)
	sourceDB.SetConnMaxLifetime(time.Hour)

	builder.Reset()

	builder.WriteString("host=")
	builder.WriteString(viper.GetString("DB_TARGET_HOST"))
	builder.WriteString(" user=")
	builder.WriteString(viper.GetString("DB_TARGET_USER"))
	builder.WriteString(" password=")
	builder.WriteString(viper.GetString("DB_TARGET_PASSWORD"))
	builder.WriteString(" dbname=")
	builder.WriteString(viper.GetString("DB_TARGET_NAME"))
	builder.WriteString(" port=")
	builder.WriteString(viper.GetString("DB_TARGET_PORT"))
	builder.WriteString(" sslmode=disable")

	dbTarget, _ = gorm.Open(postgres.Open(builder.String()), &gorm.Config{})
	targetDB, _ := dbTarget.DB()
	targetDB.SetMaxIdleConns(10)
	targetDB.SetMaxOpenConns(100)
	targetDB.SetConnMaxLifetime(time.Hour)
}

func prepareRedisConnection() {
	rdb = redis.NewClient(&redis.Options{
		Addr:     viper.GetString("REDIS_HOST") + ":" + viper.GetString("REDIS_PORT"),
		Password: viper.GetString("REDIS_PASSWORD"),
		DB:       viper.GetInt("REDIS_DB"),
	})
}
