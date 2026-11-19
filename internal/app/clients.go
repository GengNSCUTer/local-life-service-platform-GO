package app

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"

	"github.com/GengNSCUTer/local-life-service-platform-GO/internal/config"
	mysqldriver "github.com/go-sql-driver/mysql"
	"github.com/redis/go-redis/v9"
	"github.com/segmentio/kafka-go"
	"gorm.io/driver/mysql"
	"gorm.io/gorm"
)

type clients struct {
	mysql          *gorm.DB
	redis          *redis.Client
	kafka          *kafka.Client
	kafkaTransport *kafka.Transport
}

func openClients(cfg config.Config) (*clients, error) {
	if len(cfg.KafkaBrokers) == 0 {
		return nil, errors.New("KAFKA_BROKERS must contain at least one broker address")
	}

	mysqlConfig := mysqldriver.NewConfig()
	mysqlConfig.Net = "tcp"
	mysqlConfig.Addr = cfg.MySQLAddr
	mysqlConfig.User = cfg.MySQLUser
	mysqlConfig.Passwd = cfg.MySQLPassword
	mysqlConfig.DBName = cfg.MySQLDatabase
	mysqlConfig.ParseTime = true
	mysqlConfig.Loc = time.UTC
	mysqlConfig.Timeout = 3 * time.Second
	connector, err := mysqldriver.NewConnector(mysqlConfig)
	if err != nil {
		return nil, fmt.Errorf("configure MySQL: %w", err)
	}
	pool := sql.OpenDB(connector)
	pool.SetMaxOpenConns(20)
	pool.SetMaxIdleConns(10)
	pool.SetConnMaxLifetime(5 * time.Minute)

	// Readiness probes verify connectivity after the HTTP server starts.
	db, err := gorm.Open(mysql.New(mysql.Config{
		Conn:                      pool,
		SkipInitializeWithVersion: true,
	}), &gorm.Config{DisableAutomaticPing: true})
	if err != nil {
		return nil, errors.Join(fmt.Errorf("initialize GORM: %w", err), pool.Close())
	}

	transport := &kafka.Transport{DialTimeout: 2 * time.Second}
	return &clients{
		mysql: db,
		redis: redis.NewClient(&redis.Options{
			Addr:                  cfg.RedisAddr,
			Password:              cfg.RedisPassword,
			ContextTimeoutEnabled: true,
		}),
		kafka:          &kafka.Client{Addr: kafka.TCP(cfg.KafkaBrokers...), Transport: transport},
		kafkaTransport: transport,
	}, nil
}

func (c *clients) close() error {
	c.kafkaTransport.CloseIdleConnections()
	pool, err := c.mysql.DB()
	if err == nil {
		err = pool.Close()
	}
	return errors.Join(err, c.redis.Close())
}

func (c *clients) probes() []healthProbe {
	return []healthProbe{
		{name: "mysql", check: func(ctx context.Context) error {
			pool, err := c.mysql.DB()
			if err != nil {
				return err
			}
			return pool.PingContext(ctx)
		}},
		{name: "redis", check: func(ctx context.Context) error {
			return c.redis.Ping(ctx).Err()
		}},
		{name: "kafka", check: func(ctx context.Context) error {
			metadata, err := c.kafka.Metadata(ctx, &kafka.MetadataRequest{Topics: []string{}})
			if err != nil {
				return err
			}
			if len(metadata.Brokers) == 0 {
				return errors.New("Kafka returned no brokers")
			}
			return nil
		}},
	}
}
