package config

import (
	"net"
	"os"
	"strings"
)

type Config struct {
	HTTPAddr      string
	MySQLAddr     string
	MySQLUser     string
	MySQLPassword string
	MySQLDatabase string
	RedisAddr     string
	RedisPassword string
	KafkaBrokers  []string
}

func Load() Config {
	var brokers []string
	for _, broker := range strings.Split(env("KAFKA_BROKERS", localAddr("KAFKA_PORT", "29092")), ",") {
		if broker := strings.TrimSpace(broker); broker != "" {
			brokers = append(brokers, broker)
		}
	}

	return Config{
		HTTPAddr:      env("HTTP_ADDR", "127.0.0.1:8080"),
		MySQLAddr:     env("MYSQL_ADDR", localAddr("MYSQL_PORT", "23306")),
		MySQLUser:     env("MYSQL_USER", "hmdp"),
		MySQLPassword: env("MYSQL_PASSWORD", "hmdp_dev_mysql"),
		MySQLDatabase: env("MYSQL_DATABASE", "hmdp"),
		RedisAddr:     env("REDIS_ADDR", localAddr("REDIS_PORT", "26379")),
		RedisPassword: env("REDIS_PASSWORD", "hmdp_dev_redis"),
		KafkaBrokers:  brokers,
	}
}

func localAddr(portKey, fallback string) string {
	return net.JoinHostPort("127.0.0.1", env(portKey, fallback))
}

func env(key, fallback string) string {
	if value, ok := os.LookupEnv(key); ok {
		return value
	}
	return fallback
}
