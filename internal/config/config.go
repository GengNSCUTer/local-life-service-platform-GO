package config

import (
	"net"
	"os"
	"strconv"
	"strings"
	"time"
)

type Config struct {
	HTTPAddr               string
	ObservabilityAddr      string
	PprofEnabled           bool
	InventoryAuditInterval time.Duration
	SeckillOutboxLimit     int
	AuthDevLogCodes        bool
	ShopAdminUserID        uint64
	VoucherAdminUserID     uint64
	MySQLAddr              string
	MySQLUser              string
	MySQLPassword          string
	MySQLDatabase          string
	RedisAddr              string
	RedisPassword          string
	RedisSentinels         []string
	RedisMasterName        string
	KafkaBrokers           []string
	KafkaRelayWorkers      int
	KafkaConsumerWorkers   int
	SeckillPendingTimeout  time.Duration
}

func Load() Config {
	var brokers []string
	for _, broker := range strings.Split(env("KAFKA_BROKERS", localAddr("KAFKA_PORT", "29092")), ",") {
		if broker := strings.TrimSpace(broker); broker != "" {
			brokers = append(brokers, broker)
		}
	}
	var sentinels []string
	for _, sentinel := range strings.Split(env("REDIS_SENTINEL_ADDRS", ""), ",") {
		if sentinel := strings.TrimSpace(sentinel); sentinel != "" {
			sentinels = append(sentinels, sentinel)
		}
	}

	pprofEnabled, _ := strconv.ParseBool(env("PPROF_ENABLED", "false"))
	authDevLogCodes, _ := strconv.ParseBool(env("AUTH_DEV_LOG_CODES", "false"))
	shopAdminUserID, err := strconv.ParseUint(env("SHOP_ADMIN_USER_ID", "0"), 10, 64)
	voucherAdminUserID, voucherErr := strconv.ParseUint(env("VOUCHER_ADMIN_USER_ID", "0"), 10, 64)
	if err != nil {
		shopAdminUserID = 0
	}
	if voucherErr != nil {
		voucherAdminUserID = 0
	}
	return Config{
		HTTPAddr:               env("HTTP_ADDR", "127.0.0.1:8080"),
		ObservabilityAddr:      env("OBSERVABILITY_ADDR", ""),
		PprofEnabled:           pprofEnabled,
		InventoryAuditInterval: durationEnv("SECKILL_AUDIT_INTERVAL", time.Minute),
		SeckillOutboxLimit:     positiveIntEnv("SECKILL_OUTBOX_LIMIT", 0),
		AuthDevLogCodes:        authDevLogCodes,
		ShopAdminUserID:        shopAdminUserID,
		VoucherAdminUserID:     voucherAdminUserID,
		MySQLAddr:              env("MYSQL_ADDR", localAddr("MYSQL_PORT", "23306")),
		MySQLUser:              env("MYSQL_USER", "hmdp"),
		MySQLPassword:          env("MYSQL_PASSWORD", "hmdp_dev_mysql"),
		MySQLDatabase:          env("MYSQL_DATABASE", "hmdp"),
		RedisAddr:              env("REDIS_ADDR", localAddr("REDIS_PORT", "26379")),
		RedisPassword:          env("REDIS_PASSWORD", "hmdp_dev_redis"),
		RedisSentinels:         sentinels,
		RedisMasterName:        env("REDIS_MASTER_NAME", "hmdp-master"),
		KafkaBrokers:           brokers,
		KafkaRelayWorkers:      positiveIntEnv("KAFKA_RELAY_WORKERS", 4),
		KafkaConsumerWorkers:   positiveIntEnv("KAFKA_CONSUMER_WORKERS", 3),
		SeckillPendingTimeout:  durationEnv("SECKILL_PENDING_TIMEOUT", 15*time.Minute),
	}
}

func durationEnv(key string, fallback time.Duration) time.Duration {
	value, err := time.ParseDuration(env(key, fallback.String()))
	if err != nil || value < time.Second {
		return fallback
	}
	return value
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

func positiveIntEnv(key string, fallback int) int {
	value, err := strconv.Atoi(env(key, strconv.Itoa(fallback)))
	if err != nil || value < 1 {
		return fallback
	}
	return value
}
