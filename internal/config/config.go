package config

import (
	"math"
	"net"
	"os"
	"strconv"
	"strings"
	"time"
)

type Config struct {
	HTTPAddr               string
	HTTPIdleTimeout        time.Duration
	WorkerHealthAddr       string
	ObservabilityAddr      string
	PprofEnabled           bool
	InventoryAuditInterval time.Duration
	SeckillOutboxLimit     int
	SeckillPendingLimit    int
	SeckillRedisSharded    bool
	SeckillRedisCluster    []string
	SeckillStockBuckets    int
	AuthDevLogCodes        bool
	ShopAdminUserID        uint64
	VoucherAdminUserID     uint64
	MediaEndpoint          string
	MediaAccessKey         string
	MediaSecretKey         string
	MediaBucket            string
	MediaPublicURL         string
	SocialPullAuthorIDs    string
	TraceEndpoint          string
	TraceSampleRatio       float64
	AuthCodeIPLimit        int
	SeckillUserLimit       int
	ShopBreakerEnabled     bool
	MySQLAddr              string
	MySQLUser              string
	MySQLPassword          string
	MySQLDatabase          string
	MySQLMaxOpenConns      int
	RedisAddr              string
	RedisPassword          string
	RedisSentinels         []string
	RedisSentinelPassword  string
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
	seckillSharded, _ := strconv.ParseBool(env("SECKILL_REDIS_SHARDED", "false"))
	var seckillCluster []string
	for _, addr := range strings.Split(env("SECKILL_REDIS_CLUSTER_ADDRS", ""), ",") {
		if addr := strings.TrimSpace(addr); addr != "" {
			seckillCluster = append(seckillCluster, addr)
		}
	}
	shopAdminUserID, err := strconv.ParseUint(env("SHOP_ADMIN_USER_ID", "0"), 10, 64)
	voucherAdminUserID, voucherErr := strconv.ParseUint(env("VOUCHER_ADMIN_USER_ID", "0"), 10, 64)
	if err != nil {
		shopAdminUserID = 0
	}
	if voucherErr != nil {
		voucherAdminUserID = 0
	}
	traceRatio, err := strconv.ParseFloat(env("TRACE_SAMPLE_RATIO", "0.01"), 64)
	if err != nil || math.IsNaN(traceRatio) || traceRatio < 0 || traceRatio > 1 {
		traceRatio = 0.01
	}
	shopBreaker, _ := strconv.ParseBool(env("SHOP_MYSQL_BREAKER", "true"))
	return Config{
		HTTPAddr:               env("HTTP_ADDR", "127.0.0.1:8080"),
		HTTPIdleTimeout:        durationEnv("HTTP_IDLE_TIMEOUT", time.Minute),
		WorkerHealthAddr:       env("WORKER_HEALTH_ADDR", "127.0.0.1:8081"),
		ObservabilityAddr:      env("OBSERVABILITY_ADDR", ""),
		PprofEnabled:           pprofEnabled,
		InventoryAuditInterval: durationEnv("SECKILL_AUDIT_INTERVAL", time.Minute),
		SeckillOutboxLimit:     positiveIntEnv("SECKILL_OUTBOX_LIMIT", 0),
		SeckillPendingLimit:    positiveIntEnv("SECKILL_PENDING_LIMIT", 0),
		SeckillRedisSharded:    seckillSharded,
		SeckillRedisCluster:    seckillCluster,
		SeckillStockBuckets:    positiveIntEnv("SECKILL_STOCK_BUCKETS", 1),
		AuthDevLogCodes:        authDevLogCodes,
		ShopAdminUserID:        shopAdminUserID,
		VoucherAdminUserID:     voucherAdminUserID,
		MediaEndpoint:          env("MEDIA_S3_ENDPOINT", ""),
		MediaAccessKey:         env("MEDIA_S3_ACCESS_KEY", ""),
		MediaSecretKey:         env("MEDIA_S3_SECRET_KEY", ""),
		MediaBucket:            env("MEDIA_S3_BUCKET", ""),
		MediaPublicURL:         env("MEDIA_PUBLIC_URL", ""),
		SocialPullAuthorIDs:    env("SOCIAL_PULL_AUTHOR_IDS", ""),
		TraceEndpoint:          env("TRACE_OTLP_ENDPOINT", ""),
		TraceSampleRatio:       traceRatio,
		AuthCodeIPLimit:        nonNegativeIntEnv("AUTH_CODE_IP_LIMIT", 20),
		SeckillUserLimit:       nonNegativeIntEnv("SECKILL_USER_LIMIT", 0),
		ShopBreakerEnabled:     shopBreaker,
		MySQLAddr:              env("MYSQL_ADDR", localAddr("MYSQL_PORT", "23306")),
		MySQLUser:              env("MYSQL_USER", "hmdp"),
		MySQLPassword:          env("MYSQL_PASSWORD", "hmdp_dev_mysql"),
		MySQLDatabase:          env("MYSQL_DATABASE", "hmdp"),
		MySQLMaxOpenConns:      positiveIntEnv("MYSQL_MAX_OPEN_CONNS", 20),
		RedisAddr:              env("REDIS_ADDR", localAddr("REDIS_PORT", "26379")),
		RedisPassword:          env("REDIS_PASSWORD", "hmdp_dev_redis"),
		RedisSentinels:         sentinels,
		RedisSentinelPassword:  env("REDIS_SENTINEL_PASSWORD", ""),
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

func nonNegativeIntEnv(key string, fallback int) int {
	value, err := strconv.Atoi(env(key, strconv.Itoa(fallback)))
	if err != nil || value < 0 {
		return fallback
	}
	return value
}
