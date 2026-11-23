SET @add_seckill_layout = IF(
    EXISTS (SELECT 1 FROM information_schema.columns WHERE table_schema = DATABASE()
            AND table_name = 'seckill_activities' AND column_name = 'redis_shard'),
    'DO 0',
    'ALTER TABLE seckill_activities ADD COLUMN redis_shard TINYINT UNSIGNED NOT NULL DEFAULT 0, ADD COLUMN stock_buckets TINYINT UNSIGNED NOT NULL DEFAULT 0, ADD INDEX idx_seckill_activity_shard (redis_shard)'
);
PREPARE seckill_layout_stmt FROM @add_seckill_layout;
EXECUTE seckill_layout_stmt;
DEALLOCATE PREPARE seckill_layout_stmt;

SET @add_seckill_order_bucket = IF(
    EXISTS (SELECT 1 FROM information_schema.columns WHERE table_schema = DATABASE()
            AND table_name = 'seckill_orders' AND column_name = 'stock_bucket'),
    'DO 0',
    'ALTER TABLE seckill_orders ADD COLUMN stock_bucket TINYINT UNSIGNED NULL'
);
PREPARE seckill_order_bucket_stmt FROM @add_seckill_order_bucket;
EXECUTE seckill_order_bucket_stmt;
DEALLOCATE PREPARE seckill_order_bucket_stmt;

CREATE TABLE IF NOT EXISTS seckill_stock_buckets (
    voucher_id BIGINT UNSIGNED NOT NULL,
    bucket TINYINT UNSIGNED NOT NULL,
    total_stock INT UNSIGNED NOT NULL,
    remaining_stock INT UNSIGNED NOT NULL,
    PRIMARY KEY (voucher_id, bucket),
    CONSTRAINT seckill_bucket_activity FOREIGN KEY (voucher_id) REFERENCES seckill_activities (id)
) ENGINE=InnoDB;
