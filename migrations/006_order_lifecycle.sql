CREATE TABLE IF NOT EXISTS seckill_order_claims (
    voucher_id BIGINT UNSIGNED NOT NULL,
    user_id BIGINT UNSIGNED NOT NULL,
    order_id VARCHAR(32) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
    created_at DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
    PRIMARY KEY (voucher_id, user_id),
    UNIQUE KEY seckill_claim_order (order_id),
    CONSTRAINT seckill_claim_activity FOREIGN KEY (voucher_id) REFERENCES seckill_activities (id),
    CONSTRAINT seckill_claim_order_fk FOREIGN KEY (order_id) REFERENCES seckill_orders (id)
) ENGINE=InnoDB;

INSERT IGNORE INTO seckill_order_claims (voucher_id, user_id, order_id, created_at)
SELECT voucher_id, user_id, id, created_at
FROM seckill_orders
WHERE status IN ('pending', 'confirmed', 'review_required');

SET @add_order_activity_index_sql = IF(
    EXISTS (SELECT 1 FROM information_schema.statistics
            WHERE table_schema = DATABASE()
              AND table_name = 'seckill_orders'
              AND index_name = 'seckill_order_activity_idx'),
    'SELECT 1',
    'ALTER TABLE seckill_orders ADD KEY seckill_order_activity_idx (voucher_id)'
);
PREPARE add_order_activity_index_stmt FROM @add_order_activity_index_sql;
EXECUTE add_order_activity_index_stmt;
DEALLOCATE PREPARE add_order_activity_index_stmt;

SET @drop_order_buyer_index_sql = IF(
    EXISTS (SELECT 1 FROM information_schema.statistics
            WHERE table_schema = DATABASE()
              AND table_name = 'seckill_orders'
              AND index_name = 'seckill_one_order_per_user'),
    'ALTER TABLE seckill_orders DROP INDEX seckill_one_order_per_user',
    'SELECT 1'
);
PREPARE drop_order_buyer_index_stmt FROM @drop_order_buyer_index_sql;
EXECUTE drop_order_buyer_index_stmt;
DEALLOCATE PREPARE drop_order_buyer_index_stmt;

SET @add_cancelled_at_sql = IF(
    EXISTS (SELECT 1 FROM information_schema.columns
            WHERE table_schema = DATABASE()
              AND table_name = 'seckill_orders'
              AND column_name = 'cancelled_at'),
    'SELECT 1',
    'ALTER TABLE seckill_orders ADD COLUMN cancelled_at DATETIME(3) NULL AFTER created_at'
);
PREPARE add_cancelled_at_stmt FROM @add_cancelled_at_sql;
EXECUTE add_cancelled_at_stmt;
DEALLOCATE PREPARE add_cancelled_at_stmt;

CREATE TABLE IF NOT EXISTS seckill_order_resolutions (
    order_id VARCHAR(32) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
    reason VARCHAR(32) NOT NULL,
    redis_synced BOOLEAN NOT NULL DEFAULT FALSE,
    attempts INT UNSIGNED NOT NULL DEFAULT 0,
    last_error VARCHAR(512) NULL,
    created_at DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
    updated_at DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3),
    PRIMARY KEY (order_id),
    KEY idx_seckill_resolution_sync (redis_synced, updated_at),
    CONSTRAINT seckill_resolution_order FOREIGN KEY (order_id) REFERENCES seckill_orders (id)
) ENGINE=InnoDB;
