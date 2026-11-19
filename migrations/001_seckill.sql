CREATE TABLE IF NOT EXISTS seckill_activities (
    id BIGINT UNSIGNED NOT NULL,
    total_stock INT UNSIGNED NOT NULL,
    remaining_stock INT UNSIGNED NOT NULL,
    starts_at DATETIME(3) NOT NULL,
    ends_at DATETIME(3) NOT NULL,
    PRIMARY KEY (id)
) ENGINE=InnoDB;

CREATE TABLE IF NOT EXISTS seckill_orders (
    id VARCHAR(32) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
    voucher_id BIGINT UNSIGNED NOT NULL,
    user_id BIGINT UNSIGNED NOT NULL,
    status VARCHAR(20) NOT NULL,
    created_at DATETIME(3) NOT NULL,
    PRIMARY KEY (id),
    UNIQUE KEY seckill_one_order_per_user (voucher_id, user_id),
    CONSTRAINT seckill_order_activity FOREIGN KEY (voucher_id) REFERENCES seckill_activities (id)
) ENGINE=InnoDB;
