CREATE TABLE IF NOT EXISTS tb_voucher (
    id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
    shop_id BIGINT UNSIGNED NULL,
    title VARCHAR(255) NOT NULL,
    sub_title VARCHAR(255) NULL,
    rules VARCHAR(1024) NULL,
    pay_value BIGINT UNSIGNED NOT NULL DEFAULT 0,
    actual_value BIGINT NOT NULL DEFAULT 0,
    type TINYINT UNSIGNED NOT NULL DEFAULT 0,
    status TINYINT UNSIGNED NOT NULL DEFAULT 1,
    create_time TIMESTAMP NULL DEFAULT CURRENT_TIMESTAMP,
    update_time TIMESTAMP NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    PRIMARY KEY (id),
    KEY idx_voucher_shop_status (shop_id, status)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_general_ci;

CREATE TABLE IF NOT EXISTS voucher_seckill_links (
    voucher_id BIGINT UNSIGNED NOT NULL,
    PRIMARY KEY (voucher_id),
    CONSTRAINT voucher_seckill_voucher FOREIGN KEY (voucher_id) REFERENCES tb_voucher (id),
    CONSTRAINT voucher_seckill_activity FOREIGN KEY (voucher_id) REFERENCES seckill_activities (id)
) ENGINE=InnoDB;

CREATE TABLE IF NOT EXISTS voucher_create_requests (
    request_key VARCHAR(128) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
    request_hash BINARY(32) NOT NULL,
    voucher_id BIGINT UNSIGNED NULL,
    PRIMARY KEY (request_key),
    CONSTRAINT voucher_create_result FOREIGN KEY (voucher_id) REFERENCES tb_voucher (id)
) ENGINE=InnoDB;

SET @order_list_index_sql = IF(
    EXISTS (SELECT 1 FROM information_schema.statistics
            WHERE table_schema = DATABASE() AND table_name = 'seckill_orders' AND index_name = 'idx_seckill_user_history'),
    'SELECT 1',
    'CREATE INDEX idx_seckill_user_history ON seckill_orders (user_id, created_at, id)'
);
PREPARE order_list_index_stmt FROM @order_list_index_sql;
EXECUTE order_list_index_stmt;
DEALLOCATE PREPARE order_list_index_stmt;
