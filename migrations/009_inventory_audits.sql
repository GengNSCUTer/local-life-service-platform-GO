CREATE TABLE IF NOT EXISTS seckill_inventory_audits (
    voucher_id BIGINT UNSIGNED NOT NULL,
    checked_at DATETIME(6) NOT NULL,
    status VARCHAR(20) NOT NULL,
    detail VARCHAR(64) NOT NULL,
    total_stock BIGINT NOT NULL,
    mysql_remaining BIGINT NOT NULL,
    confirmed_orders BIGINT NOT NULL,
    mysql_claims BIGINT NOT NULL,
    pending_resolutions BIGINT NOT NULL,
    redis_remaining BIGINT NULL,
    redis_buyers BIGINT NULL,
    PRIMARY KEY (voucher_id),
    KEY idx_seckill_inventory_audit_status (status, checked_at)
) ENGINE=InnoDB;
