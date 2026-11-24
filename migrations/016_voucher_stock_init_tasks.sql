CREATE TABLE IF NOT EXISTS voucher_stock_init_tasks (
    voucher_id BIGINT UNSIGNED NOT NULL PRIMARY KEY,
    status ENUM('pending', 'ready', 'missed_window') NOT NULL DEFAULT 'pending',
    attempts BIGINT UNSIGNED NOT NULL DEFAULT 0,
    due_at DATETIME(3) NULL DEFAULT CURRENT_TIMESTAMP(3),
    lease_token VARCHAR(26) CHARACTER SET ascii COLLATE ascii_bin NULL,
    KEY idx_voucher_stock_init_due (due_at, voucher_id),
    CONSTRAINT voucher_stock_init_link FOREIGN KEY (voucher_id) REFERENCES voucher_seckill_links (voucher_id) ON DELETE CASCADE
) ENGINE=InnoDB;

-- Upcoming activities created before this migration are safe to check again.
INSERT INTO voucher_stock_init_tasks (voucher_id)
SELECT link.voucher_id
FROM voucher_seckill_links AS link
JOIN seckill_activities AS a ON a.id = link.voucher_id
WHERE a.starts_at > UTC_TIMESTAMP(3)
ON DUPLICATE KEY UPDATE voucher_id = voucher_stock_init_tasks.voucher_id;
