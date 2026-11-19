CREATE TABLE IF NOT EXISTS seckill_dead_letters (
    id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
    dlq_partition INT NOT NULL,
    dlq_offset BIGINT NOT NULL,
    source_key VARBINARY(255) NOT NULL,
    order_id VARCHAR(32) CHARACTER SET ascii COLLATE ascii_bin NULL,
    voucher_id BIGINT UNSIGNED NULL,
    user_id BIGINT UNSIGNED NULL,
    reason VARCHAR(64) NOT NULL,
    outcome VARCHAR(32) NOT NULL,
    payload LONGBLOB NOT NULL,
    created_at DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
    updated_at DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3),
    PRIMARY KEY (id),
    UNIQUE KEY seckill_dead_letter_position (dlq_partition, dlq_offset)
) ENGINE=InnoDB;
