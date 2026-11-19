CREATE TABLE IF NOT EXISTS seckill_reservation_anomalies (
    order_id VARCHAR(32) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
    created_at DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
    PRIMARY KEY (order_id)
) ENGINE=InnoDB;
