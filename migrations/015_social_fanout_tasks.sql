CREATE TABLE IF NOT EXISTS social_fanout_tasks (
    blog_id BIGINT UNSIGNED NOT NULL PRIMARY KEY,
    author_id BIGINT UNSIGNED NOT NULL,
    created_at DATETIME(3) NOT NULL,
    last_follower BIGINT UNSIGNED NOT NULL DEFAULT 0,
    due_at DATETIME(3) NULL DEFAULT CURRENT_TIMESTAMP(3),
    lease_token VARCHAR(26) CHARACTER SET ascii COLLATE ascii_bin NULL,
    KEY idx_social_fanout_due (due_at, blog_id)
) ENGINE=InnoDB;
