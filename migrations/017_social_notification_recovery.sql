CREATE TABLE IF NOT EXISTS social_fanout_jobs (
    blog_id BIGINT UNSIGNED NOT NULL PRIMARY KEY,
    author_id BIGINT UNSIGNED NOT NULL,
    created_at DATETIME(3) NOT NULL,
    KEY idx_social_fanout_expiry (created_at, blog_id)
) ENGINE=InnoDB;

CREATE TABLE IF NOT EXISTS social_fanout_shards (
    blog_id BIGINT UNSIGNED NOT NULL,
    shard SMALLINT UNSIGNED NOT NULL,
    last_follower BIGINT UNSIGNED NOT NULL,
    end_follower BIGINT UNSIGNED NOT NULL,
    due_at DATETIME(3) NULL DEFAULT CURRENT_TIMESTAMP(3),
    lease_token VARCHAR(26) CHARACTER SET ascii COLLATE ascii_bin NULL,
    attempts TINYINT UNSIGNED NOT NULL DEFAULT 0,
    failed_at DATETIME(3) NULL,
    PRIMARY KEY (blog_id, shard),
    KEY idx_social_fanout_claim (due_at, blog_id, shard),
    KEY idx_social_fanout_failed (failed_at),
    CONSTRAINT social_fanout_shard_job FOREIGN KEY (blog_id) REFERENCES social_fanout_jobs (blog_id) ON DELETE CASCADE
) ENGINE=InnoDB;

-- Preserve unfinished cursors and completed replay fences from migration 015.
START TRANSACTION;
INSERT INTO social_fanout_jobs (blog_id, author_id, created_at)
SELECT blog_id, author_id, created_at FROM social_fanout_tasks
WHERE created_at >= TIMESTAMPADD(DAY, -30, CURRENT_TIMESTAMP(3))
ON DUPLICATE KEY UPDATE blog_id = social_fanout_jobs.blog_id;

INSERT INTO social_fanout_shards (blog_id, shard, last_follower, end_follower, due_at)
SELECT t.blog_id, 0, t.last_follower, 18446744073709551615, CURRENT_TIMESTAMP(3)
FROM social_fanout_tasks t
JOIN social_fanout_jobs j ON j.blog_id = t.blog_id
WHERE t.due_at IS NOT NULL
ON DUPLICATE KEY UPDATE shard = social_fanout_shards.shard;
DELETE FROM social_fanout_tasks;
COMMIT;

SET @add_blog_recent_index = IF(
    EXISTS (SELECT 1 FROM information_schema.statistics WHERE table_schema = DATABASE()
            AND table_name = 'tb_blog' AND index_name = 'idx_blog_author_recent'),
    'DO 0',
    'CREATE INDEX idx_blog_author_recent ON tb_blog (user_id, create_time DESC, id DESC)'
);
PREPARE blog_recent_index_stmt FROM @add_blog_recent_index;
EXECUTE blog_recent_index_stmt;
DEALLOCATE PREPARE blog_recent_index_stmt;
