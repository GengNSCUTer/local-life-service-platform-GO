SET @comment_columns = IF(
    EXISTS (SELECT 1 FROM information_schema.columns WHERE table_schema = DATABASE()
            AND table_name = 'social_blog_comments' AND column_name = 'reply_to_comment_id'),
    'DO 0',
    'ALTER TABLE social_blog_comments ADD COLUMN reply_to_comment_id BIGINT UNSIGNED NULL, ADD COLUMN request_key BINARY(32) NULL, ADD COLUMN request_digest BINARY(32) NULL, ADD UNIQUE KEY idx_comment_request (blog_id, author_id, request_key), ADD CONSTRAINT fk_comment_reply_target FOREIGN KEY (reply_to_comment_id) REFERENCES social_blog_comments (id)'
);
PREPARE comment_columns_stmt FROM @comment_columns;
EXECUTE comment_columns_stmt;
DEALLOCATE PREPARE comment_columns_stmt;

SET @visible_reply_index = IF(
    EXISTS (SELECT 1 FROM information_schema.statistics WHERE table_schema = DATABASE()
            AND table_name = 'social_blog_comments' AND index_name = 'idx_comment_visible_reply'),
    'DO 0',
    'CREATE INDEX idx_comment_visible_reply ON social_blog_comments (blog_id, parent_id, deleted_at, id)'
);
PREPARE visible_reply_index_stmt FROM @visible_reply_index;
EXECUTE visible_reply_index_stmt;
DEALLOCATE PREPARE visible_reply_index_stmt;

CREATE TABLE IF NOT EXISTS social_blog_counter_buckets (
    blog_id BIGINT UNSIGNED NOT NULL,
    bucket TINYINT UNSIGNED NOT NULL,
    liked_delta BIGINT NOT NULL DEFAULT 0,
    comments_delta BIGINT NOT NULL DEFAULT 0,
    dirty_since DATETIME(3) NULL DEFAULT NULL,
    PRIMARY KEY (blog_id, bucket),
    KEY idx_social_counters_dirty (dirty_since, blog_id)
) ENGINE=InnoDB;
