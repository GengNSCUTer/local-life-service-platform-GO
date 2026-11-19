CREATE TABLE IF NOT EXISTS tb_blog (
    id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT PRIMARY KEY,
    shop_id BIGINT NOT NULL,
    user_id BIGINT UNSIGNED NOT NULL,
    title VARCHAR(255) NOT NULL,
    images VARCHAR(2048) NOT NULL,
    content VARCHAR(2048) NOT NULL,
    liked INT UNSIGNED NOT NULL DEFAULT 0,
    comments INT UNSIGNED NOT NULL DEFAULT 0,
    create_time TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    update_time TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    KEY idx_blog_author (user_id, id),
    KEY idx_blog_hot (liked, id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS social_blog_likes (
    blog_id BIGINT UNSIGNED NOT NULL,
    user_id BIGINT UNSIGNED NOT NULL,
    created_at DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
    PRIMARY KEY (blog_id, user_id),
    KEY idx_blog_likes_recent (blog_id, created_at, user_id),
    FOREIGN KEY (blog_id) REFERENCES tb_blog (id)
) ENGINE=InnoDB;

CREATE TABLE IF NOT EXISTS social_follows (
    user_id BIGINT UNSIGNED NOT NULL,
    follow_user_id BIGINT UNSIGNED NOT NULL,
    PRIMARY KEY (user_id, follow_user_id),
    KEY idx_followers (follow_user_id, user_id)
) ENGINE=InnoDB;

CREATE TABLE IF NOT EXISTS social_follow_versions (
    user_id BIGINT UNSIGNED NOT NULL PRIMARY KEY,
    revision BIGINT UNSIGNED NOT NULL
) ENGINE=InnoDB;

CREATE TABLE IF NOT EXISTS social_blog_outbox (
    blog_id BIGINT UNSIGNED NOT NULL PRIMARY KEY,
    author_id BIGINT UNSIGNED NOT NULL,
    created_at DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3)
) ENGINE=InnoDB;

CREATE TABLE IF NOT EXISTS shop_geo_versions (
    type_id BIGINT UNSIGNED NOT NULL PRIMARY KEY,
    revision BIGINT UNSIGNED NOT NULL
) ENGINE=InnoDB;

SET @blog_author_index = IF(
    EXISTS (SELECT 1 FROM information_schema.statistics WHERE table_schema = DATABASE() AND table_name = 'tb_blog' AND index_name = 'idx_blog_author'),
    'SELECT 1', 'CREATE INDEX idx_blog_author ON tb_blog (user_id, id)'
);
PREPARE blog_author_stmt FROM @blog_author_index;
EXECUTE blog_author_stmt;
DEALLOCATE PREPARE blog_author_stmt;

SET @blog_hot_index = IF(
    EXISTS (SELECT 1 FROM information_schema.statistics WHERE table_schema = DATABASE() AND table_name = 'tb_blog' AND index_name = 'idx_blog_hot'),
    'SELECT 1', 'CREATE INDEX idx_blog_hot ON tb_blog (liked, id)'
);
PREPARE blog_hot_stmt FROM @blog_hot_index;
EXECUTE blog_hot_stmt;
DEALLOCATE PREPARE blog_hot_stmt;
