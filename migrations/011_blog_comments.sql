CREATE TABLE IF NOT EXISTS social_blog_comments (
    id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT PRIMARY KEY,
    blog_id BIGINT UNSIGNED NOT NULL,
    author_id BIGINT UNSIGNED NOT NULL,
    parent_id BIGINT UNSIGNED NULL,
    content VARCHAR(1000) NOT NULL,
    created_at DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
    deleted_at DATETIME(3) NULL,
    KEY idx_comment_page (blog_id, parent_id, id),
    FOREIGN KEY (blog_id) REFERENCES tb_blog (id),
    FOREIGN KEY (parent_id) REFERENCES social_blog_comments (id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
