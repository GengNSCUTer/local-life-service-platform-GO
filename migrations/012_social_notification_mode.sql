SET @add_blog_notification_mode = IF(
    EXISTS (SELECT 1 FROM information_schema.columns WHERE table_schema = DATABASE()
            AND table_name = 'tb_blog' AND column_name = 'notification_mode'),
    'DO 0',
    'ALTER TABLE tb_blog ADD COLUMN notification_mode TINYINT UNSIGNED NOT NULL DEFAULT 0, ADD INDEX idx_blog_author_notification (user_id, notification_mode, id)'
);
PREPARE blog_notification_mode_stmt FROM @add_blog_notification_mode;
EXECUTE blog_notification_mode_stmt;
DEALLOCATE PREPARE blog_notification_mode_stmt;
