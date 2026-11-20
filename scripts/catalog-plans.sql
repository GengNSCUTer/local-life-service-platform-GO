SELECT 'shop_name_shallow';
EXPLAIN ANALYZE SELECT * FROM tb_shop WHERE name LIKE '%Catalog Shop%' ESCAPE '!' ORDER BY id LIMIT 10;
SELECT 'shop_name_deep';
EXPLAIN ANALYZE SELECT * FROM tb_shop WHERE name LIKE '%Catalog Shop%' ESCAPE '!' ORDER BY id LIMIT 10 OFFSET 9890;
SELECT 'shop_name_rare';
EXPLAIN ANALYZE SELECT * FROM tb_shop WHERE name LIKE '%09999%' ESCAPE '!' ORDER BY id LIMIT 10;
SELECT 'shop_type_deep';
EXPLAIN ANALYZE SELECT * FROM tb_shop WHERE type_id = 1 ORDER BY id LIMIT 5 OFFSET 975;
SELECT 'hot_shallow';
EXPLAIN ANALYZE SELECT b.id, b.title, b.content, u.nick_name, l.user_id IS NOT NULL AS is_like
FROM tb_blog b LEFT JOIN tb_user u ON u.id = b.user_id LEFT JOIN social_blog_likes l ON l.blog_id = b.id AND l.user_id = 0
ORDER BY b.liked DESC, b.id DESC LIMIT 10;
SELECT 'hot_deep';
EXPLAIN ANALYZE SELECT b.id, b.title, b.content, u.nick_name, l.user_id IS NOT NULL AS is_like
FROM tb_blog b LEFT JOIN tb_user u ON u.id = b.user_id LEFT JOIN social_blog_likes l ON l.blog_id = b.id AND l.user_id = 0
ORDER BY b.liked DESC, b.id DESC LIMIT 10 OFFSET 9990;
SELECT 'feed_first';
EXPLAIN ANALYZE SELECT b.id, b.title, b.content, u.nick_name, l.user_id IS NOT NULL AS is_like
FROM tb_blog b JOIN social_follows f ON f.follow_user_id = b.user_id AND f.user_id = 1
LEFT JOIN tb_user u ON u.id = b.user_id LEFT JOIN social_blog_likes l ON l.blog_id = b.id AND l.user_id = 1
ORDER BY b.id DESC LIMIT 11;
SELECT 'feed_next';
EXPLAIN ANALYZE SELECT b.id, b.title, b.content, u.nick_name, l.user_id IS NOT NULL AS is_like
FROM tb_blog b JOIN social_follows f ON f.follow_user_id = b.user_id AND f.user_id = 1
LEFT JOIN tb_user u ON u.id = b.user_id LEFT JOIN social_blog_likes l ON l.blog_id = b.id AND l.user_id = 1
WHERE b.id < 900000 ORDER BY b.id DESC LIMIT 11;
