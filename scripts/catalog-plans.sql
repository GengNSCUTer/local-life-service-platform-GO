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
SELECT 'hot_deep_covering';
EXPLAIN ANALYZE SELECT b.id,b.title,b.content,u.nick_name,l.user_id IS NOT NULL AS is_like
FROM (SELECT id, liked FROM tb_blog FORCE INDEX (idx_blog_hot) ORDER BY liked DESC,id DESC LIMIT 10 OFFSET 9990) ranked
JOIN tb_blog b ON b.id=ranked.id LEFT JOIN tb_user u ON u.id=b.user_id
LEFT JOIN social_blog_likes l ON l.blog_id=b.id AND l.user_id=0 ORDER BY ranked.liked DESC,ranked.id DESC;
SELECT 'hot_deep_cursor';
SET @liked = (SELECT liked FROM tb_blog FORCE INDEX (idx_blog_hot) ORDER BY liked DESC,id DESC LIMIT 1 OFFSET 9989);
SET @prior = (SELECT id FROM tb_blog FORCE INDEX (idx_blog_hot) ORDER BY liked DESC,id DESC LIMIT 1 OFFSET 9989);
EXPLAIN ANALYZE SELECT b.id,b.title,b.content,u.nick_name,l.user_id IS NOT NULL AS is_like
FROM (SELECT id, liked FROM tb_blog FORCE INDEX (idx_blog_hot) WHERE liked < @liked OR (liked = @liked AND id < @prior) OR liked IS NULL
      ORDER BY liked DESC,id DESC LIMIT 11) ranked
JOIN tb_blog b ON b.id=ranked.id LEFT JOIN tb_user u ON u.id=b.user_id
LEFT JOIN social_blog_likes l ON l.blog_id=b.id AND l.user_id=0 ORDER BY ranked.liked DESC,ranked.id DESC;
SELECT 'shop_type_cursor';
EXPLAIN ANALYZE SELECT s.* FROM (SELECT id FROM tb_shop WHERE type_id=1 AND id>9751 ORDER BY id LIMIT 6) page
JOIN tb_shop s ON s.id=page.id ORDER BY page.id;
