CREATE TABLE catalog_search_sample (
    id BIGINT UNSIGNED PRIMARY KEY,
    name VARCHAR(128) NOT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_general_ci;
INSERT INTO catalog_search_sample (id, name)
SELECT id, CONCAT(ELT(1 + MOD(id, 7), '广州老字号茶肆', '北京社区小馆', '上海咖啡烘焙',
    '深圳潮汕砂锅粥', '烧鹅·茶餐厅', '热%茶_档', '糖水铺'), ' ', LPAD(id, 5, '0')) FROM tb_shop;
ALTER TABLE catalog_search_sample ADD FULLTEXT KEY idx_ngram (name) WITH PARSER ngram;
SELECT @@ngram_token_size AS token_size;
SELECT 'two chars' AS sample,
    (SELECT COUNT(*) FROM catalog_search_sample WHERE name LIKE '%咖啡%') AS literal_matches,
    (SELECT COUNT(*) FROM catalog_search_sample WHERE MATCH(name) AGAINST('"咖啡"' IN BOOLEAN MODE)
        AND name LIKE '%咖啡%') AS verified_ngram_matches
UNION ALL SELECT 'one char',
    (SELECT COUNT(*) FROM catalog_search_sample WHERE name LIKE '%茶%'),
    (SELECT COUNT(*) FROM catalog_search_sample WHERE MATCH(name) AGAINST('"茶"' IN BOOLEAN MODE)
        AND name LIKE '%茶%')
UNION ALL SELECT 'punctuation',
    (SELECT COUNT(*) FROM catalog_search_sample WHERE name LIKE '%鹅·茶%'),
    (SELECT COUNT(*) FROM catalog_search_sample WHERE MATCH(name) AGAINST('"鹅·茶"' IN BOOLEAN MODE)
        AND name LIKE '%鹅·茶%');
EXPLAIN ANALYZE SELECT id FROM catalog_search_sample WHERE name LIKE '%砂锅粥%' ORDER BY id LIMIT 10;
EXPLAIN ANALYZE SELECT id FROM catalog_search_sample WHERE MATCH(name) AGAINST('"砂锅粥"' IN BOOLEAN MODE)
    AND name LIKE '%砂锅粥%' ORDER BY id LIMIT 10;
