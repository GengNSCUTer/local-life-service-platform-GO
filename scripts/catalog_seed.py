"""Emit deterministic fixtures for the isolated catalog scenario."""
import argparse
import random
import sys

USERS, SHOPS, BLOGS, SESSIONS, TYPES = 100_000, 10_000, 1_000_000, 10_000, 10
CENTERS = ((113.32, 23.13), (114.06, 22.54), (116.40, 39.90), (121.47, 31.23))


def insert(table, columns, rows):
    batch = []
    for row in rows:
        batch.append(row)
        if len(batch) == 2000:
            print(f"INSERT INTO {table} ({columns}) VALUES " + ",\n".join(batch) + ";")
            batch.clear()
    if batch:
        print(f"INSERT INTO {table} ({columns}) VALUES " + ",\n".join(batch) + ";")


def shop_rows():
    rng = random.Random(20261120)
    for shop in range(1, SHOPS + 1):
        kind = (shop - 1) % TYPES + 1
        center = 0 if shop <= SHOPS // 2 else 1 + (shop // TYPES) % 3
        x, y = CENTERS[center]
        x += rng.uniform(-0.018, 0.018)
        y += rng.uniform(-0.018, 0.018)
        yield (f"({shop}, 'Catalog Shop {shop:05d}', {kind}, '', 'Center {center}', "
               f"'Fixture Address {shop}', {x:.7f}, {y:.7f}, {30 + shop % 200})")


def sql():
    insert("tb_user", "id, phone, nick_name",
           (f"({i}, '199{i:08d}', 'Catalog User {i}')" for i in range(1, USERS + 1)))
    insert("tb_shop_type", "id, name, sort", (f"({i}, 'Catalog Type {i}', {i})" for i in range(1, TYPES + 1)))
    insert("tb_shop", "id, name, type_id, images, area, address, x, y, avg_price", shop_rows())
    insert("tb_blog", "id, shop_id, user_id, title, images, content, liked, create_time, update_time",
           (f"({i}, {(i - 1) * 4999 % SHOPS + 1}, {(i - 1) * 104729 % USERS + 1}, "
            f"'Catalog Blog {i}', '', 'Synthetic local shop review for catalog capacity testing.', "
            f"{i * 101 % 4096}, TIMESTAMPADD(SECOND, {i}, '2026-01-01'), TIMESTAMPADD(SECOND, {i}, '2026-01-01'))"
            for i in range(1, BLOGS + 1)))
    insert("social_follows", "user_id, follow_user_id",
           (f"({i}, {(i + offset - 1) % USERS + 1})" for i in range(1, USERS + 1) for offset in range(1, 11)))
    print("ANALYZE TABLE tb_shop, tb_blog, tb_user, social_follows;")


def sessions():
    for i in range(1, SESSIONS + 1):
        for parts in (("HSET", f"login:token:catalog-{i}", "id", str(i), "nickName", f"Catalog User {i}", "renew", "1"),
                      ("EXPIRE", f"login:token:catalog-{i}", "1800")):
            sys.stdout.write(f"*{len(parts)}\r\n")
            for part in parts:
                sys.stdout.write(f"${len(part)}\r\n{part}\r\n")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("output", choices=("sql", "sessions"))
    args = parser.parse_args()
    {"sql": sql, "sessions": sessions}[args.output]()
