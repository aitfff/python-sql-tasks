import psycopg2
from psycopg2.extras import execute_values

conn = psycopg2.connect('postgresql://postgres:originals68@localhost:5432/test_db')


# BEGIN (write your solution here)
def batch_insert(conn, products):
    with conn.cursor() as cur:
        data = [(product['name'], product['price'], product['quantity']) for product in products]
        execute_values(cur, """
            INSERT INTO products (name, price, quantity) VALUES %s
        """, data)

def get_all_products(conn):
    with conn.cursor() as cur:
        cur.execute("SELECT * FROM products ORDER BY price DESC;")
        return cur.fetchall()
# END
