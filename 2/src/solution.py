import psycopg2

conn = psycopg2.connect('postgresql://postgres:originals68@localhost:5432/test_db')


# BEGIN (write your solution here)
def make_cars_table(conn):
    with conn.cursor() as cur:
        cur.execute("""
            CREATE TABLE IF NOT EXISTS cars (
                id SERIAL PRIMARY KEY,
                brand VARCHAR(255),
                model VARCHAR(255)
            );
        """)
    conn.commit()

def populate_cars_table(conn, cars):
    with conn.cursor() as cur:
        cur.executemany("""
            INSERT INTO cars (brand, model) VALUES (%s, %s);
        """, cars)
    conn.commit() 

def get_all_cars(conn):
    with conn.cursor() as cur:
        cur.execute("SELECT * FROM cars ORDER BY brand;")
        return cur.fetchall()
# END
