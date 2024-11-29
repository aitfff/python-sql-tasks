import psycopg2

conn = psycopg2.connect('postgresql://postgres:originals68@localhost:5432/test_db')


# BEGIN (write your solution here)
def add_movies(conn):
    with conn.cursor() as cur:
        insert_query = "INSERT INTO movies (title, release_year, duration) VALUES (%s, %s, %s)"
        
        movies_data = [
            ('Godfather', 1972, 175),
            ('The Green Mile', 1999, 189)
        ]

        cur.executemany(insert_query, movies_data)
        conn.commit()

def get_all_movies(conn):
    with conn.cursor() as cur:
        select_query = "SELECT * FROM movies"
        cur.execute(select_query)

        try:
            movies = cur.fetchall()
        except UnicodeDecodeError as e:
            print(f"Ошибка декодирования: {e}")
            return []

        formatted_movies = [(id, title, release_year, duration) for (id, title, release_year, duration) in movies]
        return formatted_movies
# END
