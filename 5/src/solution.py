import psycopg2
from psycopg2.extras import DictCursor


conn = psycopg2.connect('postgresql://postgres:originals68@localhost:5432/test_db')


# BEGIN (write your solution here)
def create_post(conn, post_data):
    """
    Создает новый пост и возвращает его ID

    :param conn: Соединение с базой данных
    :param post_data: Словарь с данными поста (title, content, author_id)
    :return: ID нового поста
    """
    with conn.cursor() as cur:
        cur.execute("""
            INSERT INTO posts (title, content, author_id)
            VALUES (%s, %s, %s) RETURNING id;
        """, (post_data['title'], post_data['content'], post_data['author_id']))

        post_id = cur.fetchone()[0]

        conn.commit()

    return post_id


def add_comment(conn, comment_data):
    with conn.cursor() as cur:
        cur.execute("""
            INSERT INTO comments (post_id, author_id, content)
            VALUES (%s, %s, %s) RETURNING id;
        """, (comment_data['post_id'], comment_data['author_id'], comment_data['content']))
        comment_id = cur.fetchone()[0]
        conn.commit()
    return comment_id


def get_latest_posts(conn, limit):
    with conn.cursor(cursor_factory=DictCursor) as cur:
        cur.execute("""
            SELECT p.id, p.title, p.content, p.author_id,
                   COALESCE(json_agg(c.*) FILTER (WHERE c.id IS NOT NULL), '[]') AS comments
            FROM posts p
            LEFT JOIN comments c ON c.post_id = p.id
            GROUP BY p.id
            ORDER BY p.id DESC
            LIMIT %s;
        """, (limit,))
        return cur.fetchall()

    posts = []
    post_dict = {}

    for row in rows:
        post_id = row['id']
        
        if post_id not in post_dict:
            post_dict[post_id] = {
                'id': post_id,
                'title': row['title'],
                'content': row['content'],
                'author_id': row['author_id'],
                'created_at': row['created_at'],
                'comments': []
            }
        
        if row['comment_id'] is not None:
            post_dict[post_id]['comments'].append({
                'id': row['comment_id'],
                'author_id': row['comment_author_id'],
                'content': row['comment_content'],
                'created_at': row['comment_created_at']
            })

    posts = list(post_dict.values())
    
    return posts
# END
