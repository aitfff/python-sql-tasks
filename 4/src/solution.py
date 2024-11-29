import psycopg2
from psycopg2.extras import DictCursor


conn = psycopg2.connect('postgresql://postgres:originals68@localhost:5432/test_db')


# BEGIN (write your solution here)
def get_order_sum(conn, month):
    query = """
    SELECT
        c.customer_name,
        SUM(o.total_amount) AS total_amount
    FROM
        customers c
    LEFT JOIN
        orders o ON c.customer_id = o.customer_id
    WHERE
        EXTRACT(MONTH FROM o.order_date) = %s
    GROUP BY
        c.customer_id
    ORDER BY
        c.customer_name;
    """
    
    with conn.cursor(cursor_factory=DictCursor) as cur:
        cur.execute(query, (month,))
        results = cur.fetchall()

    output = []
    for row in results:
        customer_name = row['customer_name']
        total_amount = row['total_amount'] if row['total_amount'] is not None else 0
        output.append(f"Покупатель {customer_name} совершил покупок на сумму {total_amount}")
    
    return "\n".join(output)
# END
