import sqlite3
import pandas as pd

conn = sqlite3.connect("database.db")

query = """
SELECT
    c.customer_id AS Customer,
    c.age AS Age,
    s.item AS Item,
    SUM(s.quantity) AS Quantity
FROM customers c
JOIN orders o ON c.customer_id = o.customer_id
JOIN sales s ON o.order_id = s.order_id
WHERE c.age BETWEEN 18 AND 35
GROUP BY c.customer_id, c.age, s.item
HAVING SUM(s.quantity) > 0;
"""

df = pd.read_sql_query(query, conn)

df.to_csv("output_sql.csv", sep=";", index=False)

conn.close()

print("Done")