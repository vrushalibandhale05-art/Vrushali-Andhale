import sqlite3
import pandas as pd

conn = sqlite3.connect("database.db")
cursor = conn.cursor()


cursor.execute("""
CREATE TABLE IF NOT EXISTS customers (
    customer_id INTEGER,
    age INTEGER
);
""")

cursor.execute("""
CREATE TABLE IF NOT EXISTS orders (
    order_id INTEGER,
    customer_id INTEGER
);
""")

cursor.execute("""
CREATE TABLE IF NOT EXISTS sales (
    order_id INTEGER,
    item TEXT,
    quantity INTEGER
);
""")


cursor.execute("SELECT COUNT(*) FROM customers")
if cursor.fetchone()[0] == 0:
    cursor.executemany(
        "INSERT INTO customers VALUES (?, ?)",
        [(1, 21), (2, 23), (3, 35), (4, 40)]
    )

    cursor.executemany(
        "INSERT INTO orders VALUES (?, ?)",
        [(101, 1), (102, 1), (103, 2), (104, 3), (105, 4)]
    )

    cursor.executemany(
        "INSERT INTO sales VALUES (?, ?, ?)",
        [
            (101, 'x', 5),
            (102, 'x', 5),
            (103, 'x', 1),
            (103, 'y', 1),
            (103, 'z', 1),
            (104, 'z', 2),
            (105, 'x', 3)
        ]
    )

conn.commit()


customers = pd.read_sql("SELECT * FROM customers", conn)
orders = pd.read_sql("SELECT * FROM orders", conn)
sales = pd.read_sql("SELECT * FROM sales", conn)


df = customers.merge(orders, on="customer_id") \
              .merge(sales, on="order_id")


df = df[df["age"].between(18, 35)]


result = (
    df.groupby(["customer_id", "age", "item"], as_index=False)["quantity"]
    .sum()
)


result = result[result["quantity"] > 0]


result.to_csv("output_pandas.csv", sep=";", index=False)

conn.close()

print("Pandas output generated successfully")