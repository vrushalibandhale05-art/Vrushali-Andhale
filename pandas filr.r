import sqlite3
import pandas as pd

# 1. Connect to database
conn = sqlite3.connect("database.db")

# 2. Load tables into Pandas DataFrames
customers = pd.read_sql("SELECT * FROM customers", conn)
orders = pd.read_sql("SELECT * FROM orders", conn)
sales = pd.read_sql("SELECT * FROM sales", conn)

# 3. Merge tables (JOIN logic)
df = customers.merge(orders, on="customer_id") \
              .merge(sales, on="order_id")

# 4. Apply age filter (18–35)
df = df[df["age"].between(18, 35)]

# 5. Group by customer, age, item and sum quantity
result = (
    df.groupby(["customer_id", "age", "item"], as_index=False)["quantity"]
    .sum()
)

# 6. Remove zero or null quantities
result = result[result["quantity"] > 0]

# 7. Save output to CSV with semicolon delimiter
result.to_csv("output_pandas.csv", sep=";", index=False)

# 8. Close DB connection
conn.close()

print("Pandas output generated successfully")