import psycopg2
from psycopg2 import sql, IntegrityError
import csv

#Database credentials
dbname = 'db_products'
user = 'postgres'
password = 'postgres'
host = 'localhost'
port = '5432'

csv_file_path = './data/Orders.csv'
table_name = 'orders'

# Connect to the database
conn = psycopg2.connect(
    dbname=dbname,
    user=user,
    password=password,
    host=host,
    port=port
)

cur = conn.cursor()

# Open and read the order
with open(csv_file_path, "r") as f:
    csv_reader = csv.reader(f)
    header = next(csv_reader)  # Skip the header row

    for row in csv_reader:
        try:
            # Adjust the column names based on your table structure
            insert_sql = sql.SQL("INSERT INTO {} VALUES ({})").format(
                sql.Identifier(table_name),
                sql.SQL(', ').join(sql.Placeholder() * len(row))
            )
            cur.execute(insert_sql, row)
        except IntegrityError as e:
            conn.rollback()  # Rollback the transaction
            print(f"Skipping duplicate record: {row}")
        else:
            conn.commit()  # Commit the transaction if no error occurred

cur.close()
conn.close()

print("Data import successful.")



