

import mysql.connector as con
import pandas as pd
from cryptography.fernet import Fernet
import psycopg2
from dotenv import load_dotenv
import os
from sqlalchemy import create_engine

load_dotenv()

mysql_db_config = {

    'user': os.getenv("MYSQL_USERNAME"),
    'password': os.getenv("MYSQL_PASSWORD"),
    'host': os.getenv("MYSQL_HOST"),
    'port': os.getenv("MYSQL_PORT", default="3306"),
    'database': os.getenv("MYSQL_DATABASE")
}

postgres_db_config = {

    'user': os.getenv("POSTGRES_USERNAME"),
    'password': os.getenv("POSTGRES_PASSWORD"),
    'host': os.getenv("POSTGRES_HOST"),
    'port': os.getenv("POSTGRES_PORT", default="5432"),
    'database': os.getenv("POSTGRES_DATABASE")
}


def load_csv_data(path):
    return pd.read_csv(path,sep=';',encoding='latin-1')

ecommerce_df=load_csv_data("data/ecommerce.csv")
cupones_df=load_csv_data("data/Cupones.csv")


def transform_ecommerce_data(df):
    df.dropna(inplace=True)
    df["InvoiceNo"] = df['InvoiceNo'].astype(str)
    df["StockCode"] = df['StockCode'].astype(str)
    df["Description"] = df['Description'].astype(str)
    df['Quantity'] = pd.to_numeric(df['Quantity'], errors='coerce')
    return df


def transform_cupones_Data(df):
    df['Discountcoupon'] = pd.to_numeric(df['Discountcoupon'], errors='coerce')
    df.dropna(how='any', inplace=True)


    return df


# Generate a random encryption key
encryption_key = Fernet.generate_key()

# Create a Fernet object with the key
fernet = Fernet(encryption_key)

# Function to encrypt a column
def encrypt_column(column, fernet_obj):
    return column.apply(lambda x: fernet_obj.encrypt(str(x).encode()))



ecommerce_df_transformed=transform_ecommerce_data(ecommerce_df)

cupones_df_transformed= transform_cupones_Data(cupones_df)







# # Encrypt the 'e' column
ecommerce_df_transformed['CustomerID'] = encrypt_column(ecommerce_df_transformed['CustomerID'], fernet)
cupones_df_transformed['CustomerID'] = encrypt_column(cupones_df_transformed['CustomerID'], fernet)



connection=con.connect(host=mysql_db_config['host'],user=mysql_db_config["user"],password=mysql_db_config["password"])
cursor=connection.cursor()



# Create a schema (if not exists)
create_schema_query = "CREATE SCHEMA IF NOT EXISTS onlinestore"
cursor.execute(create_schema_query)

# Use the specified schema
use_schema_query = "USE onlinestore"
cursor.execute(use_schema_query)


# Create a table based on DataFrame columns and types
create_table_query = f"CREATE TABLE IF NOT EXISTS ecommerce ({', '.join([f'{col} VARCHAR(255)' for col in ecommerce_df_transformed.columns])})"
cursor.execute(create_table_query)
print(create_table_query)

# Insert data into the table
print(len(ecommerce_df_transformed))
insert_data_query = f"INSERT INTO ecommerce ({', '.join(ecommerce_df_transformed.columns)}) VALUES ({', '.join(['%s' for _ in ecommerce_df_transformed.columns])})"
values = [tuple(row) for row in ecommerce_df_transformed.itertuples(index=False)]

cursor.executemany(insert_data_query, values)

cursor.execute("ALTER TABLE ecommerce MODIFY COLUMN Quantity FLOAT")
cursor.execute("ALTER TABLE ecommerce MODIFY COLUMN UnitPrice FLOAT")




cursor.execute("USE onlinestore")
cursor.execute('select CustomerID , StockCode , Description, SUM(Quantity) as Quantity from ecommerce GROUP BY CustomerID , StockCode , Description;')
mysql_data = cursor.fetchall()


column_names = [desc[0] for desc in cursor.description]
mysql_df = pd.DataFrame(mysql_data, columns=column_names)




# Commit the changes
connection.commit()

# Close the cursor and connection
cursor.close()
connection.close()

print("Table and schema created, and data uploaded.")


# Create a connection string
engine_str = f"postgresql+psycopg2://{postgres_db_config['user']}:{postgres_db_config['password']}@{postgres_db_config['host']}:{postgres_db_config['port']}/{postgres_db_config['database']}"



# Create a SQLAlchemy engine
engine = create_engine(engine_str)

# Replace 'your_table' with the desired table name
table_name = 'ecommerce'

# Create the table in the PostgreSQL database with inferred data types
mysql_df.to_sql(table_name, engine, index=False, if_exists='replace', dtype=None)
mysql_df.to_sql(table_name, engine, index=False, if_exists='append', dtype=None)

query_ecommerce = 'SELECT * FROM ' + table_name
ecommerce = pd.read_sql(query_ecommerce, engine)
print(ecommerce)


table_name2="coupons"

cupones_df_transformed.to_sql(table_name2, engine, index=False, if_exists='replace', dtype=None)
cupones_df_transformed.to_sql(table_name2, engine, index=False, if_exists='append', dtype=None)

query_cupones = 'select "CustomerID", COUNT("Discountcoupon") from coupons group by "CustomerID"'
print(query_cupones)
coupons = pd.read_sql(query_cupones, engine)




# Specify the path where you want to save the CSV file
ecommerce_csv_file_path = 'Output_Data/ecommerce.csv'

# Save the DataFrame to a CSV file
ecommerce.to_csv(ecommerce_csv_file_path, index=False)


# Specify the path where you want to save the CSV file
coupons_csv_file_path = 'Output_Data/coupons.csv'

# Save the DataFrame to a CSV file
coupons.to_csv(coupons_csv_file_path, index=False)


