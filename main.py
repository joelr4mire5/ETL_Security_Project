

import mysql.connector as con
import pandas as pd
from cryptography.fernet import Fernet

from dotenv import load_dotenv
import os


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
    return df.dropna(how='all',inplace=True)


# Generate a random encryption key
encryption_key = Fernet.generate_key()

# Create a Fernet object with the key
fernet = Fernet(encryption_key)

# Function to encrypt a column
def encrypt_column(column, fernet_obj):
    return column.apply(lambda x: fernet_obj.encrypt(str(x).encode()).decode())

# Function to decrypt a column
def decrypt_column(column, fernet_obj):
    return column.apply(lambda x: fernet_obj.decrypt(str(x).encode()).decode())


ecommerce_df_transformed=transform_ecommerce_data(ecommerce_df)

# Encrypt the 'SensitiveData' column
ecommerce_df_transformed['CustomerID'] = encrypt_column(ecommerce_df_transformed['CustomerID'], fernet)


connection=con.connect(host=mysql_db_config['host'],user=mysql_db_config["user"],password=mysql_db_config["password"])
cursor=connection.cursor()

# Create a schema (if not exists)
create_schema_query = "CREATE SCHEMA IF NOT EXISTS onlinestore"
cursor.execute(create_schema_query)

# Use the specified schema
use_schema_query = "USE onlinestore"
cursor.execute(use_schema_query)

table_name = 'ecommerce'
column_name = 'Quantity'
new_data_type = 'FLOAT'



# Read CSV file into a pandas DataFrame
df = ecommerce_df_transformed

# Create a table based on DataFrame columns and types
create_table_query = f"CREATE TABLE IF NOT EXISTS ecommerce ({', '.join([f'{col} VARCHAR(255)' for col in df.columns])})"
cursor.execute(create_table_query)

# Insert data into the table
insert_data_query = f"INSERT INTO ecommerce ({', '.join(df.columns)}) VALUES ({', '.join(['%s' for _ in df.columns])})"
values = [tuple(row) for row in df.itertuples(index=False)]
cursor.executemany(insert_data_query, values)

cursor.execute("ALTER TABLE ecommerce MODIFY COLUMN Quantity FLOAT")
cursor.execute("ALTER TABLE ecommerce MODIFY COLUMN UnitPrice FLOAT")
cursor.execute("""UPDATE your_table
        SET InvoiceDate = STR_TO_DATE(InvoiceDate, '%m/%d/%Y %h:%i'""")




# Commit the changes
connection.commit()

# Close the cursor and connection
cursor.close()
connection.close()

print("Table and schema created, and data uploaded.")

