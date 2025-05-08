import pymysql
import pandas as pd
import os

# Database connection details
DB_HOST = "localhost"  # Change if MySQL is running in a container
DB_PORT = 3306
DB_USER = "root"
DB_PASSWORD = "root"
DB_NAME = "test"
TABLE_NAME = "category"
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
CSV_FILE_PATH = os.path.join(SCRIPT_DIR, "../data/")


def create_database(cursor, connection):
    # Create database if not exits

    create_database_query = f"""
        CREATE DATABASE IF NOT EXISTS test;
    """
    cursor.execute(create_database_query)
    connection.commit()
    print("Database created successfully!")


def load_holidays_events(cursor, connection, csv_file_path, table_name):
    row_id = 0
    # Read CSV file
    df = pd.read_csv(csv_file_path, encoding="utf-8", sep=",")

    # Replace NaN with None for MySQL
    df = df.where(pd.notna(df), None)

    drop_table_query = f"""
        DROP TABLE IF EXISTS test.{table_name};
    """
    cursor.execute(drop_table_query)
    connection.commit()
    print("Table deleted successfully!")

    # Create table if it doesn't exist (modify columns as needed)
    create_table_query = f"""
    CREATE TABLE IF NOT EXISTS test.{table_name} (
        id INT AUTO_INCREMENT PRIMARY KEY,
        date DATE,
        type VARCHAR(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci,
        locale VARCHAR(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci,
        locale_name VARCHAR(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci,
        description VARCHAR(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci,
        transferred BOOLEAN
    );
    """
    cursor.execute(create_table_query)
    connection.commit()
    print("Table created successfully!")

    # Insert data
    for _, row in df.iterrows():
        insert_query = f"""INSERT INTO test.{table_name} (
            date,
            type,
            locale,
            locale_name,
            description,
            transferred
        ) VALUES (%s, %s, %s, %s, %s, %s, %s)"""
        print(row)
        cursor.execute(insert_query, (
                                    str(row.iloc[1]) if pd.notna(row.iloc[1]) else "",
                                    int(row.iloc[2]) if pd.notna(row.iloc[2]) else -1,
                                    str(row.iloc[3]) if pd.notna(row.iloc[3]) else "",
                                    int(row.iloc[4]) if pd.notna(row.iloc[4]) else -1,
                                    int(row.iloc[5]) if pd.notna(row.iloc[5]) else -1,
                                    int(row.iloc[6]) if pd.notna(row.iloc[6]) else -1))

    connection.commit()


    print(f"Table {table_name} successfully loaded into MySQL.")


def load_items(cursor, connection, csv_file_path, table_name):
    row_id = 0
    # Read CSV file
    df = pd.read_csv(csv_file_path, encoding="utf-8", sep=",")

    # Replace NaN with None for MySQL
    df = df.where(pd.notna(df), None)

    drop_table_query = f"""
        DROP TABLE IF EXISTS test.{table_name};
    """
    cursor.execute(drop_table_query)
    connection.commit()
    print("Table deleted successfully!")

    # Create table if it doesn't exist (modify columns as needed)
    create_table_query = f"""
    CREATE TABLE IF NOT EXISTS test.{table_name} (
        id INT AUTO_INCREMENT PRIMARY KEY,
        item_nbr INT,
        family VARCHAR(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci,
        class INT,
        perishable INT
    );
    """
    cursor.execute(create_table_query)
    connection.commit()
    print(f"Table {table_name} created successfully!")

    # Insert data
    for _, row in df.iterrows():
        insert_query = f"""INSERT INTO test.{table_name} (
            item_nbr,
            family,
            class,
            perishable
        ) VALUES (%s, %s, %s, %s, %s)"""
        print(row)
        cursor.execute(insert_query, (
            int(row.iloc[0]) if pd.notna(row.iloc[0]) else -1,
            str(row.iloc[1]) if pd.notna(row.iloc[1]) else "",
            int(row.iloc[2]) if pd.notna(row.iloc[2]) else -1,
            int(row.iloc[3]) if pd.notna(row.iloc[3]) else -1))

    connection.commit()

    print(f"Table {table_name} successfully loaded into MySQL.")


def load_oil(cursor, connection, csv_file_path, table_name):
    row_id = 0
    # Read CSV file 
    df = pd.read_csv(csv_file_path, encoding="utf-8", sep=",")

    # Replace NaN with None for MySQL
    df = df.where(pd.notna(df), None)

    drop_table_query = f"""
        DROP TABLE IF EXISTS test.{table_name}; 
    """
    cursor.execute(drop_table_query)
    connection.commit()
    print("Table deleted successfully!")

    # Create table if it doesn't exist (modify columns as needed)
    create_table_query = f"""
    CREATE TABLE IF NOT EXISTS test.{table_name} (
        id INT AUTO_INCREMENT PRIMARY KEY,
        item_nbr INT,
        family VARCHAR(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci,
        class INT,
        perishable INT
    );
    """
    cursor.execute(create_table_query)
    connection.commit()
    print(f"Table {table_name} created successfully!")

    # Insert data   
    for _, row in df.iterrows():
        insert_query = f"""INSERT INTO test.{table_name} (
            item_nbr,
            family,
            class,
            perishable
        ) VALUES (%s, %s, %s, %s, %s)"""
        print(row)
        cursor.execute(insert_query, (
            int(row.iloc[0]) if pd.notna(row.iloc[0]) else -1,
            str(row.iloc[1]) if pd.notna(row.iloc[1]) else "",
            int(row.iloc[2]) if pd.notna(row.iloc[2]) else -1,
            int(row.iloc[3]) if pd.notna(row.iloc[3]) else -1))

    connection.commit()

    print(f"Table {table_name} successfully loaded into MySQL.")


def load_stores(cursor, connection, csv_file_path, table_name):
    row_id = 0
    # Read CSV file
    df = pd.read_csv(csv_file_path, encoding="utf-8", sep=",")


    # Replace NaN with None for MySQL
    df = df.where(pd.notna(df), None)

    drop_table_query = f"""
        DROP TABLE IF EXISTS test.{table_name};
    """

    cursor.execute(drop_table_query)
    connection.commit()
    print("Table deleted successfully!")

    # Create table if it doesn't exist (modify columns as needed)
    create_table_query = f"""
    CREATE TABLE IF NOT EXISTS test.{table_name} (
        id INT AUTO_INCREMENT PRIMARY KEY,
        item_nbr INT,
        family VARCHAR(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci,
        class INT,
        perishable INT
    );
    """
    cursor.execute(create_table_query)
    connection.commit()
    print(f"Table {table_name} created successfully!")

    # Insert data
    for _, row in df.iterrows():
        insert_query = f"""INSERT INTO test.{table_name} (
            item_nbr,
            family,
            class,
            perishable
        ) VALUES (%s, %s, %s, %s, %s)"""
        print(row)
        cursor.execute(insert_query, (
            int(row.iloc[0]) if pd.notna(row.iloc[0]) else -1,
            str(row.iloc[1]) if pd.notna(row.iloc[1]) else "",
            int(row.iloc[2]) if pd.notna(row.iloc[2]) else -1,
            int(row.iloc[3]) if pd.notna(row.iloc[3]) else -1))

    connection.commit()

    print(f"Table {table_name} successfully loaded into MySQL.")


def load_transactions(cursor, connection, csv_file_path, table_name):
    row_id = 0
    # Read CSV file
    df = pd.read_csv(csv_file_path, encoding="utf-8", sep=",")

    # Replace NaN with None for MySQL
    df = df.where(pd.notna(df), None)

    drop_table_query = f"""
        DROP TABLE IF EXISTS test.{table_name};
    """
    cursor.execute(drop_table_query)
    connection.commit()
    print("Table deleted successfully!")

    # Create table if it doesn't exist (modify columns as needed)
    create_table_query = f"""
    CREATE TABLE IF NOT EXISTS test.{table_name} (
        id INT AUTO_INCREMENT PRIMARY KEY,
        item_nbr INT,
        family VARCHAR(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci,
        class INT,
        perishable INT
    );
    """
    cursor.execute(create_table_query)
    connection.commit()
    print(f"Table {table_name} created successfully!")

    # Insert data
    for _, row in df.iterrows():
        insert_query = f"""INSERT INTO test.{table_name} (
            item_nbr,
            family,
            class,
            perishable  
        ) VALUES (%s, %s, %s, %s, %s)"""
        print(row)
        cursor.execute(insert_query, (
            int(row.iloc[0]) if pd.notna(row.iloc[0]) else -1,
            str(row.iloc[1]) if pd.notna(row.iloc[1]) else "",
            int(row.iloc[2]) if pd.notna(row.iloc[2]) else -1,
            int(row.iloc[3]) if pd.notna(row.iloc[3]) else -1))

    connection.commit()

    print(f"Table {table_name} successfully loaded into MySQL.")


def load_sales(cursor, connection, csv_file_path, table_name):
    row_id = 0
    # Read CSV file
    df = pd.read_csv(csv_file_path, encoding="utf-8", sep=",")

    # Replace NaN with None for MySQL
    df = df.where(pd.notna(df), None)

    drop_table_query = f"""
        DROP TABLE IF EXISTS test.{table_name};
    """
    cursor.execute(drop_table_query)
    connection.commit()
    print("Table deleted successfully!")

    # Create table if it doesn't exist (modify columns as needed)
    create_table_query = f"""
    CREATE TABLE IF NOT EXISTS test.{table_name} (
        id INT AUTO_INCREMENT PRIMARY KEY,
        item_nbr INT,
        family VARCHAR(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci,
        class INT,
        perishable INT
    );
    """ 
    cursor.execute(create_table_query)
    connection.commit()
    print(f"Table {table_name} created successfully!")

    # Insert data
    for _, row in df.iterrows():
        insert_query = f"""INSERT INTO test.{table_name} (
            item_nbr,
            family,
            class,
            perishable
        ) VALUES (%s, %s, %s, %s, %s)"""
        print(row)  
        cursor.execute(insert_query, (
            int(row.iloc[0]) if pd.notna(row.iloc[0]) else -1,
            str(row.iloc[1]) if pd.notna(row.iloc[1]) else "",
            int(row.iloc[2]) if pd.notna(row.iloc[2]) else -1,
            int(row.iloc[3]) if pd.notna(row.iloc[3]) else -1))

    connection.commit()

    print(f"Table {table_name} successfully loaded into MySQL.")


def main():
    # Connect to MySQL
    connection = pymysql.connect(host=DB_HOST, port=DB_PORT, user=DB_USER, password=DB_PASSWORD)
    cursor = connection.cursor()
    create_database(cursor, connection)
    load_holidays_events(cursor, connection, CSV_FILE_PATH + "holidays_events_sample.csv", "holidays_events")
    load_items(cursor, connection, CSV_FILE_PATH + "items_sample.csv", "items")
    load_oil(cursor, connection, CSV_FILE_PATH + "oil_sample.csv", "oil")
    load_stores(cursor, connection, CSV_FILE_PATH + "stores_sample.csv", "stores")
    load_transactions(cursor, connection, CSV_FILE_PATH + "transactions_sample.csv", "transactions")
    load_sales(cursor, connection, CSV_FILE_PATH + "sales_sample.csv", "sales")
    
    print("All data loaded successfully!")

    cursor.close()
    connection.close()

if __name__ == "__main__":
    main()

