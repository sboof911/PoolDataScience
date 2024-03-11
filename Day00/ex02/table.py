import psycopg2
from psycopg2 import Error
import os

def CreateTable(cursor, filename, folderPath, TableDesription):
    table_name = filename.split('.')[0]
    columnsData = TableDesription["columnsData"]

    with open(folderPath + '/' + filename, 'r') as file:
        lines = file.readlines()

        columns = [column.strip() for column in lines[0].strip().split(TableDesription["splitter"])]
        create_table_query = f'CREATE TABLE IF NOT EXISTS {table_name} ({", ".join([column + " " + columnsData[column] for column in columns])});'
        cursor.execute(create_table_query)
        print(f"Creating table {table_name}...")

        for line in lines[1:]:
            values = line.strip().split(TableDesription["splitter"])
            insert_query = f'INSERT INTO {table_name} ({", ".join(columns)}) VALUES ({", ".join(["%s" for _ in values])});'
            cursor.execute(insert_query, values)
        print("Data Filled!")

def CreateAndFillDataBase(connectData, folderPath, TableDesription):
    FileNames = os.listdir(folderPath)
    try:
        connection = psycopg2.connect(**connectData)
        cursor = connection.cursor()
        for filename in FileNames:
            CreateTable(cursor, filename, folderPath, TableDesription)
            connection.commit()

    except (Exception, Error) as error:
        print("Error while connecting to PostgreSQL:", error)

    finally:
        if connection:
            cursor.close()
            connection.close()
            print("PostgreSQL connection is closed")

if __name__ == "__main__":
    connectData = {
        "user":"amaach",
        "password":"mysecretpassword",
        "host":"localhost",
        "port":"5432",
        "dbname":"piscineds"
    }
    TableDesription = {
        "splitter":',',     #The caracter splitting ur Data
        "columnsData": {    #The Columns name and their DataType!
            "event_time":"TIMESTAMP",
            "event_type":"VARCHAR(255)",
            "product_id":"INTEGER",
            "price":"NUMERIC",
            "user_id":"INTEGER",
            "user_session":"VARCHAR(255)"
        }
    }
    folderPath = "/home/sboof/Desktop/PoolData/Day00/subject/customer"
    CreateAndFillDataBase(connectData, folderPath, TableDesription)