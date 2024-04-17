import psycopg2
from psycopg2 import Error
import os

def CreateTable(cursor, filename, folderPath, TableDesription, fillData = True):
    table_name = filename.split('.')[0]
    columnsData = TableDesription["columnsData"]

    with open(folderPath + '/' + filename, 'r') as file:
        lines = file.readlines()

        columns = [column.strip() for column in lines[0].strip().split(TableDesription["splitter"])]
        print(f"Creating table {table_name}...")
        create_table_query = f'CREATE TABLE IF NOT EXISTS {table_name} ({", ".join([column + " " + columnsData[column] for column in columns])});'
        cursor.execute(create_table_query)
        if fillData:
            for line in lines[1:]:
                values = line.strip().split(TableDesription["splitter"])
                insert_query = f'INSERT INTO {table_name} ({", ".join(columns)}) VALUES ({", ".join(["%s" for _ in values])});'
                cursor.execute(insert_query, values)
            print("Data Filled!")

def CreateFillDataBase(connectData, TableDesription, folderPath, FileName = None, fillData = True):
    FileNames = os.listdir(folderPath) if not FileName else (FileName if isinstance(FileName, list) else [FileName])
    try:
        connection = psycopg2.connect(**connectData)
        cursor = connection.cursor()
        for filename in FileNames:
            CreateTable(cursor, filename, folderPath, TableDesription, fillData)
            connection.commit()

    except (Exception, Error) as error:
        print("Error while in PostgreSQL:", error)

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
        "splitter":',',     #The character splitting ur Data
        "columnsData": {    #The Columns name and their DataType!
            "event_time": "TIMESTAMP",
            "event_type": "VARCHAR",
            "product_id": "INTEGER",
            "price": "FLOAT",
            "user_id": "UUID",
            "user_session": "VARCHAR"
        }
    }
    current_directory = os.path.dirname(os.path.abspath(__file__)) + "/../subject/customer"
    # FillNames = ["data_2022_dec.csv", "data_2022_nov.csv", "data_2022_oct.csv", "data_2022_jan.csv"]
    CreateFillDataBase(connectData, TableDesription, current_directory, fillData=False)
