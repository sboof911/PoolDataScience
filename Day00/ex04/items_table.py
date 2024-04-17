import os
import sqlalchemy
import pandas as pd
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy import MetaData


def CreateTable(engine, inspector, TableDescription, TableName):
    metadata = MetaData()
    metadata.reflect(bind=engine)

    if not inspector.has_table(TableName):
        print(f"Creating {TableName}...")
        columnsData = TableDescription["columnsData"]
        columns = [sqlalchemy.Column(name, dtype) for name, dtype in columnsData.items()]
        my_table = sqlalchemy.Table(TableName, metadata, *columns)
        metadata.create_all(engine)
        print(f"{TableName} Created!")
    else:
        print(f"{TableName} already exist")

def LoadData(engine, DataFilePath, TableName):
    if DataFilePath.endswith(".csv"):
        try:
            Data = pd.read_csv(DataFilePath)
        except Exception as e:
            print(e)
            return
    else:
        raise Exception("File must be .csv!")
    
    print(f"Inserting Data to the table: {TableName}...")
    print(len(Data))
    # Data.to_sql(TableName, engine, if_exists='replace', index=False)
    print("Data Filled!")

def ConnectDataBase(ConnectData, TableDescription, DatafolderPath, FileName=None, FillData=False):
    engine = None
    try:
        db_url = f"{ConnectData['sqltype']}://{ConnectData['user']}:{ConnectData['password']}@{ConnectData['host']}:{ConnectData['port']}/{ConnectData['dbname']}"
        engine = sqlalchemy.create_engine(db_url)
        print(f"{ConnectData['dbname']} is Connected!")
        inspector = sqlalchemy.inspect(engine)

        FileNames = os.listdir(DatafolderPath) if not FileName else (FileName if isinstance(FileName, list) else [FileName])
        for FileName in FileNames:
            TableName = FileName.split('.')[0]
            CreateTable(engine, inspector, TableDescription, TableName)
            if FillData:
                DataFilePath = DatafolderPath + FileName
                LoadData(engine, DataFilePath, TableName)

    except SQLAlchemyError as e:
        print("An error occurred:", e)
    except Exception as e:
        print(e)

    finally:
        if engine:
            engine.dispose()
            print("PostgreSQL connection is closed")

if __name__ == "__main__":
    ConnectData = {
        'sqltype':"postgresql",
        'user':"amaach",
        'password':"mysecretpassword",
        'host':"localhost",
        'port':"5432",
        'dbname':"piscineds"
    }
    TableDescription = {
        "splitter":',',     #The character splitting ur Data
        "columnsData": {    #The Columns name and their DataType!
            "product_id": sqlalchemy.types.Integer(),
            "category_id": sqlalchemy.types.UUID(),
            "category_code": sqlalchemy.types.String(length=255),
            "brand": sqlalchemy.types.String(length=255)
        }
    }
    DatafolderPath = os.path.dirname(os.path.abspath(__file__)) + "/../subject/item/"
    # FileNames = ["data_2022_dec.csv", "data_2022_nov.csv", "data_2022_oct.csv", "data_2022_jan.csv"]
    ConnectDataBase(ConnectData, TableDescription, DatafolderPath, FillData=True)