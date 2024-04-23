import os
import sqlalchemy
import pandas as pd
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy import MetaData


def CreateTable(engine, inspector, dtypes, TableName):
    metadata = MetaData()
    metadata.reflect(bind=engine)

    print(f"Creating {TableName}...")
    if not inspector.has_table(TableName):
        PrimaryColumns = []
        def PrimaryCheck(name, dtype):
            if isinstance(dtype, list):
                PrimaryColumns.append(name)
                return sqlalchemy.Column(name, dtype[0], primary_key=True)
            else:
                return sqlalchemy.Column(name, dtype)
        columns = [PrimaryCheck(name, dtype) for name, dtype in dtypes.items()]
        if len(PrimaryColumns) > 0:
            for column in PrimaryColumns:
                dtypes[column] = dtypes[column][0]

        my_table = sqlalchemy.Table(TableName, metadata, *columns)
        metadata.create_all(engine)
        print(f"{TableName} Created!")
    else:
        for key, _ in dtypes.items():
            if isinstance(dtypes[key], list):
                dtypes[key] = dtypes[key][0]
        print(f"{TableName} already exist")
    return dtypes

def LoadData(engine, DataFilePath, TableName, dtypes):
    if DataFilePath.endswith(".csv"):
        try:
            print("Reading Data...")
            Data = pd.read_csv(DataFilePath)
        except Exception as e:
            print(e)
            return
    else:
        raise Exception("File must be .csv!")

    print(f"Inserting Data to the table: {TableName}...")
    if not set(Data.columns) == set(dtypes.keys()):
        raise Exception(f"Columns in {TableName} are not the same as the dtypes setted!")
    Data.to_sql(TableName, engine, if_exists='replace', index=False, dtype=dtypes)
    print("Data Filled!")

def ConnectDataBase(ConnectData, dtypes, DatafolderPath, FileName=None, FillData=False):
    engine = None
    try:
        db_url = f"{ConnectData['sqltype']}://{ConnectData['user']}:{ConnectData['password']}@{ConnectData['host']}:{ConnectData['port']}/{ConnectData['dbname']}"
        engine = sqlalchemy.create_engine(db_url)
        print(f"{ConnectData['dbname']} is Connected!")
        inspector = sqlalchemy.inspect(engine)

        FileNames = os.listdir(DatafolderPath) if not FileName else (FileName if isinstance(FileName, list) else [FileName])
        for FileName in FileNames:
            TableName = FileName.split('.')[0]
            dtypes = CreateTable(engine, inspector, dtypes, TableName)
            if FillData:
                DataFilePath = DatafolderPath + FileName
                LoadData(engine, DataFilePath, TableName, dtypes)

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
    dtypes = {
        "product_id": [sqlalchemy.types.Integer()], # if u need to do a primary key make the value as a list
        "category_id": sqlalchemy.types.BigInteger(),
        "category_code": sqlalchemy.types.String(length=255),
        "brand": sqlalchemy.types.String(length=255)
    }
    DatafolderPath = os.path.dirname(os.path.abspath(__file__)) + "/../../data/subject/item/"
    ConnectDataBase(ConnectData, dtypes, DatafolderPath, FillData=True)
