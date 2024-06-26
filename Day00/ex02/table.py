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

def ConnectDataBase(ConnectData, dtypes, folderPath, FileName=None, FillData=False):
    try:
        db_url = f"{ConnectData['sqltype']}://{ConnectData['user']}:{ConnectData['password']}@{ConnectData['host']}:{ConnectData['port']}/{ConnectData['dbname']}"
        engine = sqlalchemy.create_engine(db_url)
        print(f"{ConnectData['dbname']} is Connected!")
        inspector = sqlalchemy.inspect(engine)

        FileNames = os.listdir(folderPath) if not FileName else (FileName if isinstance(FileName, list) else [FileName])
        for FileName in FileNames:
            dtypes = CreateTable(engine, inspector, dtypes, FileName)

    except SQLAlchemyError as e:
        print("An error occurred:", e)
    except Exception as e:
        print(e)

    finally:
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
        "event_time": sqlalchemy.DateTime(),
        "event_type": sqlalchemy.types.String(length=255),
        "product_id": sqlalchemy.types.Integer(),
        "price": sqlalchemy.types.Float(),
        "user_id": sqlalchemy.types.BigInteger(),
        "user_session": sqlalchemy.types.UUID()
    }
    CurrentDirectory = os.path.dirname(os.path.abspath(__file__)) + "/../subject/customer"
    FileNames = ["data_2022_dec.csv", "data_2022_nov.csv", "data_2022_oct.csv", "data_2023_jan.csv"]
    ConnectDataBase(ConnectData, dtypes, CurrentDirectory, FileName=FileNames, FillData=False)
