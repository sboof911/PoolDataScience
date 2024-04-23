import os
import sqlalchemy
import pandas as pd
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy import MetaData
from typing import Literal
import pickle


class Sqlmanup():
    _engine:sqlalchemy.Engine
    _inspector:sqlalchemy.Inspector
    _metadata:sqlalchemy.MetaData

    def __init__(self, ConnectData:dict, dtypes:dict = {},
                    folderPath:str = "") -> None:

        self._ConnectData = ConnectData
        self._dtypes = dtypes
        self._folderPath = folderPath
        self.Connect()

    @property
    def folderPath(self):
        return self._folderPath

    def disconnect(self):
        if self._engine:
            self._engine.dispose()
            print("PostgreSQL connection is closed")

    def Connect(self):
        db_url = f"{self._ConnectData['sqltype']}://{self._ConnectData['user']}:{self._ConnectData['password']}@{self._ConnectData['host']}:{self._ConnectData['port']}/{self._ConnectData['dbname']}"
        self._engine = sqlalchemy.create_engine(db_url)
        print(f"{self._ConnectData['dbname']} is Connected!")
        self._metadata = MetaData()
        self._metadata.reflect(bind=self._engine)
        self._inspector = sqlalchemy.inspect(self._engine)

    def CreateTable(self, TableName) -> None:
        print(f"Creating {TableName}...")
        if not self._inspector.has_table(TableName):
            PrimaryColumns = []
            def PrimaryCheck(name, dtype):
                if isinstance(dtype, list):
                    PrimaryColumns.append(name)
                    return sqlalchemy.Column(name, dtype[0], primary_key=True)
                else:
                    return sqlalchemy.Column(name, dtype)
            if not self._dtypes:
                raise Exception("Creating of columns need dtypes!")
            columns = [PrimaryCheck(name, dtype) for name, dtype in self._dtypes.items()]
            if len(PrimaryColumns) > 0:
                for column in PrimaryColumns:
                    self._dtypes[column] = self._dtypes[column][0]

            _ = sqlalchemy.Table(TableName, self._metadata, *columns)
            self._metadata.create_all(self._engine)
            print(f"{TableName} Created!")
        else:
            if self._dtypes:
                for key, _ in self._dtypes.items():
                    if isinstance(self._dtypes[key], list):
                        self._dtypes[key] = self._dtypes[key][0]
            print(f"{TableName} already exist")

    def LoadData(self, TableName, Dataframe:pd.DataFrame = None, filename = "", if_exists: Literal["fail", "replace", "append"] = 'fail'):
        if filename:
            if not self._folderPath:
                raise Exception("You need to provide the folder path!")
            if not filename.endswith(".csv"):
                raise Exception("Unfortunately we can work with just the csv files for the moment... wait for the update lol")
            DataFilePath = self._folderPath + filename
            if DataFilePath.endswith(".csv"):
                try:
                    print("Reading Data...")
                    Data = pd.read_csv(DataFilePath)
                except Exception as e:
                    print(e)
                    return
            else:
                raise Exception("File must be .csv!")
        elif Dataframe is not None:
            Data = Dataframe
        else:
            raise Exception("Provide filename or Dataframe!")

        print(f"Inserting Data to the table: {TableName}...")
        if not set(Data.columns) == set(self._dtypes.keys()):
            print(f"Columns in {TableName} are not the same as the dtypes setted!")
            return
        Data.to_sql(TableName, self._engine, if_exists=if_exists, index=False, dtype=self._dtypes)
        print("Data Filled!")

    def JoinTablesInOne(self, TablesToJoin, NewTableName):
        import re

        def GetMatchingTables(TablesToJoin):
            MatchingTables = []
            if isinstance(TablesToJoin, str):
                TablesToJoin = [TablesToJoin]
            for TableName in TablesToJoin:
                TableNames = self._inspector.get_table_names()
                pattern_str = re.sub(r'\*', r'.', TableName)
                pattern = re.compile(pattern_str)
                MatchingTables += [table_name for table_name in TableNames if pattern.match(table_name)]
            MatchingTables = set(MatchingTables)
            if not MatchingTables.__len__() > 1:
                raise Exception("Tables to join must be more then One!")
            return list(MatchingTables)

        MatchingTables = GetMatchingTables(TablesToJoin)
        query = f"CREATE TABLE IF NOT EXISTS {NewTableName} AS ("

        for i, SrcTablename in enumerate(MatchingTables):
            if i > 0:
                query += " UNION ALL "
            query += f"SELECT * FROM {SrcTablename}"

        query += ")"
        with self._engine.connect() as connection:
            print("Excecuting the union script!")
            connection.execute(sqlalchemy.text(query))
            print("Commitig the DataLoaded!")
            connection.commit()
        print("All TableData are joined.")

    def DeleteTable(self, TableName):
        print(f"Deleting {TableName}...")
        if self._inspector.has_table(TableName):
            table = sqlalchemy.Table(TableName, self._metadata, autoload=True, autoload_with=self._engine)

            # Drop the reflected table
            table.drop(self._engine)
            print(f"{TableName} Deleted!")
        else:
            print(f"{TableName} not found!")

    def RemoveDuplicates(self, TableName):
        tmpTableName = "tmp_" + TableName
        query = f"""
                CREATE TEMPORARY TABLE {tmpTableName} AS SELECT DISTINCT * FROM {TableName};
                TRUNCATE {TableName};
                INSERT INTO {TableName} SELECT * FROM {tmpTableName};
                """
        with self._engine.connect() as connection:
            print("Creating a tmpTable and removing duplicates...")
            connection.execute(sqlalchemy.text(query))
            print("Commitig the DataLoaded!")
            connection.commit()


############################################################################################################################################################

def tasks(sql: Sqlmanup):
    # The tasks that should be Done
    sql.RemoveDuplicates("customers")

if __name__ == "__main__":
    kwargs = {
        'ConnectData' : { #the data to connect to the DataBase
            'sqltype':"postgresql",
            'user':"amaach",
            'password':"mysecretpassword",
            'host':"localhost",
            'port':"5432",
            'dbname':"piscineds"
        },
        'dtypes' : { #Datatypes of each column of the DataBase
            "event_time": sqlalchemy.DateTime(),
            "event_type": sqlalchemy.types.String(length=255),
            "product_id": sqlalchemy.types.Integer(),
            "price": sqlalchemy.types.Float(),
            "user_id": sqlalchemy.types.BigInteger(),
            "user_session": sqlalchemy.types.UUID(as_uuid=True)
        },
        'folderPath' : os.path.dirname(os.path.abspath(__file__)) + "/../../data/subject/customer/" #PathtoDataToBeFilled
    }

    sql = None
    try:
        sql = Sqlmanup(**kwargs)
        tasks(sql)
    except SQLAlchemyError as e:
        print("An error occurred:", e)
    except Exception as e:
        print(e)
    finally:
        if sql:
            sql.disconnect()
            