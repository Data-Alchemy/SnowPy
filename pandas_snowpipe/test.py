import snowflake.connector
from snowflake.connector.pandas_tools import write_pandas
from snowflake.connector.pandas_tools import pd_writer
import pandas as pd
import os
import datetime
import re

###########################################################################
########################## Pandas Settings ################################
pd.set_option('display.max_rows', None)
pd.set_option('display.max_columns', None)
pd.set_option('display.width', None)
pd.set_option('display.max_colwidth', None)


###########################################################################


class SnowPipe():

    def __init__(self, org, warehouse, usr, pwd, role, database, schema=None, cleanup=False):
        self.org = org
        self.warehouse = warehouse
        self.usr = usr
        self.pwd = pwd
        self.role = role
        self.database = database
        self.schema = schema
        self.cleanup = cleanup

    @property
    def Validate_Parms(self):
        return {'org': self.org,
                'usr': self.usr,
                'warehouse': self.warehouse,
                'pwd': self.pwd,
                'role': self.role,
                'database': self.database,
                'schema': self.schema,
                }

    @property
    def Connection_Cursor(self) -> snowflake.connector.connect:
        try:
            ctx = snowflake.connector.connect(
                user=self.usr,
                password=self.pwd,
                account=self.org
            )
            return ctx
        except Exception as e:
            print(f"connection to Snowflake failed \n Error received {e}:")

    def SnowPy(self, file_type: str):
        self.file_type = file_type
        if self.file_type not in ['csv', 'json', 'parquet']:
            print('invalid file type specified please use one of the following "csv","json","parquet"')
            exit(-1)

        if self.file_type == 'csv':

            self.dirname = '../Upload/csv/'
            self.full_path = os.path.abspath(self.dirname)
            self.dirfiles = os.listdir(self.dirname)
            self.fullpaths = map(lambda name: os.path.join(self.full_path, name), self.dirfiles)
            self.file_list = [file for file in self.fullpaths if os.path.isfile(file)]

            for file in self.file_list:

                self.file_name = str(os.path.basename(file))
                self.file_type = self.file_name.split('.')[1]
                if self.file_type != 'csv':

                    print('Error wrong file type added to this folder please add only csv files to this directory')
                    exit(-1)

                else:

                    self.table_name = re.sub("[^0-9a-zA-Z$]+", "_", self.file_name.upper().split('.')[0])
                    self.df = pd.read_csv(file)
                    self.df.columns = self.df.columns.str.replace("[^0-9a-zA-Z$]+", "_", regex=True)
                    self.df.columns = [c.upper() for c in self.df.columns]
                    self.df['LOAD_DATE'] = datetime.datetime.strftime(datetime.datetime.now(), '%Y/%m/%d %H:%M:%S %p')
                    self.df['FILE_NAME'] = os.path.basename(file)
                    # print(self.df.head(1))
                    self.ddl = pd.io.sql.get_schema(self.df, self.table_name).replace('"', '')


                    self.table_cursor= self.Connection_Cursor.execute_string(f'''
                    USE ROLE {self.role};
                    USE DATABASE {self.database};
                    SHOW TABLES
                    ''',remove_comments=True)

                    self.df_table_list = pd.DataFrame(self.table_cursor[-1])

                if self.cleanup == True:
                    os.remove(file)


SnowPipe(org='xoa77688', usr='ZM_PROD_DEVOPS_ADMIN', pwd='7bda50233add907d18e2a2e866c073a1',
         role="ACCOUNTADMIN", database="PROD_BRONZE_DB", schema="SANDBOX", warehouse="PROD_ELT_WH",
         cleanup=False).SnowPy("csv")
