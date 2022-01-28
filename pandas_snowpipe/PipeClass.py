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
pd.set_option('display.max_colwidth',None)
###########################################################################



class SnowPipe():

    def __init__(self,org,warehouse,usr,pwd,role,database,schema=None, cleanup=False):
        self.org            = org
        self.warehouse      = warehouse
        self.usr            = usr
        self.pwd            = pwd
        self.role           = role
        self.database       = database
        self.schema         = schema
        self.cleanup        = cleanup

    @property
    def Validate_Parms(self):
         return {'org'              : self.org,
                 'usr'              : self.usr,
                 'warehouse'        : self.warehouse,
                 'pwd'              : self.pwd,
                 'role'             : self.role,
                 'database'         : self.database,
                 'schema'           : self.schema,
                 }
    @property
    def Connection_Cursor(self) -> snowflake.connector.connect:
        try:
            ctx = snowflake.connector.connect(
                user        =self.usr,
                password    =self.pwd,
                account     =self.org
            )
            return ctx
        except Exception as e:
            print(f"connection to Snowflake failed \n Error received {e}:")

    def SnowPy(self,file_type: str):
        self.file_type = file_type
        if self.file_type not in ['csv','json','parquet']:
            print('invalid file type specified please use one of the following "csv","json","parquet"')
            exit(-1)

        if self.file_type == 'csv':

            self.dirname = '../Upload/csv/'
            self.full_path = os.path.abspath(self.dirname)
            self.dirfiles = os.listdir(self.dirname)
            self.fullpaths = map(lambda name: os.path.join(self.full_path,name), self.dirfiles)
            self.file_list = [file for file in self.fullpaths if os.path.isfile(file)]

            for file in self.file_list:

                self.file_name = str(os.path.basename(file))
                self.file_type = self.file_name.split('.')[1]
                if self.file_type != 'csv':

                    print('Error wrong file type added to this folder please add only csv files to this directory')
                    exit(-1)

                else:

                    self.table_name = re.sub("[^0-9a-zA-Z$]+","_",self.file_name.upper().split('.')[0])
                    self.df = pd.read_csv(file)
                    self.df.columns = self.df.columns.str.replace("[^0-9a-zA-Z$]+","_",regex=True)
                    self.df.columns = [c.upper() for c in self.df.columns]
                    self.df['LOAD_DATE'] =  datetime.datetime.strftime(datetime.datetime.now(),  '%Y/%m/%d %H:%M:%S %p')
                    self.df['FILE_NAME'] = os.path.basename(file)
                    #print(self.df.head(1))
                    self.ddl = pd.io.sql.get_schema(self.df,self.table_name).replace('"','')


                    self.table_cursor= self.Connection_Cursor.execute_string(f'''
                    USE ROLE {self.role};
                    USE DATABASE {self.database};
                    SHOW TABLES
                    ''',remove_comments=True)

                    self.df_table_list = pd.DataFrame(self.table_cursor[-1])
                    if self.df_table_list.empty:
                        self.df_table_list
                    else:
                        self.filter =  self.df_table_list[1] == self.table_name
                        self.df_table_list = self.df_table_list[self.filter]

                    if self.df_table_list.empty:

                        print(f'Table does not exist creating new table {self.table_name}')
                        self.create_table = self.Connection_Cursor.execute_string(f"""
                                            USE ROLE {self.role};
                                            USE DATABASE {self.database};
                                            USE SCHEMA {self.schema};
                                            {self.ddl};""")

                        for c in self.create_table:
                            for row in c:
                                print(row)


                        # generating insert statement guarantees data accuracy #

                        self.df['col']          = '("'+'","'.join([str(i) for i in self.df.columns.tolist()])+'")'
                        self.df['values']       = self.df.apply(lambda x: str(tuple(x)).replace(f", '{x['col']}'",'') ,axis =1 )
                        self.df['insert']       = self.df.apply(lambda x: f'INSERT INTO {self.database}.{self.schema}.{self.table_name} { x["col"]} VALUES {x["values"]};',axis =1)

                        self.insert_df = self.df['insert']
                        self.insert_statement = self.insert_df.to_string(index=False)

                        #print(self.insert_statement)
                        self.insert_into = self.Connection_Cursor.execute_string(f'''
                        USE ROLE {self.role};
                        USE WAREHOUSE {self.warehouse};
                        USE DATABASE {self.database};
                        USE SCHEMA {self.schema};
                        
                        {self.insert_statement}
                        ''')
                        for c in self.insert_into:
                            for row in c:
                                print(row)

                    else:

                        print(f'Table exists loading data into existing table {self.table_name}')

                        # generating insert statement guarantees data accuracy #
                        self.df['col'] = '("' + '","'.join([str(i) for i in self.df.columns.tolist()]) + '")'
                        self.df['values'] = self.df.apply(lambda x: str(tuple(x)).replace(f", '{x['col']}'", ''),axis=1)
                        self.df['insert'] = self.df.apply(lambda x: f'INSERT INTO {self.database}.{self.schema}.{self.table_name} {x["col"]} VALUES {x["values"]};', axis=1)

                        self.insert_df = self.df['insert']
                        self.insert_statement = self.insert_df.to_string(index=False)

                        #print(self.insert_statement)
                        self.insert_into = self.Connection_Cursor.execute_string(f'''
                        USE ROLE {self.role};
                        USE WAREHOUSE {self.warehouse};
                        USE DATABASE {self.database};
                        USE SCHEMA {self.schema};
                        {self.insert_statement}
                        ''')
                        for c in self.insert_into:
                            for row in c:
                                print(row)
                if self.cleanup == True  :
                    os.remove(file)

        ########################################################################################################################
        if self.file_type == 'json':

            self.dirname = '../Upload/json/'
            self.dirfiles = os.listdir(self.dirname)
            self.fullpaths = map(lambda name: os.path.join(self.full_path,name), self.dirfiles)
            self.file_list = [file for file in self.fullpaths if os.path.isfile(file)]
            for file in self.file_list:

                self.file_name = str(os.path.basename(file))
                self.file_type = self.file_name.split('.')[1]
                if self.file_type != 'json':

                    print('Error wrong file type added to this folder please add only csv files to this directory')
                    exit(-1)

                else:

                    self.df = pd.read_json(file)
                    self.df.columns = self.df.columns.str.replace("[^0-9a-zA-Z$]+", "_", regex=True)
                    self.df.columns = [c.upper() for c in self.df.columns]
                    self.df['LOAD_DATE'] = datetime.datetime.strftime(datetime.datetime.now(), '%Y/%m/%d %H:%M:%S %p')
                    self.df['FILE_NAME'] = os.path.basename(file)
                    # print(self.df.head(1))
                    self.ddl = pd.io.sql.get_schema(self.df, self.table_name).replace('"', '')

                    self.table_cursor = self.Connection_Cursor.execute_string(f'''
                                        USE ROLE {self.role};
                                        USE DATABASE {self.database};
                                        SHOW TABLES
                                        ''', remove_comments=True)

                    self.df_table_list = pd.DataFrame(self.table_cursor[-1])
                    if self.df_table_list.empty:
                        self.df_table_list
                    else:
                        self.filter = self.df_table_list[1] == self.table_name
                        self.df_table_list = self.df_table_list[self.filter]

                    if self.df_table_list.empty:

                        print(f'Table does not exist creating new table {self.table_name}')
                        self.create_table = self.Connection_Cursor.execute_string(f"""
                                                                USE ROLE {self.role};
                                                                USE DATABASE {self.database};
                                                                USE SCHEMA {self.schema};
                                                                {self.ddl};""")

                        for c in self.create_table:
                            for row in c:
                                print(row)

                        # generating insert statement guarantees data accuracy #

                        self.df['col'] = '("' + '","'.join([str(i) for i in self.df.columns.tolist()]) + '")'
                        self.df['values'] = self.df.apply(lambda x: str(tuple(x)).replace(f", '{x['col']}'", ''),
                                                          axis=1)
                        self.df['insert'] = self.df.apply(lambda
                                                              x: f'INSERT INTO {self.database}.{self.schema}.{self.table_name} {x["col"]} VALUES {x["values"]};',
                                                          axis=1)

                        self.insert_df = self.df['insert']
                        self.insert_statement = self.insert_df.to_string(index=False)


                        self.insert_into = self.Connection_Cursor.execute_string(f'''
                                            USE ROLE {self.role};
                                            USE WAREHOUSE {self.warehouse};
                                            USE DATABASE {self.database};
                                            USE SCHEMA {self.schema};

                                            {self.insert_statement}
                                            ''')
                        for c in self.insert_into:
                            for row in c:
                                print(row)

                    else:

                        print(f'Table exists loading data into existing table {self.table_name}')

                        # generating insert statement guarantees data accuracy #
                        self.df['col'] = '("' + '","'.join([str(i) for i in self.df.columns.tolist()]) + '")'
                        self.df['values'] = self.df.apply(lambda x: str(tuple(x)).replace(f", '{x['col']}'", ''),
                                                          axis=1)
                        self.df['insert'] = self.df.apply(lambda
                                                              x: f'INSERT INTO {self.database}.{self.schema}.{self.table_name} {x["col"]} VALUES {x["values"]};',
                                                          axis=1)

                        self.insert_df = self.df['insert']
                        self.insert_statement = self.insert_df.to_string(index=False)

                        # print(self.insert_statement)
                        self.insert_into = self.Connection_Cursor.execute_string(f'''
                                            USE ROLE {self.role};
                                            USE WAREHOUSE {self.warehouse};
                                            USE DATABASE {self.database};
                                            USE SCHEMA {self.schema};
                                            {self.insert_statement}
                                            ''')
                        for c in self.insert_into:
                            for row in c:
                                print(row)
                if self.cleanup == True  :
                    os.remove(file)

########################################################################################################################

        if self.file_type == 'parquet':
            self.full_path = os.path.abspath('../Upload/parquet/')
            self.dirname = '../Upload/csv/'
            self.dirfiles = os.listdir(self.dirname)
            self.fullpaths = map(lambda name : os.path.join(self.full_path,name), self.dirfiles)
            self.file_list = [file for file in self.fullpaths if os.path.isfile(file)]
            for file in self.file_list:
                self.file_name = str(os.path.basename(file))
                self.file_type = self.file_name.split('.')[1]
                if self.file_type != 'parquet':
                    print('Error wrong file type added to this folder please add only csv files to this directory')
                    exit(-1)
                else:
                    self.df = pd.read_parquet(file)
                    self.df.columns = self.df.columns.str.replace("[^0-9a-zA-Z$]+", "_", regex=True)
                    self.df.columns = [c.upper() for c in self.df.columns]
                    self.df['LOAD_DATE'] = datetime.datetime.strftime(datetime.datetime.now(), '%Y/%m/%d %H:%M:%S %p')
                    self.df['FILE_NAME'] = os.path.basename(file)
                    # print(self.df.head(1))
                    self.ddl = pd.io.sql.get_schema(self.df, self.table_name).replace('"', '')

                    self.table_cursor = self.Connection_Cursor.execute_string(f'''
                                        USE ROLE {self.role};
                                        USE DATABASE {self.database};
                                        SHOW TABLES
                                        ''', remove_comments=True)

                    self.df_table_list = pd.DataFrame(self.table_cursor[-1])
                    if self.df_table_list.empty:
                        self.df_table_list
                    else:
                        self.filter = self.df_table_list[1] == self.table_name
                        self.df_table_list = self.df_table_list[self.filter]

                    if self.df_table_list.empty:

                        print(f'Table does not exist creating new table {self.table_name}')
                        self.create_table = self.Connection_Cursor.execute_string(f"""
                                                                USE ROLE {self.role};
                                                                USE DATABASE {self.database};
                                                                USE SCHEMA {self.schema};
                                                                {self.ddl};""")

                        for c in self.create_table:
                            for row in c:
                                print(row)

                        # generating insert statement guarantees data accuracy #

                        self.df['col'] = '("' + '","'.join([str(i) for i in self.df.columns.tolist()]) + '")'
                        self.df['values'] = self.df.apply(lambda x: str(tuple(x)).replace(f", '{x['col']}'", ''),
                                                          axis=1)
                        self.df['insert'] = self.df.apply(lambda
                                                              x: f'INSERT INTO {self.database}.{self.schema}.{self.table_name} {x["col"]} VALUES {x["values"]};',
                                                          axis=1)

                        self.insert_df = self.df['insert']
                        self.insert_statement = self.insert_df.to_string(index=False)


                        self.insert_into = self.Connection_Cursor.execute_string(f'''
                                            USE ROLE {self.role};
                                            USE WAREHOUSE {self.warehouse};
                                            USE DATABASE {self.database};
                                            USE SCHEMA {self.schema};

                                            {self.insert_statement}
                                            ''')
                        for c in self.insert_into:
                            for row in c:
                                print(row)

                    else:

                        print(f'Table exists loading data into existing table {self.table_name}')

                        # generating insert statement guarantees data accuracy #
                        self.df['col'] = '("' + '","'.join([str(i) for i in self.df.columns.tolist()]) + '")'
                        self.df['values'] = self.df.apply(lambda x: str(tuple(x)).replace(f", '{x['col']}'", ''),
                                                          axis=1)
                        self.df['insert'] = self.df.apply(lambda
                                                              x: f'INSERT INTO {self.database}.{self.schema}.{self.table_name} {x["col"]} VALUES {x["values"]};',
                                                          axis=1)

                        self.insert_df = self.df['insert']
                        self.insert_statement = self.insert_df.to_string(index=False)

                        # print(self.insert_statement)
                        self.insert_into = self.Connection_Cursor.execute_string(f'''
                                            USE ROLE {self.role};
                                            USE WAREHOUSE {self.warehouse};
                                            USE DATABASE {self.database};
                                            USE SCHEMA {self.schema};
                                            {self.insert_statement}
                                            ''')
                        for c in self.insert_into:
                            for row in c:
                                print(row)
                if self.cleanup == True  :
                    os.remove(file)

SnowPipe(org='',usr='',pwd='',role="",database="",schema="",warehouse="",cleanup=False).SnowPy("csv")
