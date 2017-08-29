import pandas as pd
import sqlite3 

class SqlLite2Pandas:
    def __init__(self, db_path):
        self.db_path = db_path 
        
    def connect_sqllite(self):
        """Establish connection between Python and sqllite databatse and returns a cursor 
        for execution.
        """
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        return cursor
    
    def fetch_data(self, sql_command):
        """Excecutes the sqllite commands after a cursor with an established cursor. 
        : param cursor: An established cursor object between Python and sqllite. 
        : param sql_command: The sqllite command to be executed.
        
        """
        cursor = self.connect_sqllite()
        cursor.execute(sql_command)
        results = cursor.fetchall() 
        return results 
    
    def show_columns_details(self, table_name):
        """Show all tables name in the database. 
        : param table_name: Table name. 
        """
        cursor = self.connect_sqllite()
        
        sql_comment = "PRAGMA table_info({table_name})".format(table_name=table_name)
        cursor.execute(sql_comment) 
        tb_cols_data = cursor.fetchall()
        
        def match_data_with_colsname(data):
            ordered_cols_name = ['column_id','column_name','datatype','nullable','default_value','primary_key']
            list_ = []
            for data in tb_cols_data:
                dict_ = {}
                for index, ele in enumerate(data):
                    index_name = ordered_cols_name[index] 
                    if index_name in ['nullable', 'primary_key']:
                        dict_[index_name] = True if ele == 1 else False     
                    else:
                        dict_[index_name] = ele
                list_.append(dict_)      
            return list_
                
        return match_data_with_colsname(tb_cols_data)  
    
    def show_tables(self):
        """Show all table names available in the connected database.
        """ 
        sql_command = ("SELECT name FROM sqlite_master "
                       "WHERE type='table' "
                       "ORDER BY name;") 
        all_tables = self.fetch_data(sql_command)    
        all_tables = [tb_name[0] for tb_name in all_tables]
        return all_tables   
    
    def to_pandas(self, table_name, limit=None):
        """Convert sqllite data to Python pandas's dataframe. 
        : param table_name: Table name to convert to pandas
        : param limit: The number of record to return. (Set a small limit number for viewing purpose)
        """
        data_dict = self.show_columns_details(table_name)
        column_names = [i['column_name'] for i in data_dict] 
        
        limit_string = "" if limit is None else "limit {0}".format(limit) 
        data = self.fetch_data("select * from {0} {1}".format(table_name, limit_string))
        
        df_pandas = pd.DataFrame(data=data, columns=column_names)
        return df_pandas  