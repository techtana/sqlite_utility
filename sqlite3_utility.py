"""
Utility for using sqlite3 built on top of sqlite3
"""

import sqlite3
import contextlib
import json
import pandas as pd
import pickle

def replace_specialchars2underscore(string):
    import re
    string = re.sub("-| ", "_", str(string))
#     string = string.encode('latin1')
    return string

def replace_single2doublequote(string):
    import re
    return re.sub(r"\'|'", "\"", str(string))

def sqlite3_safe_execute(db_path:str, statement:str, fetch:bool=False):
    """
    execute statement with auto-commit and connection/cursor auto-close
    
    Return:
    cursor: sqlite3.conn.cursor
    """
    with contextlib.closing(sqlite3.connect(db_path)) as conn: # auto-closes
        with conn: # auto-commits
            with contextlib.closing(conn.cursor()) as cursor: # auto-closes
                cur = cursor.execute(statement)
                if fetch==True:
                    return cur.fetchall()

def sqlite3_safe_execute_binary(db_path:str, statement:str, binary_vals:tuple, fetch:bool=False):
    """
    execute statement with auto-commit and connection/cursor auto-close
    
    Return:
    cursor: sqlite3.conn.cursor
    """
    with contextlib.closing(sqlite3.connect(db_path)) as conn: # auto-closes
        with conn: # auto-commits
            with contextlib.closing(conn.cursor()) as cursor: # auto-closes
                cur = cursor.execute(statement, binary_vals)
                if fetch==True:
                    return cur.fetchall()

def binary_encode(data):
    """ default binary encoder
    """
    return pickle.dumps(data, 0)
def binary_decode(data):
    """ default binary decoder
    """
    return pickle.loads(data)
            
def sqlite3_get_tableinfo(db_path:str, table:str):
    """
    Get schema info from sqlite3 table
    
    Returns
    -------
    column_indices: list of int
    column_names: list of string
    column_type: list of string
    column_notnull: list of int or bool
    column_fk: None or list of string  # TODO: check if this is actually a foreign key
    column_pk: list of int or bool
    
    TODO: there should be a check of data type. Link to data types https://www.sqlite.org/datatype3.html
    """
    statement = f"PRAGMA table_info('{table}')"
    table_info = sqlite3_safe_execute(db_path, statement, fetch=True)
    table_info_transpose = tuple(map(list, zip(*table_info)))
    column_indices = table_info_transpose[0]
    column_names = table_info_transpose[1]
    column_type = table_info_transpose[2]
    column_notnull = table_info_transpose[3]
    column_fk = table_info_transpose[4]  # TODO: check if this is actually a foreign key
    column_pk = table_info_transpose[5]
    
    return column_indices, column_names, column_type, column_notnull, column_pk

def sqlite3_get_primarykeys(db_path, table):
    """
    Get list of primary keys in order
    """
    column_indices, column_names, column_type, column_notnull, column_pk = sqlite3_get_tableinfo(db_path, new_table)
    PK = [col[0] for col in sorted(zip(column_names,column_pk), key = lambda x: x[1]) if col[1]>0]
    if PK:
        return PK
    else:
        print("This table does not have primary key, return `rowid` instead e.g. `SELECT rowid FROM table_name;`") 
        return ["rowid"]

def sqlite3_get_notnull(db_path, table):
    """
    Get list of not-null columns in order
    """
    column_indices, column_names, column_type, column_notnull, column_pk = sqlite3_get_tableinfo(db_path, new_table)
    notnull = [col[0] for col in sorted(zip(column_names,column_notnull), key = lambda x: x[1]) if col[1]>0]
    if notnull:
        return notnull
    else:
        print("This table does not have Not-Null columns.") 
        return None

def sqlite3_add_record(db_path:str, table:str, column_vals:list):
    """
    add new records in sqlite3 table
    """
    column_indices, column_names, column_type, column_notnull, column_pk = sqlite3_get_tableinfo(db_path, table)
    try:
        if not len(column_names) == len(column_vals):
            raise ValueError(f"Unexpected length for column_vals; expect {len(column_names)}: {column_names}")  
            
        column_names_str = ','.join([str(name) for name in column_names])
        column_vals_str = ','.join([str(val) for val in column_vals])
        statement = f"INSERT INTO {table} ({column_names_str}) VALUES ({column_vals_str})"
        cursor = sqlite3_safe_execute(db_path, statement)
    except Exception as e:
        error_message = f"{type(e).__name__}: {str(e)}"
        print("Failed to ADD record to table. If Integrity error is found, "
              "this could be caused by adding a record with duplicated primary key, "
              "or the table does not have adequate primary key structure, "
              "or the incoming data is not valid e.g. for binary data, use `sqlite3_add_record_binary`.")
        print(error_message)
        
def sqlite3_add_record_binary(db_path:str, table:str, add_keys:dict):
    """
    add new records in sqlite3 table (BINARY data)
    """
    _, table_cols, _, _, _ = sqlite3_get_tableinfo(db_path, table)
    table_notnull_cols = sqlite3_get_notnull(db_path, table)
    try:
        add_cols = list(add_keys)
        add_vals = [add_keys[key] for key in add_cols]
        
        if set(table_notnull_cols)-set(add_cols):
            raise ValueError(f"some not-null columns was not given value in add_cols")  
                    
        add_cols_str = ','.join([str(col) for col in add_cols])
        add_vals_kw = ','.join(['?' for col in add_cols])
        add_vals_bin = tuple([sqlite3.Binary(data) for data in add_vals])
        statement = f'''INSERT INTO {table} ({add_cols_str}) VALUES ({add_vals_kw})'''
        sqlite3_safe_execute_binary(db_path, statement, add_vals_bin)
        
    except Exception as e:
        error_message = f"{type(e).__name__}: {str(e)}"
        print(statement)
        print("Failed to ADD record to table with sqlite3_add_record_binary.")
        raise e
                
def sqlite3_update_record_binary(db_path:str, table:str, primary_keys:dict, update_keys:dict):     
    """
    update records in sqlite3 table
    """
    _, table_cols, _, _, _ = sqlite3_get_tableinfo(db_path, table)
    table_pk_cols = sqlite3_get_primarykeys(db_path, table)
    
    try:
        pk_cols = list(primary_keys)
        pk_vals = [primary_keys[key] for key in pk_cols]
        update_cols = list(update_keys)
        update_vals = [update_keys[col] for col in update_cols]
        
        if not sorted(table_pk_cols) == sorted(pk_cols):
            raise ValueError(f"pk_cols must be the same as table_pk; expect {table_pk_cols} given {pk_cols}")
        if set(update_cols)-set(table_cols):
            raise ValueError(f"one or more cols in update_cols does not exist in table_cols")
    
        update_vals_kw = ",".join([f"{str(col)} = ?" for col in update_cols])
        update_vals_bin = tuple([sqlite3.Binary(binary_encode(data)) for data in update_vals])
        pk_vals_kw = ",".join([f"{str(col)} = ?" for col in pk_cols])
        pk_vals_bin = tuple([sqlite3.Binary(binary_encode(data)) for data in pk_vals])
        
        statement = f'''UPDATE {table} SET {update_vals_kw} WHERE {pk_vals_kw}'''
        
        cursor = sqlite3_safe_execute_binary(db_path, statement, (update_vals_bin+pk_vals_bin))
    except Exception as e:
        error_message = f"{type(e).__name__}: {str(e)}"
        print(statement)
        print((update_vals_bin+pk_vals_bin))
        print("Failed to UPDATE record to table with sqlite3_update_record_binary.")
        raise e
        
def sqlite3_parse_datatypes(dtype):
    """
    PARSE column_types into valid SQLITE DATA TYPE
    
    ONLY SUPPORT: TEXT, NUM, INT, REAL, "" (BLOB: empty string)
    (2021-05-05) all TEXT will now become a BLOB. This is to prevent invalid quotation characters. 
                 all string will be processed by binary_decode(data) to prevent this issue.
    DATA TYPE documentation at https://sqlite.org/lang_createtable.html
    """
    if not type(dtype) == type:
        raise ValueError("Provided dtype is not a python type object.")
        
    if dtype == int:
        return "INT"
    elif dtype == float:
        return "REAL"
    return ""

def sqlite3_import_json2table(db_path:str, new_table:str, json_list:list, PK_list:list=None):     
    """
    import JSON into sqlite3 database as new table
    expected a structure of List of Dict i.e. [{ "key_1":"value_1", "key_2":"value_2",... },...]
    TABLE's Column Name and Data Type is defined by the first layer of dict keys. 
    
    """
    if not PK_list:
        PK_list = ["rowid"]
        json_list = [dict(item, **{"rowid":_index}) for _index,item in enumerate(json_list)]
            
    # validate json_list format : list of dict
    if not all([True if isinstance(item,dict) else False for item in json_list]):
        raise ValueError("json_list is not a valid list of dict")
    
    column_names = list(json_list[0].keys())
    column_types = [type(json_list[0][key]) for key in column_names]
    column_types_parsed = [sqlite3_parse_datatypes(dtype) for dtype in column_types]
    column_notnull = ["NOT NULL" if col in PK_list else "" for col in column_names] 

    # validate PK_list : must exists in JSON dict keys
    if not all([True if col in column_names else False for col in PK_list]):
        raise ValueError("some items in PK_list does not exist as dict keys in json_list")
    
    # create a new table
    column_names_parsed = [replace_specialchars2underscore(col) for col in column_names]
    column_definition = [" ".join(line) for line in list(zip(column_names_parsed, column_types_parsed, column_notnull))]
    statement = f""" CREATE TABLE {new_table}({",".join(column_definition)},PRIMARY KEY ({','.join(PK_list)})) """
    print(statement)
    cursor = sqlite3_safe_execute(db_path, statement)
    print("New table added to database")  
    
    try:
        # import JSON content to database table
        for item in json_list:
            columns_vals = [item[key] for key in column_names]
            columns_vals_zip = list(zip(columns_vals, column_types_parsed))
            # Convert data object that is not supported by SQLITE to string `binary_encode()`
            # They can be parsed back using `binary_decode()` 
            columns_vals_converted = [f"'{binary_encode(pair[0])}'" if pair[1]=="" 
                                      else pair[0] 
                                      for pair in columns_vals_zip]
            sqlite3_add_record(db_path, new_table, columns_vals_converted)

        print("JSON data imported into SQLITE table")
        
    except Exception as e:
        # drop the new table if adding record fails
        error_message = f"{type(e).__name__}: {str(e)}"
        statement = f""" DROP TABLE {new_table} """
        cursor = sqlite3_safe_execute(db_path, statement)

def sqlite3_import_pandas2table(db_path:str, new_table:str, pandas_df:pd.core.frame.DataFrame, PK_list:list=None):     
    """
    import Pandas DF into sqlite3 database as new table
    TABLE's Column Name and Data Type is defined by the pandas columns
    
    """
    if not PK_list:
        PK_list = ["rowid"]
        pandas_df["rowid"] = pandas_df.index.to_list()
        
    # validate pandas_df format : list of dict
    if not type(pandas_df) == pd.core.frame.DataFrame:
        raise ValueError("pandas_df is not a valid pd.core.frame.DataFrame")
    
    column_names = list(pandas_df.columns)
    # DEFAULT column_types to BLOB or empty string
    column_types_parsed = ["" for dtype in column_names]
    column_notnull = ["NOT NULL" if col in PK_list else "" for col in column_names]

    # validate PK_list : must exists in JSON dict keys
    if not all([True if col in column_names else False for col in PK_list]):
        raise ValueError("some items in PK_list does not exist in column_names")
    
    # create a new table
    column_names_parsed = [replace_specialchars2underscore(key) for key in column_names]
    column_definition = [" ".join(line) for line in list(zip(column_names_parsed, column_types_parsed, column_notnull))]
    if PK_list:
        statement = f""" CREATE TABLE {new_table}({",".join(column_definition)},PRIMARY KEY ({','.join(PK_list)})) """
    else:
        statement = f""" CREATE TABLE {new_table}({",".join(column_definition)}) """
    print(statement)
    cursor = sqlite3_safe_execute(db_path, statement)
    print("New table added to database")  
    
    try:
        # import JSON content to database table
        for _index,item in pandas_df.iterrows():
            columns_vals_bin = {replace_specialchars2underscore(key):binary_encode(item[key]) for key in column_names}
            sqlite3_add_record_binary(db_path, new_table, columns_vals_bin)
            
        print("JSON data imported into SQLITE table")
        
    except Exception as e:
        # drop the new table if adding record fails
        error_message = f"{type(e).__name__}: {str(e)}"
        print(error_message)
        statement = f""" DROP TABLE {new_table} """
        cursor = sqlite3_safe_execute(db_path, statement)

def sqlite3_parse_table2dataframe(db_path, table):
    """
    Convert TEXT->str, INT->int, REAL->float, ''->binary_encode(data)
    This is assuming that all BLOBS are text string converted by binary_encode().
    binary_decode() will convert these objects back to original data objects.
    """
    import pandas as pd
    with contextlib.closing(sqlite3.connect(db_path)) as conn: # auto-closes
        df = pd.read_sql(f"SELECT * from {table}", conn)
    
    # get data type
    column_indices, column_names, column_type, column_notnull, column_pk = sqlite3_get_tableinfo(db_path, table)
    column_definition_zip = list(zip(column_names, column_type))
    
    # parse data type in each column
    for item in column_definition_zip:
        if item[1]=="":
            df[item[0]] = df[item[0]].apply(lambda x: binary_decode(x))
        elif item[1]=="TEXT":
            df[item[0]] = df[item[0]].apply(lambda x: str(x))
        elif item[1]=="INT":
            df[item[0]] = df[item[0]].apply(lambda x: int(x))
        elif item[1]=="REAL":
            df[item[0]] = df[item[0]].apply(lambda x: float(x))
    return df
