"""
Utility for using sqlite3 built on top of sqlite3
"""
import sqlite3
import contextlib
import json


def replace_specialchars2underscore(string):
    import re
    return re.sub("'|-| ", "_", str(string))

def replace_single2doublequote(string):
    import re
    return re.sub(r"\'|'", "", str(string))


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
                else:
                    # you cannot operate on closed cursor.
                    # this return is not useful
                    return cur
            
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

def sqlite3_add_record(db_path:str, table:str, column_vals:list):
    """
    add new records in sqlite3 table
    """
    column_indices, column_names, column_type, column_notnull, column_pk = sqlite3_get_tableinfo(db_path, table)
    if not len(column_names) == len(column_vals):
        raise ValueError(f"Unexpected length for column_vals; expect {len(column_names)}: {column_names}")    
    clear_output()
    column_names_str = ','.join([str(name) for name in column_names])
    column_vals_str = ','.join([str(val) for val in column_vals])
    statement = f"INSERT INTO {table} ({column_names_str}) VALUES ({column_vals_str})"
    print(statement)
    cursor = sqlite3_safe_execute(db_path, statement)
                
def sqlite3_update_record(db_path:str, table:str, primary_keys:list, update_vals:list):     
    """
    update records in sqlite3 table
    """
    column_indices, column_names, column_type, column_notnull, column_pk = sqlite3_get_tableinfo(db_path, table)
    if not len(column_pk) == len(primary_keys):
        raise ValueError(f"Unexpected length for columns_vals; expect {len(column_names)}: {column_names}")
    if not len(column_names) == len(primary_keys+update_vals):
        raise ValueError(f"Unexpected length for columns_vals; expect {len(column_names)}: {column_names}")
        
    statement = f"""
    UPDATE {table}
    SET key1 = 'some value',
        key2 = 'some value',
        key3 = 'some value',
        key4 = 'some value',
    WHERE primary_key1 = 'some value',
          primary_key2 = 'some value',
          primary_key3 = 'some value';
    """
    cursor = sqlite3_safe_execute(db_path, statement)

def sqlite3_parse_datatypes(dtype):
    """
    PARSE column_types into valid SQLITE DATA TYPE
    
    ONLY SUPPORT: TEXT, NUM, INT, REAL, "" (BLOB: empty string)
    (2021-05-05) all TEXT will now become a BLOB. This is to prevent invalid quotation characters. 
                 all string will be processed by json.dumps() to prevent this issue.
    DATA TYPE documentation at https://sqlite.org/lang_createtable.html
    """
    if not type(dtype) == type:
        raise ValueError("Provided dtype is not a python type object.")
        
    if dtype == int:
        return "INT"
    elif dtype == float:
        return "REAL"
    return ""


def sqlite3_json_to_table(db_path:str, new_table:str, json_list:list, PK_list:list):     
    """
    import JSON into sqlite3 database as new table
    expected a structure of List of Dict i.e. [{ "key_1":"value_1", "key_2":"value_2",... },...]
    TABLE's Column Name and Data Type is defined by the first layer of dict keys. 
    
    """
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
    column_names_parsed = [replace_specialchars2underscore(string) for string in column_names]
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
            # Convert data object that is not supported by SQLITE to string `json.dumps`
            # They can be parsed back using `json.loads` 
            columns_vals_converted = [f"'{json.dumps(replace_single2doublequote(pair[0]))}'" if pair[1]=="" 
#                                       else f"'{replace_single2doublequote(pair[0])}'" if pair[1]=="TEXT" 
                                      else pair[0] 
                                      for pair in columns_vals_zip]
            sqlite3_add_record(db_path, new_table, columns_vals_converted)

        print("JSON data imported into SQLITE table")
        
    except Exception as e:
        # drop the new table if adding record fails
        error_message = f"{type(e).__name__}: {str(e)}"
        print(error_message)
        statement = f""" DROP TABLE {new_table} """
        cursor = sqlite3_safe_execute(db_path, statement)

def sqlite3_parse_table2dataframe(db_path, table):
    """
    Convert TEXT->str, INT->int, REAL->float, ''->json.loads()
    This is assuming that all BLOBS are text string converted by json.dumps().
    json.loads() will convert these objects back to original data objects.
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
            df[item[0]] = df[item[0]].apply(lambda x: json.loads(x))
        elif item[1]=="TEXT":
            df[item[0]] = df[item[0]].apply(lambda x: str(x))
        elif item[1]=="INT":
            df[item[0]] = df[item[0]].apply(lambda x: int(x))
        elif item[1]=="REAL":
            df[item[0]] = df[item[0]].apply(lambda x: float(x))
    return df

def sqlite3_get_primarykeys(db_path, table):
    """
    Get list of primary keys in order
    """
    column_indices, column_names, column_type, column_notnull, column_pk = sqlite3_get_tableinfo(db_path, new_table)
    return [col[0] for col in sorted(zip(column_names,column_pk), key = lambda x: x[1]) if col[1]>0]
