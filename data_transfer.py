import configparser
import pandas as pd
import psycopg2



def create_connection(hostname, db_name, username, pwd, port_id):
    """ function to establish a connection to a PostgreSQL database

    Args:
        hostname (string): hostname
        db_name (string): database-name
        username (string): username
        pwd (string): password
        port_id (integer): id of the port
    Returns:
        object: connection object
    """
    try: 
        conn = psycopg2.connect(
            host=hostname,
            database=db_name,
            user=username,
            password=pwd,
            port = port_id
        )
        
        print("Database connection is live...")
        return conn
    except Exception as e:
            print(f"Error connecting to database: {e}")
        

def create_cursor(conn):
    """ function to establish a cursor to a PostgreSQL database

    Args:
        conn (object): object of database connection

    Returns:
        object: cursur object
    """
    try: 
        cur = conn.cursor()
        return cur
    except Exception as e:
            print(f"Error establishing to cursor: {e}")
    


def extract_data(file_path):
    """ function to load csv-file and transform it into suitable format

    Args:
        file_path (string): path to csv-file

    Returns:
        object: dataframe object of the clean csv-file
    """
    # read file in
    df = pd.read_csv(file_path, index_col=0)
    # rename column names
    columns = list(df.columns)
    columns = [item.lower().replace(" ", "_") for item in columns]
    df.columns = columns
    # find columns with null values
    null_cols = df.columns[df.isnull().sum() > 0]
    df[null_cols] = df[null_cols].fillna(0)
    # Drop duplicate rows
    df.drop_duplicates(inplace=True)
    
    return df 

# Define function to create database table
def create_table(conn, cur, table_name, columns):
    """ function to create database table

    Args:
        conn (object): connection object
        cur (object): cursor object
        table_name (string): name of the database table
        columns (dict): dictionary of columns and respective datatypes
    """
    # drop table first
    cur.execute(f"DROP TABLE IF EXISTS {table_name}")
    # Construct SQL statement to create table
    create_script = f"CREATE TABLE {table_name} ({', '.join([f'{k} {v}' for k, v in columns.items()])})"
   
    try:
        cur.execute(create_script)
        conn.commit()
        print(f"Table '{table_name}' created successfully!")
    except Exception as e:
        print(f"Error creating table: {e}")


def load_data_to_postgres(conn, cur, df, table_name, column_names):
    """ function to transfer data from a pandas DataFrame to a PostgreSQL table

    Args:
        conn (object): connection object
        cur (object): cursor object
        df (object): dataframe of clean file
        table_name (string): name of the database table
    """
    # Construct SQL statement to insert row into table
    num_columns = len(df.columns)
    values = ', '.join(['%s' for _ in range(num_columns)])
    insert_script = f'INSERT INTO {table_name} ({column_names}) VALUES ({values})'
    try: 
        for row in df.itertuples(index=False):
            cur.execute(insert_script, row)

        conn.commit()
    except Exception as e:
            print(f"Error creating transfer: {e}")

        
def query_data_from_postgres(conn, cur, query):
    """ function to query data from database

    Args:
        conn (object): connection object
        cur (object): cursor object
        query (string): query for the database
    """
    try:
        # execute query
        cur.execute(query)
        # fetch the first row from the cursor
        first_row = cur.fetchone()
        print(first_row)
    except Exception as e:
            print(f"Error sending query: {e}")
        


def main(database):
    """
    main function to load CSV files and transfer data to a PostgreSQL database
    
    """
    # load the configuration file
    config = configparser.ConfigParser()
    config.read('config.ini')
    # get the settings for the database
    table_name = config.get(database, 'table_name')
    file_path = config.get(database, 'file_path')
    column_names = config.get(database, 'column_names')
    column_dict = eval(config.get(database, 'column_dict'))
    query = f"SELECT * FROM {table_name}"
    
    # Load CSV file into pandas DataFrame
    df = extract_data(file_path)
    
    # Establish connection to PostgreSQL database
    conn = create_connection("localhost", "database_name", "user", "pw", "port_id")
    
    # Creat cursor
    cur = create_cursor(conn)
    
    # Create table in PostgreSQL database
    create_table(conn, cur, table_name, column_dict)
    
    # Transfer data from pandas DataFrame to PostgreSQL table
    load_data_to_postgres(conn, cur, df, table_name, column_names)
    
    # Query data from PostgreSQL table
    query_data_from_postgres(conn, cur, query)
    
    # Close database connection
    cur.close()
    conn.close()

if __name__ == '__main__':
    main('db_videos')