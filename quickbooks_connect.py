import pyodbc
import pandas as pd


def provide_table_list(cursor : pyodbc.Cursor):
    """
    fetches name and description of each table in ODBC driver

    parameters:
        cursor : database cursor fetched using pyodbc library
    
    returns:
        table_df : pd.DataFrame with columns = 'Name' and 'Description' if each queryable table in the driver
    """
    SQL_COMMAND = "SP_TABLES"
    cursor.execute(SQL_COMMAND)

    table_df = pd.DataFrame(columns=['Name', 'Description'])

    for row in cursor.fetchall():
        # 2 is the index of the actual table name
        # 4 is the index of the table descriptor
        table_name_and_description = pd.DataFrame(data=[[row[2],row[4]]], columns=['Name', 'Description'], index=[len(table_df)])
        table_df = pd.concat([table_df, table_name_and_description])

    return table_df


def provide_customer_list(cursor : pyodbc.Cursor):
    """
    fetches name and meta-data of customers

    parameters:
        cursor : database cursor fetched using pyodbc library
    
    returns:
        customer_df : pd.DataFrame with columns generically received from sql SP_COLUMNS command
    """
    # Get all of the column names associated with the customer table
    COLLECT_TABLE_INFO="SP_COLUMNS Customer"
    cursor.execute(COLLECT_TABLE_INFO)
    df_columns = [row[3] for row in cursor.fetchall()]

    # Get all of the customers
    SQL_COMMAND = "SELECT * FROM Customer"
    cursor.execute(SQL_COMMAND)
    # use "map" to cast each item within the iterator to a list - that makes the data
    # digestable to the pd.DataFrame initialization
    customer_df = pd.DataFrame(data=map(list, cursor.fetchall()), columns=df_columns)
    return customer_df



if __name__ == "__main__":
    """
    run the functions
    """

    # this will change based on the name of the service in the
    # "ODBC Data Sources" application in Windows
    ODBC_CONNECTION_STR = f'DSN=QuickBooks Online Data 64-Bit;'

    connection = pyodbc.connect(ODBC_CONNECTION_STR)
    cursor = connection.cursor()
    print(provide_table_list(cursor))
    print(provide_customer_list(cursor))
    connection.close()