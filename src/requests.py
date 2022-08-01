from mysql.connector import Error


def execute_read_query(connection, query: str) -> list:
    """
    Getting records from a database

    Parameters
    ----------
    connection : mysql.connector.connect
        Database connection object
    query : str
        Request to extract records from the database

    Returns
    -------
    result : list
        tuple of records
    """
    cursor = connection.cursor()
    result = None
    try:
        cursor.execute(query)
        result = cursor.fetchall()
        return result
    except Error as e:
        print(f"The error '{e}' occurred")


def execute_write_query(connection, query: str) -> None:
    """
    Inserting/updating records in the database

    Parameters
    ----------
    connection : mysql.connector.connect
        Database connection object
    query : str
        Database write request

    Returns
    -------
    None
    """
    cursor = connection.cursor()
    try:
        cursor.execute(query)
        connection.commit()
        print("Query executed successfully")
    except Error as e:
        print(f"The error '{e}' occurred")
