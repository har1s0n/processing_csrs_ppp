import mysql.connector
import sshtunnel
from mysql.connector import Error


class MySQLConnection:
    """
    A class for interacting with the database
    """

    connection = None
    tunnel = None

    def __init__(self, host_name: str, database_name, user_name: str, user_password: str, port: int):
        self.host_name = host_name
        self.database_name = database_name
        self.user_name = user_name
        self.user_password = user_password
        self.port = port

    def create_connection(self):
        try:
            self.connection = mysql.connector.connect(host=self.host_name, database=self.database_name,
                                                      user=self.user_name,
                                                      passwd=self.user_password, port=self.port)
            self.__checking_connection()
        except Error as e:
            print(f"The error '{e}' occurred")

        return self.connection

    def create_connection_tunnel(self, ssh_host: str, ssh_port: int, ssh_username: str, ssh_password: str):
        # with sshtunnel.SSHTunnelForwarder(
        #         (ssh_host, ssh_port),
        #         ssh_username=ssh_username,
        #         ssh_password=ssh_password,
        #         remote_bind_address=('127.0.0.1', 3306)
        # ) as tunnel:
        self.tunnel = sshtunnel.SSHTunnelForwarder((ssh_host, ssh_port), ssh_username=ssh_username,
                                                   ssh_password=ssh_password,
                                                   remote_bind_address=('127.0.0.1', 3306))
        self.tunnel.start()
        try:
            self.connection = mysql.connector.connect(user=self.user_name, password=self.user_password,
                                                      host='127.0.0.1',
                                                      database=self.database_name,
                                                      port=self.tunnel.local_bind_port)
            self.__checking_connection()
        except Error as e:
            print(f"The error '{e}' occurred")

        return self.connection

    def close_connection(self) -> None:
        self.connection.close()
        self.tunnel.close()
        print("Disconnection from the database through the tunnel has been completed successfully")

    def __checking_connection(self) -> bool:
        is_connected = False
        if self.connection.is_connected():
            db_nfo = self.connection.get_server_info()
            print(f"Connected to MySQL Server version: {db_nfo}")
            cursor = self.connection.cursor()
            cursor.execute("select database();")
            record = cursor.fetchone()
            print(f"You're connected to database: {record}")
            is_connected = True
        return is_connected
