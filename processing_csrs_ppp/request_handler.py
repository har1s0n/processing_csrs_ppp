import requests
import datetime
from dataclasses import dataclass
from typing import Type


@dataclass
class Coordinates:
    """
    A class for representing station coordinates
    """
    name: str
    dt: datetime
    x: float = 0.0
    y: float = 0.0
    z: float = 0.0
    latitude: float = 0.0
    longitude: float = 0.0
    height: float = 0.0


class RequestHandler:
    """
    A class for processing database queries

    Attributes
    ----------
    connection : mysql.connector.connect
        Database connection object

    Methods
    -------
    getting_station_data(station_id, dt)
        Getting records from station_tb for a specific station ID and for a given date
    """

    def __init__(self, connection):
        self.connection = connection

    # Section for working with the station_tb table
    def select_station_data(self, station_id: str, dt: datetime) -> list:
        """
        Getting records from station_tb for a specific station ID and for a given date
        
        Parameters
        ----------
        station_id : str
            Station identifier (station name)
        dt : datetime
            The epoch of obtaining the coordinates of the station

        Returns
        -------
        list()
            A list of records from the database
        """
        dt_to_str = dt.strftime("%Y-%m-%d %H:%M:%S")
        select_station_data_query = f"SELECT * " \
                                    f"FROM odtssw_paf.station_tb " \
                                    f"WHERE station_id='{station_id}' AND date='{dt_to_str}'; "
        return requests.execute_read_query(self.connection, select_station_data_query)

    def insert_station_data(self, data: Type[Coordinates]) -> None:
        """
        Inserting records from station_tb for a specific station ID and for a given date

        Parameters
        ----------
        data : Coordinates
            Station data structure (name, datetime, x, y, z, latitude, longitude, height)

        Returns
        -------
        None
        """
        valid_from = data.dt - datetime.timedelta(days=1)
        dt_to_str = data.dt.strftime("%Y-%m-%d %H:%M:%S")
        valid_dt_to_str = valid_from.strftime("%Y-%m-%d %H:%M:%S")
        insert_station_data_query = f"INSERT INTO odtssw_paf.station_tb (station_id, user_id, station_config_id," \
                                    f" latitude, longitude, download, masterClockPriority, dataRate," \
                                    f" daily_download_site_id," \
                                    f"backup_download_site_id, rinexFileRate, x, y, z, vx, vy, vz, date," \
                                    f" available, precise, forceC1, public, satelliteSystem," \
                                    f"oceanLoadingData, AntDome, AntType, OffsetNorth, OffsetEast," \
                                    f" OffsetUp, validFrom, validUntil)" \
                                    f"VALUES ('{data.name}', '1', '1', '{data.latitude}', '{data.longitude}'," \
                                    f" '1', '0', '30', '31', '31', '', '{data.x}', '{data.y}', '{data.z}'," \
                                    f"'0', '0', '0', '{dt_to_str}', '1', '0', '0', '0', 'GRE', '', '', ''," \
                                    f" '0', '0', '0', '{valid_dt_to_str}', '2030-01-01 00:00:00');"
        requests.execute_write_query(self.connection, insert_station_data_query)

    def update_station_data(self, station_id: str, dt: datetime, data: Type[Coordinates]) -> None:
        """
        Updating records from station_tb for a specific station ID and for a given date

        Parameters
        ----------
        station_id : str
            Station ID (name of the station) whose data needs to be updated
        dt: datetime
            The epoch for which the data needs to be updated
        data : Coordinates
            Station data structure (name, datetime, x, y, z, latitude, longitude, height)

        Returns
        -------
        None
        """
        selected_dt_to_str = dt.strftime("%Y-%m-%d %H:%M:%S")
        update_station_data_query = f"UPDATE odtssw_paf.station_tb " \
                                    f"SET latitude='{data.latitude}', longitude='{data.longitude}', " \
                                    f"x='{data.x}', y='{data.y}', z='{data.z}' " \
                                    f"WHERE station_id='{station_id}' AND date='{selected_dt_to_str}';"
        requests.execute_write_query(self.connection, update_station_data_query)

    # Section for working with the scenario_station_tb table
    def insert_station(self, scenario_id: int, station_id: str, user_id: int = 1, station_config_tb: int = 1) -> None:
        """
        Deleting stations from table

        Parameters
        ----------
        scenario_id : int
            Scenario ID for which you want to remove stations from the table
        station_id
            Station ID (name of the station)
        user_id : int
            The user ID received during registration (see the user_tb table)
        station_config_tb : int
            The ID of the config for determining system parameters (see the table station_config_tb)

        Returns
        -------
        None
        """
        insert_station_query = f"INSERT INTO odtssw_paf.scenario_station_tb " \
                               f"(scenario_id, station_id, user_id, station_config_id) " \
                               f"VALUES ('{scenario_id}', '{station_id}', '{user_id}', '{station_config_tb}');"
        requests.execute_write_query(self.connection, insert_station_query)

    def delete_stations(self, scenario_id: int) -> None:
        """
        Deleting stations from table

        Parameters
        ----------
        scenario_id : int
            Scenario ID for which you want to remove stations from the table

        Returns
        -------
        None
        """
        delete_stations_query = f"DELETE FROM odtssw_paf.scenario_station_tb " \
                                f"WHERE scenario_id='{scenario_id}';"
        requests.execute_write_query(self.connection, delete_stations_query)
