from sys import platform
from datetime import datetime
import zipfile
import argparse
import os
import shutil
import mysqldb
import configparser
import request_handler


def os_dependency_slash() -> str:
    if platform == 'win32':
        return '\\'
    return '/'


def path_processing() -> bool:
    if not os.path.exists(input_data):
        return False
    if not zipfile.is_zipfile(input_data):
        return False
    print(
        f'Size: {os.path.basename(input_data)} {os.path.getsize(input_data) // 1024} Кб')

    if not os.path.exists(output_data):
        os.mkdir(output_data)
        if not os.path.exists(output_data):
            return False
    return True


def extracting_files() -> None:
    try:
        with zipfile.ZipFile(input_data, 'r') as zip_file:
            zip_file.extractall(os.path.dirname(input_data) + os_dependency_slash() + 'tmp_')
            print(f"Unpacking of the archive has been successfully completed.")
    except OSError:
        print(f"Could not open/read file: {input_data}")
        exit()


def conversion_decimal_deg(raw_data: str) -> float:
    raw_data_split = [name.strip() for name in raw_data.split()]
    degrees = int(raw_data_split[0])
    minutes = int(raw_data_split[1])
    seconds = float(raw_data_split[2])
    if degrees >= 0:
        decimal = degrees + (float(minutes) / 60) + (float(seconds) / 3600)
    else:
        decimal = degrees - float(minutes) / 60 - float(seconds) / 3600

    return decimal


def custom_separator(content: str) -> list:
    result = list()
    for line in content:
        # Get all variable-length spaces down to two. Then use two spaces as the delimiter.
        while line.replace("   ", "  ") != line:
            line = line.replace("   ", "  ")

        # The strip is optional here.
        result.extend(line.strip().split("  "))

    return result


def get_pos_data(file: str) -> request_handler.Coordinates:
    result_pos_data = request_handler.Coordinates()
    with open(file) as f:
        pos_counter = 0
        for line in f:
            current_data = line.split()
            if pos_counter == 7:
                break
            if current_data[0].strip() == 'RNX':
                result_pos_data.name = current_data[1].strip()[:4]
            if current_data[0].strip() == 'BEG':
                dt_string = ' '.join(current_data[1:])
                result_pos_data.dt = datetime.strptime(dt_string, "%Y-%m-%d %H:%M:%S.%f")
            if current_data[0].strip() == 'POS':
                pos_counter += 1
                if pos_counter == 1:
                    continue
                if current_data[1].lower() == 'lat':
                    result_pos_data.latitude = conversion_decimal_deg(' '.join(current_data[7:10]))
                if current_data[1].lower() == 'lon':
                    result_pos_data.longitude = conversion_decimal_deg(' '.join(current_data[7:10]))
                if current_data[1].lower() == 'x':
                    result_pos_data.x = float(current_data[5])
                if current_data[1].lower() == 'y':
                    result_pos_data.y = float(current_data[5])
                if current_data[1].lower() == 'z':
                    result_pos_data.z = float(current_data[5])
    return result_pos_data


def check_station_id(handler: request_handler.RequestHandler) -> bool:
    if len(scenario_id) == 0:
        print(f"Verification error scenario_id. "
              f"You must specify the scenario_id in config.ini")
        return False
    selected_scenario_tb = handler.select_scenario(int(scenario_id))
    if len(selected_scenario_tb) == 0:
        print(f"Verification error scenario_id. "
              f"Check the correctness of the specified.")
        return False
    return True


def updating_list_stations(handler: request_handler.RequestHandler, stations: set) -> None:
    if not check_station_id(handler):
        return
        # делаем delete из scenario_station_tb по id
    handler.delete_stations(int(scenario_id))
    for station in stations:
        # делаем insert
        handler.insert_station(int(scenario_id), station)


def sending_data_database(records: list) -> None:
    handler = request_handler.RequestHandler(db_connection)
    stations = set()
    for record in records:
        stations.add(record.name)
        # делаем select из station_tb
        selected_station_data = handler.select_station_data(record.name, record.dt)
        if len(selected_station_data) == 0:
            # делаем insert
            handler.insert_station_data(record)
        else:
            # делаем update
            selected_station_id = selected_station_data[0][0]
            selected_dt = selected_station_data[0][17]
            handler.update_station_data(station_id=selected_station_id, dt=selected_dt, data=record)
    if flag_upd:
        updating_list_stations(handler, stations)


def parsing() -> list:
    result = list()
    tmp_path = os.path.dirname(input_data) + os_dependency_slash() + 'tmp_'
    for current_file_name in os.listdir(tmp_path):
        current_file = tmp_path + os_dependency_slash() + current_file_name
        if not zipfile.is_zipfile(current_file):
            continue

        with zipfile.ZipFile(current_file, 'r') as zip_file:
            file_names = zip_file.namelist()
            for filename in file_names:
                if filename.endswith('sum'):
                    content = zip_file.open(filename).read()
                    open(filename, 'wb').write(content)
                    # получение POS из файла
                    result.append(get_pos_data(filename))
                    try:
                        os.remove(filename)
                    except OSError as e:
                        print(f"Error: {e.filename} - {e.strerror}.")
        zip_file.close()
    return result


if __name__ == '__main__':
    parser = argparse.ArgumentParser(
        description='Parser of the result of processing the CSRS-PPP SPARK service')
    parser.add_argument('--i', type=str, dest='input',
                        default=os.getcwd() + os_dependency_slash() + "full_output.zip",
                        help="absolute file path for parsing")
    parser.add_argument('--o', type=str, dest='output',
                        default=os.getcwd(),
                        help="the absolute path of the file "
                             "to record the parsing result")
    parser.add_argument('--u', type=str, dest='update',
                        default=True,
                        help="flag for updating "
                             "the list of scenario stations (odtssw_paf.scenario_station_tb)")
    args = parser.parse_args()
    input_data = args.input
    output_data = args.output
    flag_upd = args.update

    if str(flag_upd).lower() in ['False', 'false', '0', 'f', 'n', 'no']:
        flag_upd = False

    if not path_processing():
        print(
            f'Data buffering error. '
            f'It is necessary to check the correctness of the entered data.')
        exit()

    # Чтение config-файла
    config = configparser.ConfigParser(allow_no_value=True)
    config.read('../config/config.ini')
    host = config['Database']['address']
    db_name = config['Database']['db_name']
    username = config['Database']['username']
    password = config['Database']['password']
    port = config.getint('Database', 'port')
    ssh_host = config['SSH']['ssh_host']
    ssh_port = config.getint('SSH', 'ssh_port')
    ssh_user = config['SSH']['ssh_user']
    ssh_password = config['SSH']['ssh_password']
    scenario_id = config['ODTS']['scenario_id']
    # Тест коннекта к БД
    database = mysqldb.MySQLConnection(host_name=host, database_name=db_name, user_name=username,
                                       user_password=password, port=port)
    db_connection = database.create_connection_tunnel(ssh_host=ssh_host, ssh_port=ssh_port, ssh_username=ssh_user,
                                                      ssh_password=ssh_password)
    # запуск распаковки в tmp каталок
    extracting_files()

    # парсинг
    parsed_data = parsing()

    # отправка данных в БД odtssw_paf
    sending_data_database(parsed_data)

    # удаление tmp каталога после завершения парсинга
    shutil.rmtree(os.path.dirname(input_data) + os_dependency_slash() + 'tmp_', ignore_errors=True)

    # Закрыть подключение к БД
    database.close_connection()
