import psycopg2

def connect_to_db(db_name, username, pwd, host='127.0.0.1', port='5432'):
    connection = psycopg2.connect(database = db_name, user = username, password = pwd, host = host, port = port)
    cursor = connection.cursor()
    return connection, cursor

def close_connection_to_db(connection, cursor):
    cursor.close()
    connection.close()

def load_file_into_db(filename):
    with open(filename) as file:
        data = file.read()
        all_rows = data.split('\n')
        for each_row in all_rows:
            standard_part, operation_part = each_row.split('.rb: ')
            logging_level, timestamp, other = standard_part.split(', ')
            downloader_id, retrieval_stage = other.split(' -- ')
            print(logging_level, timestamp, downloader_id, retrieval_stage, operation_part)

if __name__ == '__main__':
    db_name = 'postgres_db'
    user = 'madhur'
    password = 'bda_gh_torrent'
    filename = 'ghtorrent-logs.txt'

    connection, cursor = connect_to_db(db_name, user, password)
    load_file_into_db(filename)
    close_connection_to_db(connection, cursor)
