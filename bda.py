import psycopg2

def connect_to_db(db_name, username, pwd, host='127.0.0.1', port='5432'):
    connection = psycopg2.connect(database = db_name, user = username, password = pwd, host = host, port = port)
    cursor = connection.cursor()
    return connection, cursor

def close_connection_to_db(connection, cursor):
    cursor.close()
    connection.close()

def create_table(connection, cursor, table_name):
    # RESET JUST IN CASE
    cursor.execute('DROP TABLE IF EXISTS {0}'.format(table_name))
    cursor.execute('DROP TYPE IF EXISTS logging_level_categories')
    cursor.execute('DROP TYPE IF EXISTS retrieval_stage_categories')

    # CREATE TYPE logging_level_categories
    cursor.execute("CREATE TYPE logging_level_categories AS ENUM ('DEBUG', 'INFO', 'WARN', 'ERROR')")

    # CREATE TYPE retrieval_stage_categories
    cursor.execute("CREATE TYPE retrieval_stage_categories AS ENUM ('event_processing', 'ght_data_retrieval', 'api_client', 'retriever', 'ghtorrent')")

    # CREATE TABLE bda_gh_torrent
    cursor.execute("CREATE TABLE {0} (logging_level logging_level_categories, timestamp CHAR(25), downloader_id VARCHAR(15), retrieval_stage retrieval_stage_categories, operation_part TEXT)".format(table_name))

    connection.commit()

def load_file_into_db(filename, connection, cursor):
    with open(filename) as file:
        data = file.read()
        all_rows = data.split('\n')
        for each_row in all_rows[0:5]:
            try:
                standard_part, operation_part = each_row.split('.rb: ')
                logging_level, timestamp, other = standard_part.split(', ')
                downloader_id, retrieval_stage = other.split(' -- ')
                insert_query_string = "INSERT INTO bda_gh_torrent (logging_level, timestamp, downloader_id, retrieval_stage, operation_part) VALUES ({0}, {1}, {2}, {3}, {4})".format(logging_level, timestamp, downloader_id, retrieval_stage, operation_part)
                cursor.execute(insert_query_string)
            except (Exception, psycopg2.DatabaseError) as e:
                print('Error: {}'.format(e))
                pass
        connection.commit()

def task_2(connection, cursor, table_name):
    query = "SELECT count(*) FROM {0}".format(table_name)
    cursor.execute(query)
    count = int(cursor.fetchone()[0])
    print(count)


if __name__ == '__main__':
    db_name = 'postgres_db'
    user = 'madhur'
    password = 'bda_gh_torrent'
    table_name = 'bda_gh_torrent'
    filename = 'ghtorrent-logs.txt'

    connection, cursor = connect_to_db(db_name, user, password)
    create_table(connection, cursor, table_name)
    load_file_into_db(filename, connection, cursor)
    task_2(connection, cursor, table_name)
    close_connection_to_db(connection, cursor)
