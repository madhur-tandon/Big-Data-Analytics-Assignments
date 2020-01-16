import psycopg2
import time
import pandas as pd
import sqlalchemy
from sqlalchemy import create_engine


def eliminate_extra_info(url_text):
    url_text = url_text[0]
    i = -1
    count = 0
    while True:
        i = url_text.find('/', i+1)
        if i == -1:
            return url_text[:-1]
        else:
            count += 1
            if count == 3:
                return url_text[:i]


def connect_to_db(db_name, username, pwd, host='127.0.0.1', port='5432'):
    connection = psycopg2.connect(
        database=db_name, user=username, password=pwd, host=host, port=port)
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
    cursor.execute(
        "CREATE TYPE logging_level_categories AS ENUM ('DEBUG', 'INFO', 'WARN', 'ERROR')")

    # CREATE TYPE retrieval_stage_categories
    cursor.execute(
        "CREATE TYPE retrieval_stage_categories AS ENUM ('event_processing', 'ght_data_retrieval', 'api_client', 'retriever', 'ghtorrent', 'geolocator')")

    # CREATE TABLE bda_gh_torrent
    cursor.execute(
        "CREATE TABLE {0} (logging_level logging_level_categories, timestamp CHAR(25), downloader_id VARCHAR(15), retrieval_stage retrieval_stage_categories, operation_part TEXT)".format(table_name))

    connection.commit()


def load_file_into_db(filename, connection, cursor):
    with open(filename) as file:
        data = file.read()
        all_rows = data.split('\n')
        for each_row in all_rows:
            try:
                standard_part, operation_part = each_row.split('.rb: ')
                logging_level, timestamp, other = standard_part.split(', ')
                downloader_id, retrieval_stage = other.split(' -- ')
                insert_query_string = "INSERT INTO bda_gh_torrent (logging_level, timestamp, downloader_id, retrieval_stage, operation_part) VALUES (%s, %s, %s, %s, %s)"
                cursor.execute(insert_query_string, (logging_level, timestamp,
                                                     downloader_id, retrieval_stage, operation_part))
                connection.commit()
            except (Exception, psycopg2.DatabaseError) as e:
                print('Error: {0} {1}'.format(e, each_row))
                cursor.execute("ROLLBACK")
                connection.commit()
                pass


def task_2(connection, cursor, table_name):
    query = "SELECT count(*) FROM {0}".format(table_name)
    cursor.execute(query)
    count = int(cursor.fetchone()[0])
    print(count)


def task_3(connection, cursor, table_name):
    query = "SELECT count(*) FROM {0} WHERE logging_level = 'WARN'".format(table_name)
    cursor.execute(query)
    count = int(cursor.fetchone()[0])
    print(count)


def task_4(connection, cursor, table_name):
    query = "SELECT DISTINCT SUBSTRING(operation_part, STRPOS(operation_part, 'repos/'), STRPOS(SUBSTRING(operation_part, STRPOS(operation_part, 'repos/')), '?')) FROM {0} WHERE logging_level = 'WARN' AND retrieval_stage = 'api_client' AND SUBSTRING(operation_part, STRPOS(operation_part, 'repos/'), 4) = 'repo'".format(
        table_name)
    cursor.execute(query)
    cleaned_url_text = list(map(eliminate_extra_info, cursor.fetchall()))
    print(len(set(cleaned_url_text)))


def task_5(connection, cursor, table_name):
    query = "SELECT downloader_id, COUNT(*) frequency FROM {0} WHERE SUBSTRING(operation_part, STRPOS(operation_part, 'http'), 4) = 'http' GROUP BY downloader_id ORDER BY frequency DESC LIMIT 10".format(table_name)
    cursor.execute(query)
    for c in cursor:
        print(c)


def task_6(connection, cursor, table_name):
    query = "SELECT downloader_id, COUNT(*) frequency FROM {0} WHERE SUBSTRING(operation_part, STRPOS(operation_part, 'Failed'), 6) = 'Failed' AND SUBSTRING(operation_part, STRPOS(operation_part, 'http'), 4) = 'http' GROUP BY downloader_id ORDER BY frequency DESC LIMIT 10".format(table_name)
    cursor.execute(query)
    for c in cursor:
        print(c)


def task_7(connection, cursor, table_name):
    query = "SELECT SUBSTRING(timestamp, STRPOS(timestamp, 'T')+1, 2) AS hour, COUNT(*) frequency FROM {0} GROUP BY hour ORDER BY frequency DESC LIMIT 1".format(
        table_name)
    cursor.execute(query)
    print(cursor.fetchone())


def task_8(connection, cursor, table_name):
    """
    Slight different from Task 4 query, remove DISTINCT here
    """
    query = "SELECT SUBSTRING(operation_part, STRPOS(operation_part, 'repos/'), STRPOS(SUBSTRING(operation_part, STRPOS(operation_part, 'repos/')), '?')) FROM {0} WHERE logging_level = 'WARN' AND retrieval_stage = 'api_client' AND SUBSTRING(operation_part, STRPOS(operation_part, 'repos/'), 4) = 'repo'".format(
        table_name)
    cursor.execute(query)
    cleaned_url_text = list(map(eliminate_extra_info, cursor.fetchall()))
    print(max(set(cleaned_url_text), key=cleaned_url_text.count))


def task_9(connection, cursor, table_name):
    """
    'geolocator' queries have no access keys
    """
    query = "SELECT SUBSTRING(operation_part, STRPOS(operation_part, 'Access')+8, 11) AS access_key, COUNT(*) frequency FROM {0} WHERE retrieval_stage != 'geolocator' AND SUBSTRING(operation_part, STRPOS(operation_part, 'Failed'), 6) = 'Failed' GROUP BY access_key ORDER BY frequency DESC LIMIT 10".format(
        table_name)
    cursor.execute(query)
    for c in cursor:
        print(c)


def task_10(connection, cursor, table_name):
    create_index_query = "CREATE INDEX IF NOT EXISTS downloader_id_index ON {0} (downloader_id)".format(
        table_name)
    cursor.execute(create_index_query)
    start = time.time()
    query = "SELECT DISTINCT SUBSTRING(operation_part, STRPOS(operation_part, 'repos/'), STRPOS(SUBSTRING(operation_part, STRPOS(operation_part, 'repos/')), '?')) FROM {0} WHERE downloader_id = 'ghtorrent-22' AND logging_level = 'WARN' AND retrieval_stage = 'api_client' AND SUBSTRING(operation_part, STRPOS(operation_part, 'repos/'), 4) = 'repo'".format(
        table_name)
    cursor.execute(query)
    cleaned_url_text = list(map(eliminate_extra_info, cursor.fetchall()))
    print(len(set(cleaned_url_text)))
    end = time.time()
    print(end - start)


def task_11(connection, cursor, table_name):
    drop_index_query = "DROP INDEX downloader_id_index"
    cursor.execute(drop_index_query)
    start = time.time()
    query = "SELECT DISTINCT SUBSTRING(operation_part, STRPOS(operation_part, 'repos/'), STRPOS(SUBSTRING(operation_part, STRPOS(operation_part, 'repos/')), '?')) FROM {0} WHERE downloader_id = 'ghtorrent-22' AND logging_level = 'WARN' AND retrieval_stage = 'api_client' AND SUBSTRING(operation_part, STRPOS(operation_part, 'repos/'), 4) = 'repo'".format(
        table_name)
    cursor.execute(query)
    cleaned_url_text = list(map(eliminate_extra_info, cursor.fetchall()))
    print(len(set(cleaned_url_text)))
    end = time.time()
    print(end - start)


def task_12(filename, connection, cursor):
    cursor.execute('DROP TABLE IF EXISTS interesting')
    connection.commit()

    df = pd.read_csv(filename)

    engine = create_engine(
        'postgresql+psycopg2://madhur:bda_gh_torrent@127.0.0.1:5432/postgres_db')

    df.to_sql('interesting', engine, if_exists='replace', index=False,
              dtype={'id': sqlalchemy.types.VARCHAR(length=8),
                     'url': sqlalchemy.types.TEXT,
                     'owner_id': sqlalchemy.types.VARCHAR(length=8),
                     'name': sqlalchemy.types.TEXT,
                     'language': sqlalchemy.types.TEXT,
                     'created_at': sqlalchemy.types.TIMESTAMP,
                     'forked_from': sqlalchemy.types.VARCHAR(length=10),
                     'deleted': sqlalchemy.types.BOOLEAN,
                     'updated_at': sqlalchemy.types.TEXT})

    query = "SELECT count(*) FROM interesting"
    cursor.execute(query)
    count = int(cursor.fetchone()[0])
    print(count)


if __name__ == '__main__':
    db_name = 'postgres_db'
    user = 'madhur'
    password = 'bda_gh_torrent'
    table_name = 'bda_gh_torrent'
    filename = 'ghtorrent-logs.txt'
    filename_task_12 = 'important-repos.csv'
    table_name_task_12 = 'interesting'
    connection, cursor = connect_to_db(db_name, user, password)
    # create_table(connection, cursor, table_name)
    # load_file_into_db(filename, connection, cursor)
    task_2(connection, cursor, table_name)
    task_3(connection, cursor, table_name)
    task_4(connection, cursor, table_name)
    task_5(connection, cursor, table_name)
    task_6(connection, cursor, table_name)
    task_7(connection, cursor, table_name)
    task_8(connection, cursor, table_name)
    task_9(connection, cursor, table_name)
    task_10(connection, cursor, table_name)
    task_11(connection, cursor, table_name)
    task_12(filename_task_12, connection, cursor)
    close_connection_to_db(connection, cursor)
