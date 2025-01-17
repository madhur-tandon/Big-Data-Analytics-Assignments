"""
CSE506: Big Data Analytics
Assignment 1
Madhur Tandon (2016053)
Taejas Gupta (2016204)
January 17, 2020

Data Analytics using RDBMS
"""


import psycopg2
import time
import pandas as pd
import sqlalchemy
from sqlalchemy import create_engine


def eliminate_extra_info(url_text):
    if type(url_text) == tuple:
        url_text = url_text[0]
    http_pos = url_text.find('http')
    i = -1
    count = 0
    while True:
        i = url_text.find('/', i+1)
        if i == -1:
            i = url_text.find('?')
            return url_text[http_pos:i]
        else:
            count += 1
            if count == 6:
                return url_text[http_pos:i]


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
                insert_query_string = ("INSERT INTO bda_gh_torrent (logging_level, timestamp, downloader_id, retrieval_stage, operation_part) "
                                       "VALUES (%s, %s, %s, %s, %s)"
                                       )
                cursor.execute(insert_query_string, (logging_level, timestamp,
                                                     downloader_id, retrieval_stage, operation_part))
                connection.commit()
            except (Exception, psycopg2.DatabaseError) as e:
                print('Error: {0} {1}'.format(e, each_row))
                cursor.execute("ROLLBACK")
                connection.commit()
                pass


def task_2(connection, cursor, table_name):
    query = ("SELECT count(*) "
             "FROM {0}"
             ).format(table_name)
    cursor.execute(query)
    count = int(cursor.fetchone()[0])
    print(count)


def task_3(connection, cursor, table_name):
    query = ("SELECT count(*) "
             "FROM {0} "
             "WHERE logging_level = 'WARN'"
             ).format(table_name)
    cursor.execute(query)
    count = int(cursor.fetchone()[0])
    print(count)


def task_4(connection, cursor, table_name):
    query = ("SELECT DISTINCT operation_part "
             "FROM {0} "
             "WHERE (logging_level = 'WARN' OR logging_level = 'INFO') AND retrieval_stage = 'api_client' AND STRPOS(operation_part, 'repos/')>0"
             ).format(table_name)
    cursor.execute(query)
    cleaned_url_text = list(map(eliminate_extra_info, cursor.fetchall()))
    print(len(set(cleaned_url_text)))


def task_5(connection, cursor, table_name):
    query = ("SELECT downloader_id, COUNT(*) frequency "
             "FROM {0} "
             "WHERE retrieval_stage = 'api_client' "
             "GROUP BY downloader_id "
             "ORDER BY frequency DESC "
             "LIMIT 10"
             ).format(table_name)
    cursor.execute(query)
    for c in cursor:
        print(c)


def task_6(connection, cursor, table_name):
    query = ("SELECT downloader_id, COUNT(*) frequency "
             "FROM {0} "
             "WHERE retrieval_stage = 'api_client' AND STRPOS(operation_part, 'Failed')=1 "
             "GROUP BY downloader_id "
             "ORDER BY frequency "
             "DESC LIMIT 10"
             ).format(table_name)
    cursor.execute(query)
    for c in cursor:
        print(c)


def task_7(connection, cursor, table_name):
    query = ("SELECT SUBSTRING(timestamp, STRPOS(timestamp, 'T')+1, 2) AS hour, COUNT(*) frequency "
             "FROM {0} "
             "GROUP BY hour "
             "ORDER BY frequency DESC "
             "LIMIT 1"
             ).format(table_name)
    cursor.execute(query)
    print(cursor.fetchone())


def task_8(connection, cursor, table_name):
    """
    Slight different from Task 4 query, remove DISTINCT here
    only considering logging_level as WARN since python post processing of figuring out max in last line takes too much time with both WARN and INFO
    """
    query = ("SELECT operation_part "
             "FROM {0} "
             "WHERE (logging_level = 'WARN') AND retrieval_stage = 'api_client' AND STRPOS(operation_part, 'repos/')>0"
             ).format(table_name)
    cursor.execute(query)
    cleaned_url_text = list(map(eliminate_extra_info, cursor.fetchall()))
    print(max(set(cleaned_url_text), key=cleaned_url_text.count))


def task_9(connection, cursor, table_name):
    """
    'geolocator' queries have no access keys
    """
    query = ("SELECT SUBSTRING(operation_part, STRPOS(operation_part, 'Access')+8, 11) AS access_key, COUNT(*) frequency "
             "FROM {0} "
             "WHERE retrieval_stage != 'geolocator' AND STRPOS(operation_part, 'Failed')=1 "
             "GROUP BY access_key "
             "ORDER BY frequency DESC "
             "LIMIT 10"
             ).format(table_name)
    cursor.execute(query)
    for c in cursor:
        print(c)


def task_10(connection, cursor, table_name):
    create_index_query = ("CREATE INDEX IF NOT EXISTS downloader_id_index "
                          "ON {0} (downloader_id)"
                          ).format(table_name)
    cursor.execute(create_index_query)
    start = time.time()
    query = ("SELECT DISTINCT operation_part "
             "FROM {0} "
             "WHERE downloader_id = 'ghtorrent-22' AND (logging_level = 'WARN' OR logging_level = 'INFO') AND retrieval_stage = 'api_client' AND STRPOS(operation_part, 'repos/')>0"
             ).format(table_name)
    cursor.execute(query)
    cleaned_url_text = list(map(eliminate_extra_info, cursor.fetchall()))
    print(len(set(cleaned_url_text)))
    end = time.time()
    print(end - start)


def task_11(connection, cursor, table_name):
    drop_index_query = "DROP INDEX downloader_id_index"
    cursor.execute(drop_index_query)
    start = time.time()
    query = ("SELECT DISTINCT operation_part "
             "FROM {0} "
             "WHERE downloader_id = 'ghtorrent-22' AND (logging_level = 'WARN' OR logging_level = 'INFO') AND retrieval_stage = 'api_client' AND STRPOS(operation_part, 'repos/')>0"
             ).format(table_name)
    cursor.execute(query)
    cleaned_url_text = list(map(eliminate_extra_info, cursor.fetchall()))
    print(len(set(cleaned_url_text)))
    end = time.time()
    print(end - start)


def task_12(filename, connection, cursor, username):
    cursor.execute('DROP TABLE IF EXISTS interesting')
    connection.commit()

    df = pd.read_csv(filename)

    engine = create_engine(
        'postgresql+psycopg2://{0}:bda_gh_torrent@127.0.0.1:5432/postgres_db'.format(username))

    # updated_at cannot be TIMESTAMP since postgres doesn't accept 0000-00-00 00:00:00
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

    query = ("SELECT count(*) "
             "FROM interesting"
             )
    cursor.execute(query)
    count = int(cursor.fetchone()[0])
    print(count)


def task_13(connection, cursor):
    query = ("SELECT COUNT(*) "
             "FROM bda_gh_torrent, interesting "
             "WHERE (logging_level = 'WARN' OR logging_level = 'INFO') AND retrieval_stage = 'api_client' AND STRPOS(operation_part, url)>0"
             )
    cursor.execute(query)
    count = int(cursor.fetchone()[0])
    print(count)


def task_14(connection, cursor):
    query = ("SELECT url, COUNT(*) frequency "
             "FROM bda_gh_torrent, interesting "
             "WHERE (logging_level = 'WARN' OR logging_level = 'INFO') AND retrieval_stage = 'api_client' AND STRPOS(operation_part, url)>0 AND STRPOS(operation_part, 'Failed')=1 "
             "GROUP BY url "
             "ORDER BY frequency DESC "
             "LIMIT 1"
             )
    cursor.execute(query)
    for c in cursor:
        print(c)


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
    print("-----TASK 2-----")
    task_2(connection, cursor, table_name)
    print()
    print("-----TASK 3-----")
    task_3(connection, cursor, table_name)
    print()
    print("-----TASK 4-----")
    task_4(connection, cursor, table_name)
    print()
    print("-----TASK 5-----")
    task_5(connection, cursor, table_name)
    print()
    print("-----TASK 6-----")
    task_6(connection, cursor, table_name)
    print()
    print("-----TASK 7-----")
    task_7(connection, cursor, table_name)
    print()
    print("-----TASK 8-----")
    task_8(connection, cursor, table_name)
    print()
    print("-----TASK 9-----")
    task_9(connection, cursor, table_name)
    print()
    print("-----TASK 10-----")
    task_10(connection, cursor, table_name)
    print()
    print("-----TASK 11-----")
    task_11(connection, cursor, table_name)
    print()
    print("-----TASK 12-----")
    task_12(filename_task_12, connection, cursor, user)
    print()
    print("-----TASK 13-----")
    task_13(connection, cursor)
    print()
    print("-----TASK 14-----")
    task_14(connection, cursor)
    print()
    close_connection_to_db(connection, cursor)
