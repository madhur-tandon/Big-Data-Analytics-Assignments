from pyspark.sql import SparkSession, types, Row
from pyspark.sql.functions import desc, count, instr, lit, col
import psycopg2, time

def connect_to_db(db_name, username, pwd, host='127.0.0.1', port='5432'):
    connection = psycopg2.connect(
        database=db_name, user=username, password=pwd, host=host, port=port)
    cursor = connection.cursor()
    return connection, cursor

def close_connection_to_db(connection, cursor):
    cursor.close()
    connection.close()

def eliminate_extra_info(url_text):
    if type(url_text) == tuple:
        url_text = url_text[0]
    if type(url_text) == types.Row:
        url_text = url_text.operation_part
    http_pos = url_text.find('http')
    i = -1
    count = 0
    while True:
        i = url_text.find('/', i+1)
        if i == -1:
            i = url_text.find('?')
            return Row(repo=url_text[http_pos:i])
        else:
            count += 1
            if count == 6:
                return Row(repo=url_text[http_pos:i])

spark = SparkSession.builder \
            .master("spark://127.0.0.1:7077") \
            .appName("Spark with Postgres") \
            .config("spark.driver.extraClassPath", "postgresql-42.2.6.jar") \
            .config("spark.driver.memory", "2g") \
            .config("spark.executor.memory", "8g") \
            .config("spark.executor.instances", "2") \
            .config("spark.executor.cores", "6") \
            .getOrCreate()

def get_db_as_spark_dataframe(username, table_name):
    df = spark.read \
        .format("jdbc") \
        .option("url", "jdbc:postgresql://localhost:5432/postgres_db") \
        .option("dbtable", table_name) \
        .option("user", username) \
        .option("password", "bda_gh_torrent") \
        .load()
    
    return df

def task_2(df):
    print(df.count())

def task_3(df):
    print(df.filter(df.logging_level == 'WARN').count())

def task_4(df):
    print(df.filter(((df.logging_level == 'WARN') | (df.logging_level == 'INFO')) & (df.retrieval_stage == 'api_client') & (df.operation_part.contains('repos/'))).select(df.operation_part).distinct().rdd.map(eliminate_extra_info).distinct().count())
    
def task_5(df):
    df.filter(df.retrieval_stage == 'api_client').groupBy(df.downloader_id).agg(count(df.downloader_id).alias("frequency")).orderBy(desc("frequency")).show(10)

def task_6(df):
    df.filter((df.retrieval_stage == 'api_client') & (df.operation_part.contains('Failed'))).groupBy(df.downloader_id).agg(count(df.downloader_id).alias("frequency")).orderBy(desc("frequency")).show(10)

def task_7(df):
    df.withColumn("hour", col("timestamp").substr(instr(col("timestamp"), 'T')+1, lit(2))).groupBy("hour").agg(count("hour").alias("frequency")).orderBy(desc("frequency")).show(1)

def task_8(df):
    df.filter((df.logging_level == 'WARN') & (df.retrieval_stage == 'api_client') & (df.operation_part.contains('repos/'))).select(df.operation_part).rdd.map(eliminate_extra_info).toDF().groupBy("repo").agg(count("repo").alias("frequency")).orderBy(desc("frequency")).show(1, False)

def task_9(df):
    df.filter((df.retrieval_stage != 'geolocator') & (df.operation_part.contains('Failed'))).withColumn("access_key", col("operation_part").substr(instr(col("operation_part"), 'Access')+8, lit(11))).groupBy("access_key").agg(count("access_key").alias("frequency")).orderBy(desc("frequency")).show(10)

def task_10(connection, cursor):
    create_index_query = ("CREATE INDEX IF NOT EXISTS downloader_id_index "
                          "ON bda_gh_torrent (downloader_id)"
                          )
    cursor.execute(create_index_query)
    connection.commit()
    df = get_db_as_spark_dataframe('madhur', 'bda_gh_torrent')
    start = time.time()
    print(df.filter((df.downloader_id == 'ghtorrent-22') & ((df.logging_level == 'WARN') | (df.logging_level == 'INFO')) & (df.retrieval_stage == 'api_client') & (df.operation_part.contains('repos/'))).select(df.operation_part).distinct().rdd.map(eliminate_extra_info).distinct().count())
    end = time.time()
    print(end - start)

def task_11(connection, cursor):
    drop_index_query = "DROP INDEX downloader_id_index"
    cursor.execute(drop_index_query)
    connection.commit()
    df = get_db_as_spark_dataframe('madhur', 'bda_gh_torrent')
    start = time.time()
    print(df.filter((df.downloader_id == 'ghtorrent-22') & ((df.logging_level == 'WARN') | (df.logging_level == 'INFO')) & (df.retrieval_stage == 'api_client') & (df.operation_part.contains('repos/'))).select(df.operation_part).distinct().rdd.map(eliminate_extra_info).distinct().count())
    end = time.time()
    print(end - start)

def task_12(df2):
    print(df2.count())

def task_13(df, df2):
    condition = df.operation_part.contains(df2.url)
    print(df.filter(((df.logging_level == 'WARN') | (df.logging_level == 'INFO')) & (df.retrieval_stage == 'api_client')).join(df2, condition, 'inner').count())

def task_14(df, df2):
    condition = df.operation_part.contains(df2.url)
    df.filter(((df.logging_level == 'WARN') | (df.logging_level == 'INFO')) & (df.retrieval_stage == 'api_client') & (df.operation_part.contains('Failed'))).join(df2, condition, 'inner').groupBy(df2.url).agg(count(df2.url).alias("frequency")).orderBy(desc("frequency")).show(1, False)

if __name__ == '__main__':
    db_name = 'postgres_db'
    user = 'madhur'
    password = 'bda_gh_torrent'
    connection, cursor = connect_to_db(db_name, user, password)
    df = get_db_as_spark_dataframe(user, 'bda_gh_torrent')
    df2 = get_db_as_spark_dataframe(user, 'interesting')
    print("-----TASK 2-----")
    task_2(df)
    print()
    print("-----TASK 3-----")
    task_3(df)
    print()
    print("-----TASK 4-----")
    task_4(df)
    print()
    print("-----TASK 5-----")
    task_5(df)
    print()
    print("-----TASK 6-----")
    task_6(df)
    print()
    print("-----TASK 7-----")
    task_7(df)
    print()
    print("-----TASK 8-----")
    task_8(df)
    print()
    print("-----TASK 9-----")
    task_9(df)
    print()
    print("-----TASK 10-----")
    task_10(connection, cursor)
    print()
    print("-----TASK 11-----")
    task_11(connection, cursor)
    print()
    print("-----TASK 12-----")
    task_12(df2)
    print()
    print("-----TASK 13-----")
    task_13(df, df2)
    print()
    print("-----TASK 14-----")
    task_14(df, df2)
    print()
    close_connection_to_db(connection, cursor)

