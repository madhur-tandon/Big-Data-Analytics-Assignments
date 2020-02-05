from pyspark.sql import SparkSession, types, Row
from pyspark.sql.functions import desc, count, substring
from pymongo import MongoClient
import pandas as pd

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

def load_file_into_db(filename, db_name, collection_name):
    client = MongoClient()
    db = client[db_name]
    collection = db[collection_name]
    if collection_name in db.list_collection_names():
        collection.drop()
        collection = db[collection_name]
        with open(filename) as file:
            data = file.read()
            all_rows = data.split('\n')
            for each_row in all_rows:
                try:
                    standard_part, operation_part = each_row.split('.rb: ')
                    logging_level, timestamp, other = standard_part.split(', ')
                    downloader_id, retrieval_stage = other.split(' -- ')

                    if logging_level in ['DEBUG', 'INFO', 'WARN', 'ERROR']:
                        record_dict = {
                            'logging_level': logging_level,
                            'timestamp': timestamp,
                            'downloader_id': downloader_id,
                            'retrieval_stage': retrieval_stage,
                            'operation_part': operation_part
                        }

                        collection.insert_one(record_dict)
                except Exception as e:
                    print('Error: {0} {1}'.format(e, each_row))
    
        return db, collection

def load_csv_into_db(filename, db_name, collection_name):
    client = MongoClient()
    db = client[db_name]
    collection = db[collection_name]
    df = pd.read_csv(filename)
    all_records = df.to_dict('records')
    if collection_name in db.list_collection_names():
        collection.drop()
        collection = db[collection_name]
    collection.insert_many(all_records)
    return db, collection

spark = SparkSession.builder \
            .master("local") \
            .appName("Spark_with_MongoDB") \
            .config("spark.jars.packages", "org.mongodb.spark:mongo-spark-connector_2.11:2.4.1") \
            .getOrCreate()

def get_df_from_db(db_name, collection_name):
    df = spark.read \
        .format("mongo") \
        .option("uri", "mongodb://localhost:27017/") \
        .option("database", db_name) \
        .option("collection", collection_name) \
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
    # USES Substring, takes a lot of time, find alternate solution
    df.select(substring(df.timestamp, 12, 2).alias("hour")).groupBy("hour").agg(count("hour").alias("frequency")).orderBy(desc("frequency")).show(1)

def task_8(df):
    df.filter((df.logging_level == 'WARN') & (df.retrieval_stage == 'api_client') & (df.operation_part.contains('repos/'))).select(df.operation_part).rdd.map(eliminate_extra_info).toDF().groupBy("repo").agg(count("repo").alias("frequency")).orderBy(desc("frequency")).show(1, False)

if __name__ == '__main__':
    filename = 'ghtorrent-logs.txt'
    db_name = 'bda_mongo_db'
    collection_name = 'bda_gh_torrent'

    filename_task_12 = 'important-repos.csv'
    collection_name_task_12 = 'interesting'

    # db, collection = load_file_into_db(filename, db_name, collection_name)
    # db, collection = load_csv_into_db(filename_task_12, db_name, collection_name_task_12)
    df = get_df_from_db(db_name, collection_name)
    df2 = get_df_from_db(db_name, collection_name_task_12)

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

    # client = MongoClient()
    # db = client[db_name]
    # collection = db[collection_name]
    # collection_interesting = db[collection_name_task_12]

    # print(collection.count_documents({}))
    # print(collection_interesting.count_documents({}))
