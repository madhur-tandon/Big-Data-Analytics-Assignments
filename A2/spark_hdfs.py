from pyspark.sql import SparkSession, types, Row
from pyspark.sql.functions import desc, count, instr, lit, col
import time

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

def clean_up_text(record):
    try:
        standard_part, operation_part = record.value.split('.rb: ')
        logging_level, timestamp, other = standard_part.split(', ')
        downloader_id, retrieval_stage = other.split(' -- ')

        if logging_level in ['DEBUG', 'INFO', 'WARN', 'ERROR']:
            record_row = Row(
                logging_level=logging_level,
                timestamp=timestamp,
                downloader_id=downloader_id,
                retrieval_stage=retrieval_stage,
                operation_part=operation_part
                )
            return record_row
    except Exception as e:
        return None

spark = SparkSession.builder \
            .master("local[*]") \
            .appName("Spark with HDFS") \
            .getOrCreate()

raw_df = spark.read \
     .format("text") \
     .load("hdfs://localhost/madhur/input/ghtorrent-logs.txt")

df2 = spark.read \
      .format("csv") \
      .option("header", "true") \
      .load("hdfs://localhost/madhur/input/important-repos.csv")

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

def task_10(df):
    start = time.time()
    print(df.filter((df.downloader_id == 'ghtorrent-22') & ((df.logging_level == 'WARN') | (df.logging_level == 'INFO')) & (df.retrieval_stage == 'api_client') & (df.operation_part.contains('repos/'))).select(df.operation_part).distinct().rdd.map(eliminate_extra_info).distinct().count())
    end = time.time()
    print(end - start)

def task_11(df):
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
    clean_rdd = raw_df.rdd.map(clean_up_text).filter(lambda x: x is not None)
    df = spark.createDataFrame(clean_rdd)

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
    task_10(df)
    print()
    print("-----TASK 11-----")
    task_11(df)
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


