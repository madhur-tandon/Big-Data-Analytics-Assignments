from pyspark.sql import SparkSession, types, Row

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
            return url_text[http_pos:i]
        else:
            count += 1
            if count == 6:
                return url_text[http_pos:i]

spark = SparkSession.builder \
            .master("local") \
            .appName("Spark with Postgres") \
            .config("spark.driver.extraClassPath", "postgresql-42.2.6.jar") \
            .getOrCreate()

df = spark.read \
    .format("jdbc") \
    .option("url", "jdbc:postgresql://localhost:5432/postgres_db") \
    .option("dbtable", "bda_gh_torrent") \
    .option("user", "madhur") \
    .option("password", "bda_gh_torrent") \
    .load()

def task_2():
    print(df.count())

def task_3():
    print(df.filter(df.logging_level == 'WARN').count())

def task_4():
    print(df.filter(((df.logging_level == 'WARN') | (df.logging_level == 'INFO')) & (df.retrieval_stage == 'api_client') & (df.operation_part.contains('repos/'))).select(df.operation_part).distinct().rdd.map(eliminate_extra_info).distinct().count())
    

if __name__ == '__main__':
    print("-----TASK 2-----")
    task_2()
    print()
    print("-----TASK 3-----")
    task_3()
    print()
    print("-----TASK 4-----")
    task_4()
    print()

