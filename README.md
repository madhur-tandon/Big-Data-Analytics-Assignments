# BDA Assignments

## For Assignment 1
```
python bda.py
```

---

## For Assignment 2
### Start Master

```
$SPARK_HOME/sbin/start-master.sh
```

### Start Slave

```
$SPARK_HOME/sbin/start-slave.sh spark://127.0.0.1:7077 --host 127.0.0.1
```

### NumExecutors

--total-executor-cores X --executor-cores Y --executor-memory 8G

then, number of executors = X/Y and each executor has 8 GB of memory

### For Postgres, Submit Job to Spark

```
spark-submit --master spark://127.0.0.1:7077 --jars ./postgresql-42.2.6.jar --driver-class-path ./postgresql-42.2.6.jar --total-executor-cores 12 --executor-cores 6 --executor-memory 8G spark_postgres.py
```

### For MongoDB

#### Start MongoDB
```
mongod --dbpath /usr/local/var/mongodb
```

#### Submit Job to Spark
```
spark-submit --master spark://127.0.0.1:7077 --packages org.mongodb.spark:mongo-spark-connector_2.11:2.4.1 --total-executor-cores 12 --executor-cores 6 --executor-memory 8G spark_mongo.py
```

### For HDFS

#### Start HDFS
```
hstart
```

#### Submit Job to Spark
```
spark-submit --master spark://127.0.0.1:7077 --total-executor-cores 12 --executor-cores 6 --executor-memory 8G spark_hdfs.py
```
#### Stop HDFS
```
hstop
```

### Observe Spark Master UI and Spark UI

[http://localhost:8080/](http://localhost:8080/)

[http://127.0.0.1:4040/executors/](http://127.0.0.1:4040/executors/)

### Stop Slave

```
$SPARK_HOME/sbin/stop-slave.sh spark://127.0.0.1:7077 --host 127.0.0.1
```

### Stop Master

```
$SPARK_HOME/sbin/stop-master.sh
```

---

## For Assignment 3

1. Run the following commands in separate terminals
    - `storm dev-zookeeper`
    - `storm nimbus`
    - `storm supervisor`
    - `storm ui`

2. Start Redis in yet another terminal
    - `redis-server`

3. Clear the data present in Redis
    - `redis-cli`
    - `FLUSHALL`
    - `EXIT`

4. Go to the project folder using `cd`

4. Submit the Topology to Apache Storm
    - `sparse run -n surprise_number_twitter`

5. Run the Flask server
    - `python app.py`

6. Open up `localhost:8772` and `localhost:5000` in separate tabs in the browser

7. Enjoy the Live Chart
