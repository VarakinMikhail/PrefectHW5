
Задание: 
В рамках потока (prefect) или ОАГ (Airflow) реализовать процесс обработки данных, состоящий из следующих шагов:
1. Запустить сессию Apache Spark под управлением YARN в рамках кластера, развернутого в предыдущих заданиях
2. Подключиться к кластеру HDFS, развернутому в предыдущих заданиях
3. Используя Spark прочитать данные, которые были предварительно загружены на HDFS
4. Выполнить несколько трансформаций данных (например, агрегацию или преобразование типов)
5. Сохранить данные как таблицу

Данные: 

узел для входа 176.109.81.242 

jn 192.168.1.106 

nn 192.168.1.107 

dn-00 192.168.1.109 

dn-01 192.168.1.108

Пререквизиты: выполнены инструкции из предыдущих заданий (в частности, их пререквизиты)


Подключаемся к ноде:
```
ssh team@176.109.81.242
```

переходим на пользователя hadoop:
```
sudo -i -u hadoop
```

Установим prefect:
```
pip install prefect
```
Теперь можем создавать процесс.
Создаем файл со следующим содержимым (и именем, например process_data.py):
```
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from onetl.connection import SparkHDFS
from onetl.connection import Hive
from onetl.file import FileDFReader
from onetl.file.format import CSV
from onetl.db import DBWriter
from prefect import flow, task

@task
def get_spark():
    spark = SparkSession.builder \
        .master("yarn") \
        .appName("spark-with-yarn") \
        .config("spark.sql.warehouse.dir", "/user/hive/warehouse") \
        .config("spark.hive.metastore.uris", "thrift://tmpl-jn:9083") \
        .enableHiveSupport() \
        .getOrCreate()
    return spark


@task
def stop_spark(spark):
    spark.stop()


@task
def extract(spark):
    hdfs = SparkHDFS(host="tmpl-nn", port=9000, spark=spark, cluster="test")
    reader = FileDFReader(connection=hdfs, format=CSV(delimiter=",", header=True), source_path="/input")
    df = reader.run(["for_spark.csv"])
    return df


@task
def transform(df):
    df = df.withColumn("reg_year", F.col("registration date").substr(0, 4))
    return df


@task
def load(spark, df):
    hive = Hive(spark=spark, cluster="test")
    writer = DBWriter(connection=hive, table="test.spark_parts", options={"if_exists": "replace_entire_table", "partitionBy": "reg_year"})
    writer.run(df)


@flow
def process_data():
    spark = get_spark()
    df = extract(spark)
    df = transform(df)
    load(spark, df)
    stop_spark(spark)

if __name__ == "__main__":
    process_data()
```

И выполняем его:
```
python process_data.py
```
