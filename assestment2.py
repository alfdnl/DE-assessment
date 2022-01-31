"""
QUESTION:
  Given an API that contain data as below :
  https://randomuser.me/api/0.8/?results=1000

  Write a spark code to produce an output like below :
  https://gitlab.com/im-batman/myassestment/-/tree/master/assestment_2nd_total_cnt    ## Single CSV file contain total count
  https://gitlab.com/im-batman/myassestment/-/tree/master/assestment_2nd_pqt          ## Parquet file partition by gender and state

  Example output of parquet file are like below
  +--------------------+------+------------+----------+------+--------------+----------+----------------+----------+-------------------+
  |               email|gender|        city|     state|  last|         phone|registered|        username|       dob|       current_time|
  +--------------------+------+------------+----------+------+--------------+----------+----------------+----------+-------------------+
  |piper.wang@exampl...|female|  lower hutt| southland|  wang|(609)-250-9520|1258039450|organicrabbit927| 200099271|2021-03-11 11:14:39|
  |mackenzie.chen@ex...|female|    auckland|  gisborne|  chen|(408)-993-1967| 957372389|      bluecat416| 434114864|2021-03-11 11:14:39|
  |willow.taylor@exa...|female|new plymouth|wellington|taylor|(678)-224-6132|1027132841|    whiteswan853| 715792352|2021-03-11 11:14:39|
  |hannah.davies@exa...|female|    gisborne| northland|davies|(584)-411-3519| 915793891|ticklishkoala505| 632753972|2021-03-11 11:14:39|
  |jackson.wright@ex...|  male|  wellington|  gisborne|wright|(138)-814-9811|1389882539|     tinyfish981|1004565351|2021-03-11 11:14:39|
  +--------------------+------+------------+----------+------+--------------+----------+----------------+----------+-------------------+

USE CASE:
  - Get total count of user group by gender and email_provider and save as CSV file
  - Produce list of data like example above and save it as parquet file partition by gender and state
  - current_time column are base on MYT time
  - parquet file are base on single thread

HINT:
  - You can follow below step to setup Spark in the environment:
      Click on Shell tab, execute below command
          wget https://downloads.apache.org/spark/spark-3.0.2/spark-3.0.2-bin-hadoop2.7.tgz && tar -zxf spark-3.0.2-bin-hadoop2.7.tgz         
          export SPARK_LOCAL_IP=127.0.0.1
          export SPARK_MAJOR_VERSION=3
          export SPARK_HOME=/home/runner/myAssestment/spark-3.0.2-bin-hadoop2.7
          export JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64
          export PYTHONPATH=$SPARK_HOME/python
          export PYSPARK_PYTHON=python
          export PATH=$JAVA_HOME/bin:/usr/local/bin:$SPARK_HOME/bin:$PATH:~/.local/bin:$PATH
          pyspark --version   


"""
#!/usr/bin/env python
# coding: utf-8
import requests
from pyspark.sql import SparkSession
from pandas.io.json import json_normalize
import pandas as pd
from pyspark.sql.functions import explode, col, lit, udf
from pyspark.sql import functions as F
from pyspark.sql.types import StringType


def fetch_data_from_api(url="https://randomuser.me/api/0.8/?results=1000") -> dict:
    """
    Function to fetch data from api by giving url

    @return dict
    """
    response = requests.get(url)
    resp_json = response.json()
    return resp_json["results"]


def normalize_data(data) -> list:
    """
    Function to flatten dictionary data:

    @return list
    """
    data_norm = [i["user"] for i in data]
    return data_norm


def main():
    data = fetch_data_from_api()
    data_norm = normalize_data(data)

    # create spark session
    spark = SparkSession.builder.appName("assesment 2").getOrCreate()
    # create spark dataFrame
    df = spark.createDataFrame(data_norm)
    write_df_to_csv(df)
    write_df_to_parquet(df)


def write_df_to_csv(df):
    """
    Function to processed dataframe and write it to csv file.
    """
    # apply function to get email provider data
    udf_email_add = udf(lambda x: email_provider_splitter(x), StringType())
    df_email_provider = df.withColumn(
        "email_provider", udf_email_add(col("email"))
    ).select("gender", "email_provider")
    # Groupby gender and email_provider
    df_email_provider = df_email_provider.groupBy("gender", "email_provider").count()
    # write df to csv
    df_email_provider.write.csv("assessment_2/csv/", mode="overwrite", header="true")


def email_provider_splitter(email_add) -> str:
    """
    Function to split email provider from email address

    @return String
    """
    return email_add.split("@")[-1]


def write_df_to_parquet(df):
    """
    Function to processed dataframe and write it to parquet file partitioned by gender and state.
    """
    # Select the required columns
    df_to_dump = df.select(
        "email",
        "gender",
        col("location.city"),
        col("location.state"),
        col("name.last"),
        "phone",
        "registered",
        "username",
        "dob",
    )
    # Create current time column with Asia/Kuala_Lumpur timezone
    df_to_dump = df_to_dump.withColumn("current_time", F.current_timestamp())
    df_to_dump = df_to_dump.withColumn(
        "current_time", F.from_utc_timestamp(col("current_time"), "Asia/Kuala_Lumpur")
    )
    # Write df to parquet file partionedBy gender and state
    df_to_dump.repartition(1).write.partitionBy("gender", "state").format(
        "parquet"
    ).mode("overwrite").save("assessment_2/parquet/")


if __name__ == "__main__":
    main()
