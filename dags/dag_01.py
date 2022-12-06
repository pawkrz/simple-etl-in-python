from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import requests, zipfile, io
import pandas as pd
import sqlite3 as db
import logging
import findspark
import os
from pyspark.sql import SparkSession
import pyspark.pandas as ps


def pull():
    zip_file_url = 'https://files.grouplens.org/datasets/movielens/ml-1m.zip'
    r = requests.get(zip_file_url)
    z = zipfile.ZipFile(io.BytesIO(r.content))
    z.extractall("data/")

def extract():
    logging.basicConfig(format='%(message)s', level=logging.INFO)

    col_users = ['user_id', 'gender', 'age', 'occupation', 'zip']
    df_users = pd.read_table('data/ml-1m/users.dat', 
        sep='::', 
        header=None, 
        names=col_users, 
        engine='python', 
        encoding='latin-1')
    logging.info("df_users")
    logging.info(f"Count: {len(df_users)}")
    logging.info(df_users[:1])

    col_ratings = ['user_id', 'movie_id', 'rating', 'timestamp']
    df_ratings = pd.read_table('data/ml-1m/ratings.dat', 
        sep='::', 
        header=None, 
        names=col_ratings, 
        engine='python', 
        encoding='latin-1')
    logging.info("df_ratings")
    logging.info(f"Count: {len(df_ratings)}")
    logging.info(df_ratings[:1])

    col_movies = ['movie_id', 'title', 'genres']
    df_movies = pd.read_table('data/ml-1m/movies.dat', 
        sep='::', 
        header=None, 
        names=col_movies, 
        engine='python', 
        encoding='latin-1')
    logging.info("df_movies")
    logging.info(f"Count: {len(df_movies)}")
    logging.info(df_movies[:1])

    logging.info("Init sqlite db")
    con = db.connect('test.db')

    logging.info("Insert users and rating to db")
    df_users.to_sql(name='users', con=con, if_exists='replace', index=False)
    df_ratings.to_sql(name='ratings', con=con, if_exists='replace', index=False)
    print(pd.read_sql('select * from users limit 2', con))
    con.commit()
    con.close()

    logging.info("Dump movies to json")
    df_movies.to_json("movies.json", orient='records', lines=True)

    logging.info("validate movies.json")
    df_movies = pd.read_json("movies.json", lines=True)
    logging.info(df_movies.head(1))

def load():
    findspark.init()
    print(findspark.find())

    spark = SparkSession.builder.master("local[*]") \
                        .appName('test') \
                        .config("spark.sql.warehouse.dir", "./spark-warehouse") \
                        .config("spark.executor.memory", "2g") \
                        .config("spark.driver.memory", "4g") \
                        .config("spark.jars","jars/sqlite-jdbc-3.34.0.jar") \
                        .config("spark.driver.extraClassPath","jars/sqlite-jdbc-3.34.0.jar") \
                        .enableHiveSupport() \
                        .getOrCreate()

    df_movies = spark.read.format("json").load("movies.json").createOrReplaceTempView("movies")

    pdf_users = ps.read_sql("users", con="jdbc:sqlite:{}/test.db".format(os.getcwd()))
    df_users = pdf_users.to_spark().createOrReplaceTempView("users")

    pdf_ratings = ps.read_sql("ratings", con="jdbc:sqlite:{}/test.db".format(os.getcwd()))
    df_ratings = pdf_ratings.to_spark().createOrReplaceTempView("ratings")

    age_hist = spark.sql("""
    select u.age, count(1) ratings, round(avg(r.rating), 2) as avg_rating
    from ratings r
    join users u on u.user_id = r.user_id
    group by u.age
    order by age
    """)
    age_hist.show(truncate=False)
    age_hist.write.saveAsTable("default.age_hist", mode="overwrite")
    spark.read.table("default.age_hist").show(1)

    top_100_movies = spark.sql("""
    select m.title, count(1) ratings, round(avg(r.rating), 2) as avg_rating
    from ratings r
    join movies m on m.movie_id = r.movie_id
    group by m.title
    having count(1) > 100
    order by avg_rating desc
    limit 100
    """)
    top_100_movies.show(truncate=False)
    top_100_movies.write.saveAsTable("default.top_100_movies", mode="overwrite")
    spark.read.table("default.top_100_movies").show(1)

dag = DAG(dag_id="etlWithPyspark",
         start_date=datetime(2022,1,1),
         schedule_interval=None,
         catchup=False) 

task1 = PythonOperator(
    task_id="pull",
    python_callable=pull,
    dag=dag)

task2 = PythonOperator(
    task_id="extract",
    python_callable=extract,
    dag=dag)

task3 = PythonOperator(
    task_id="load",
    python_callable=load,
    dag=dag)

task1 >> task2 >> task3