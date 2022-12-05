import findspark
import logging
import os

findspark.init()
print(findspark.find())

from pyspark.sql import SparkSession
import pyspark.pandas as ps


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