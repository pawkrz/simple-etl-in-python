import pandas as pd
import sqlite3 as db
import logging

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