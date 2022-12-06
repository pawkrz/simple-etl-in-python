# Setup
- Windows platform
- [spark 3.2.3](https://spark.apache.org/downloads.html)
- [MovieLens 1M Dataset](https://grouplens.org/datasets/movielens/1m/)
- docker desktop

## INSTALL VIRTUAL ENVIRONMENT
```s
python -m venv venv
```

## ACTIVATE VIRTUAL ENVIRONMENT
```s
.\venv\Scripts\activate
```

## INSTALL PYTHON MODULES
```s
pip install -r requirements.txt
```

# RUN ETL JOB IN DOCKER USING AIRFLOW
```s
docker build -t pawkrz/etl .

docker run -d -p 8080:8080 --name etl pawkrz/etl
```




