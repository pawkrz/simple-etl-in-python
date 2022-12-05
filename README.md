# Setup
- Windows platform
- [spark 3.2.3](https://spark.apache.org/downloads.html)
- [MovieLens 1M Dataset](https://grouplens.org/datasets/movielens/1m/)

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

## SETUP AIRLFOW
```s
set AIRFLOW_HOME=%cd%\airflow
airflow db init
```