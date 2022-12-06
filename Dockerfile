FROM python:3.8-slim

COPY requirements.txt /requirements.txt

RUN pip3 install -r requirements.txt --no-cache-dir

RUN apt update
RUN apt install default-jdk -y

RUN export AIRFLOW_HOME=~/airflow
RUN airflow db init

RUN airflow users create --username admin --firstname admin --lastname testing --role Admin --email admin@domain.com --password admin

COPY airflow.cfg root/airflow/airflow.cfg
COPY dags/ root/airflow/dags/
COPY . .

EXPOSE 8080

CMD ["/bin/bash","-c","./init.sh"]

