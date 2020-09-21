from airflow import DAG
from datetime import datetime, timedelta
from airflow.sensors.http_sensor import HttpSensor
from airflow.contrib.sensors.file_sensor import FileSensor
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash_operator import BashOperator
from airflow.operators.hive_operator import HiveOperator
from airflow.contrib.operators.spark_submit_operator import SparkSubmitOperator
from airflow.operators.email_operator import EmailOperator

import json
import csv
import requests

# define default args dictionary that will be used in dags
default_args = {
    "owner": "airflow",
    "start_date": datetime(2020, 9, 19), # past time makes the dag be triggered immediately
    "depends_on_past": False, # tells a task to run evenif the last run of that task has failed
    "email_on_failure": False,
    "email_on_retry": False,
    "email": "youremail@xxxmail.com",
    "retries": 1, # task should be restarted at least once after a failure
    "retry_delay": timedelta(minutes = 5) # and the scheduler should wait 5 min before restarting
}

def download_rates():
    with open('/usr/local/airflow/dags/files/forex_currencies.csv') as forex_currencies:
        reader = csv.DictReader(forex_currencies, delimiter=';')
        for row in reader:
            base = row['base']
            with_pairs = row['with_pairs'].split(' ')
            indata = requests.get('https://api.exchangeratesapi.io/latest?base=' + base).json()
            outdata = {'base': base, 'rates': {}, 'last_update': indata['date']}
            for pair in with_pairs:
                outdata['rates'][pair] = indata['rates'][pair]
            with open('/usr/local/airflow/dags/files/forex_rates.json', 'a') as outfile:
                json.dump(outdata, outfile)
                outfile.write('\n')

# instantiate a DAG object
# note that every task need task_id, and if we use sensors, better define the poke_interval and timeout
# is_forex_rates_available task is to check if the forex API to get the rates data is available
# https://exchangeratesapi.io
# only use this one: https://api.exchangeratesapi.io/latest
with DAG(dag_id = "forex_data_pipeline",
        schedule_interval = "@daily",
        default_args = default_args,
        catchup = False) as dag: # None if we havnt defined any tasks yet
    is_forex_rates_available = HttpSensor(
        task_id = "is_forex_rates_available",
        method = 'GET',
        http_conn_id = 'forex_api', # link we want to check, in this case is the name of the connection we gonna create in airflow
        endpoint = 'latest',
        response_check =  lambda response: "rates" in response.text, # give a lambda func returning boolean indicating if we get the expected response from the sensor
        poke_interval = 5, # the sensor should send a http request every 5s
        timeout = 20 # max waiting time
    )
# next task check if the file containing the currencies is available
# use FileSensor, to check if data arrived into the specific location
    is_forex_currencies_file_available = FileSensor(
        task_id = "is_forex_currencies_file_available",
        fs_conn_id = "forex_path",
        filepath = "forex_currencies.csv",
        poke_interval = 5,
        timeout = 20
    )
# next task, dowloading the data
# use PythonOperator, which allows you to exec python callable func
# in this case we use above udf
    downloading_rates = PythonOperator(
        task_id = "downloading_rates",
        python_callable = download_rates
    )
# next task, BashOperator to save the rates data into HDFS
# goto hue container port to see the result
    saving_rates = BashOperator(
        task_id = "saving_rates",
        bash_command = """
            hdfs dfs -mkdir -p /forex &&\
            hdfs dfs -put -f $AIRFLOW_HOME/dags/files/forex_rates.json /forex
        """
    )
# next, create hice table using HiveOperator
# goto hue container port to see the result
    creating_hive_table = HiveOperator(
        task_id = "creating_hive_table",
        hive_cli_conn_id = "hive_conn", # note that this connection created in the dashboard based on one of our container (hive-server)
        hql = """
            CREATE EXTERNAL TABLE IF NOT EXISTS forex_rates(
                base STRING,
                last_update DATE,
                eur DOUBLE,
                usd DOUBLE,
                nzd DOUBLE,
                gbp DOUBLE,
                jpy DOUBLE,
                cad DOUBLE
                )
            ROW FORMAT DELIMITED
            FIELDS TERMINATED BY ','
            STORED AS TEXTFILE
        """
    )
# next processing data with spark
# SparkSubmitOperator allows you to kick off a spark-submit job
# after this job the data in the warehouse(hive) is clean
    processing_data = SparkSubmitOperator(
        task_id = "processing_data",
        conn_id = "spark_conn",
        application = "/usr/local/airflow/dags/scripts/forex_processing.py",
        verbose = False
    )
# send email notification
# first step is to generate an app password to be able to send emails with your account
# e.g using gmail: http://security.google.com/settings/security/apppasswords
# configure airflow.cfg smtp
    email_notification = EmailOperator(
        task_id = "email_notification",
        to = "someone@gmail.com",
        subject = "this is an important message",
        html_content = "<h3> Just kidding it actually is a spam!</h3> "

    )
# we have implemented all the task, now we need to add the dependencies between them
    is_forex_rates_available >> is_forex_currencies_file_available >> downloading_rates \
    >> saving_rates >> creating_hive_table >> processing_data >> email_notification
