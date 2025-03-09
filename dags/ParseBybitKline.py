import pandas as pd
import pytz
import time
import datetime as datetime
import re
import math
import requests
import numpy as np
from clickhouse_driver import Client

from airflow import DAG
from airflow.operators.python import PythonOperator

default_args = {
    'owner': 'airflow',
    'start_date': datetime.datetime(2022, 7, 13)
}

dag = DAG(
    dag_id='ParseBybitKline',
    default_args=default_args,
    schedule_interval='5,55 * * * *',
    description='ETL Kline с Bybit',
    catchup=False,
    max_active_runs=1,
    tags=["MYJOB"]
)



# запрос на получение данных - нужны 4 параметра: тикер, таймфрейм, дата начала, дата конца данных
def req(symbol, interval, start_time, end_time):
    url = f"https://api.bybit.com/derivatives/v3/public/kline?category=linear&symbol={symbol}&interval={interval}&start={start_time}&end={end_time}"
    resp = requests.get(url)
    if resp.status_code == 200:
        return resp
    else:
        raise ConnectionError("request failed")

# функция приводит данные запросы в нужный вид датафрейма
def parse_resp(resp):
    df = pd.DataFrame(data=resp, columns=["open_time", "open", "high", "low", "close", "volume", "turnover"])
    df["open_time"] = df["open_time"].astype(int)
    df["time"] = pd.to_datetime(df["open_time"], unit='ms')
    df["time"] = df["time"] + pd.Timedelta(hours=3)
    df[["open", "high", "low", "close"]] = df[["open", "high", "low", "close"]].astype(np.float64)
    df.drop(["open_time", "turnover"], axis=1, inplace=True)
    return df

# функция получения данных, простенькая, без CDC
def get_data(symbol):
    tz_moscow = pytz.timezone("Europe/Moscow")
    now = datetime.datetime.now(tz_moscow)
    now = now.strftime('%Y-%m-%d %H')
    now = datetime.datetime.strptime(now, '%Y-%m-%d %H')
    now = int(now.timestamp()) * 1000
    start_time = now - 114000000 # без CDC, нахлестом в 24 часа
    end_time = now
    resp = req(symbol=symbol + "USDT", interval="60", start_time=start_time, end_time=end_time)
    resp = resp.json()
    df = parse_resp(resp["result"]["list"])
    dt_load = datetime.datetime.now()
    dt_load = dt_load + datetime.timedelta(hours=3)
    dt_load = dt_load.strftime("%Y-%m-%d %H:%M:%S")
    df = df.assign(dt_load = dt_load)
    df.insert(0, "ticker", symbol)
    return df

# функция коннект + вставка данных, клик (ну или куда там надо) поднимается отдельно
def insert(symbol):
    client = Client('some-clickhouse-server',
                    user='airflow',
                    password='airflow',
                    port=9000,
                    verify=False,
                    database='default',
                    settings={"numpy_columns": False, 'use_numpy': True},
                    compression=False)

    df = get_data(symbol)
    client.insert_dataframe(f'INSERT INTO default.TickerPriceUSDT VALUES', df)


def main():
    list_ticker = ["BTC", "ETH"]
    for ticker in list_ticker:
        insert(ticker)

task1 = PythonOperator(
    task_id='ParseBybitKline_task', python_callable=main, dag=dag)