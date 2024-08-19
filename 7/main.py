from airflow import DAG
from airflow.operators.python import PythonOperator
from clickhouse_driver import Client
import psycopg2
import os
import json
import pandas as pd

ch_connection_string = os.environ.get('CH_CONNECTION_STRING', 'clickhouse://admin:admin@clickhouse:9000/db')
pg_connection_string = os.environ.get('PG_CONNECTION_STRING', 'postgres://airflow:airflow@postgres:5432/airflow')

query = f"""
select
    shk_id,
    dt,
    employee_id,
    place_cod,
    tare_id,
    tare_type,
    isdeleted,
    isstock,
    chrt_id,
    state_id,
    transfer_box_id,
    correction_dt,
    wbsticker_id
from
    default.ttt
"""

reports_query = f"""
select
    state_id,
    qty,
    dt
from
    reports
"""

def task_transform():
    with Client.from_url(ch_connection_string) as ch:
        # query data
        df = ch.query_dataframe(query)

        # count states
        counts = df['state_id'].value_counts()
        states = pd.DataFrame(
            data={
                'state_id': counts.index.to_numpy(),
                'qty': counts.to_numpy(),
                'dt': pd.Timestamp.now(),
            }, 
            columns=['state_id', 'qty', 'dt'],
        )

        # save results
        ch.insert_dataframe('insert into reports values', states, settings={'use_numpy': True})

def task_import():
    with Client.from_url(ch_connection_string) as ch:
        df = ch.query_dataframe(reports_query)

    # print(df)
    print(df.to_json(orient='records', date_format='iso')  )

    with psycopg2.connect(pg_connection_string) as pg_conn:
        with pg_conn.cursor() as cur:
            df = df.to_json(orient='records', date_format='iso')            
            cur.execute(f"CALL report.state_count_import('{df}');")

dag = DAG(
    dag_id='homework-7',
    default_args={
        'owner': 'rodishevdd',
    },
    tags=[
        "airflow",
    ]
)

transform = PythonOperator(
    task_id='transform',
    python_callable=task_transform, 
    dag=dag,
)


import_ = PythonOperator(
    task_id='import',
    python_callable=task_import, 
    dag=dag,
)

transform >> import_
