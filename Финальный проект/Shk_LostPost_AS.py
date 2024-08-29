from airflow import DAG
from airflow.operators.python import PythonOperator
from Utils.clickhouse_hook import ClickHouseHook
from datetime import datetime
from airflow_clickhouse_plugin.operators.clickhouse import ClickHouseOperator

default_args = {
    'owner': 'Rodishev',
    'depends_on_past': False,
    'start_date': datetime(2024, 8, 5),
}

dag = DAG(
    dag_id='Shk_LostPost_AS',
    default_args=default_args,
    schedule_interval='*/10 * * * *',
    description='Даг обновляет данные в таблице Shk_LostPost_AS, в которой хранятся только активные списания (без оприходований)',
    catchup=False,
    max_active_runs=1,
    tags=["Rodishev"]
)
def select_client():
    try:
        client_CH = ClickHouseHook(
            clickhouse_conn_id='CH_1').get_conn()
        client_CH.execute('show databases')
        return client_CH
    except:
        print('wh-foreign-integrations-hard-01.nb.wb.ru - нет доступа')
    try:
        client_CH = ClickHouseHook(
            clickhouse_conn_id='CH_2').get_conn()
        client_CH.execute('show databases')
        return client_CH
    except:
        print('wh-foreign-integrations-hard-02.dm.wb.ru - нет доступа')
    raise Exception('Нет коннекта до БД')

def main():

    client_CH = select_client()

    client_CH.execute("""drop table if exists tmp.Shk_LostPost_last""")
    client_CH.execute("""drop table if exists tmp.Shk_LostPost_last_opr""")
    client_CH.execute("""drop table if exists tmp.Shk_LostPost_last_opr_iskl""")
    client_CH.execute("""drop table if exists tmp.Shk_LostPost_last_order""")

    # Определяем последнее событие из таблицы
    max_dt = client_CH.execute("""
    select max(dt_operation)
    from Shk_LostPost_AS
    where 1
      and dt_operation < now()::timestamp
    """)
    print(max_dt)
    if max_dt:
        for row in max_dt:
            dt_max = row[0]
            print(dt_max)
    # Забираем последние данные из оригинального Shk_LostPost
    client_CH.execute(f"""
    create table tmp.Shk_LostPost_last engine = MergeTree() order by shk_id as 
    select *
    from Shk_LostPost
    where 1
      and dt_operation > parseDateTimeBestEffortOrNull(\'{dt_max}\') 
    """)
    # Опредление оприходований
    client_CH.execute("""
    create table tmp.Shk_LostPost_last_opr engine = MergeTree() order by shk_id as 
    select shk_id
    from tmp.Shk_LostPost_last
    where 1
      and lostreason_id = 0
    """)
    # Исключение оприходований
    client_CH.execute("""
    create table tmp.Shk_LostPost_last_opr_iskl engine = MergeTree() order by shk_id as 
    select slpl.*
    from tmp.Shk_LostPost_last slpl
    left anti join tmp.Shk_LostPost_last_opr opr on slpl.shk_id = opr.shk_id
    """)
    # Сортируем для красоты и отбрасываем лишнее, если было два новых спсиания и более
    client_CH.execute("""
    create table tmp.Shk_LostPost_last_order engine = MergeTree() order by shk_id as 
    select *
    from tmp.Shk_LostPost_last_opr_iskl
    order by dt_operation desc
    limit 1 by shk_id
    """)
    # Заливаем подготовленные данные
    client_CH.execute("""
    insert into Shk_LostPost_AS 
    select *
    from tmp.Shk_LostPost_last_order
    """)
    # Делаем OPTIMIZE, чтобы не ждать чистки
    client_CH.execute("""
    optimize table Shk_LostPost_AS 
    """)


PythonOperator(task_id='Shk_LostPost_AS', python_callable=main, dag=dag)