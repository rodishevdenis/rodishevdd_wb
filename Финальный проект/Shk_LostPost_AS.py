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
    schedule_interval="@continuous",
    description='Даг обновляет данные в таблице Shk_LostPost_AS, в которой хранятся только активные списания (без оприходований) с последним статусом перед списанием',
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
    client_CH.execute("""drop table if exists tmp.Shk_LostPost_dt""")
    client_CH.execute("""drop table if exists tmp.Shk_LostPost_mer""")
    client_CH.execute("""drop table if exists tmp.Shk_LostPost_last""")
    client_CH.execute("""drop table if exists tmp.Shk_LostPost_last_opr""")
    client_CH.execute("""drop table if exists tmp.Shk_LostPost_last_opr_iskl""")
    client_CH.execute("""drop table if exists tmp.Shk_LostPost_last_order""")
    client_CH.execute("""drop table if exists tmp.ShkOnPlace_search""")
    client_CH.execute("""drop table if exists tmp.itog_for_ins""")

    # Определяем последнее событие из таблицы Shk_LostPost_not_parsed_itog
    max_dt = client_CH.execute("""
    select max(dt_load)
    from datamart.Shk_LostPost_AS
    """)
    print(max_dt)
    if max_dt:
        for row in max_dt:
            dt_max = row[0]
            print(dt_max)
    # Забираем не обработанные данные из Shk_LostPost_not_parsed_itog
    client_CH.execute(f"""
    -- Shk_LostPost_AS/Shk_LostPost_dt
    create table tmp.Shk_LostPost_dt engine = MergeTree() order by shk_id as
    select *
    from Shk_LostPost_not_parsed_itog
    where 1
      and dt_load > parseDateTimeBestEffortOrNull(\'{dt_max}\')
    """)
    # Забираем не обработанные данные из оригинального Shk_LostPost
    client_CH.execute(f"""
    -- Shk_LostPost_AS/Shk_LostPost_last
    create table tmp.Shk_LostPost_last engine = MergeTree() order by shk_id as 
    select *
    from Shk_LostPost
    where 1
      and shk_id in (select shk_id from Shk_LostPost_not_parsed_itog)
    """)
    # Склеиваем данные
    client_CH.execute(f"""
    -- Shk_LostPost_AS/Shk_LostPost_mer
    create table tmp.Shk_LostPost_mer engine = MergeTree() order by shk_id as
    select 
      l.*,
      dt.dt_load
    from tmp.Shk_LostPost_last l
    join tmp.Shk_LostPost_dt dt on dt.shk_id = l.shk_id
    """)
    # Опредление оприходований
    client_CH.execute("""
    -- Shk_LostPost_AS/Shk_LostPost_last_opr
    create table tmp.Shk_LostPost_last_opr engine = MergeTree() order by shk_id as 
    select shk_id
    from tmp.Shk_LostPost_mer
    where 1
      and lostreason_id = 0
    """)
    # Исключение оприходований
    client_CH.execute("""
    -- Shk_LostPost_AS/Shk_LostPost_last_opr_iskl
    create table tmp.Shk_LostPost_last_opr_iskl engine = MergeTree() order by shk_id as 
    select mer.*
    from tmp.Shk_LostPost_mer mer
    left anti join tmp.Shk_LostPost_last_opr opr on mer.shk_id = opr.shk_id
    """)
    # Сортируем для красоты и отбрасываем лишнее, если было два новых спсиания и более
    client_CH.execute("""
    -- Shk_LostPost_AS/Shk_LostPost_last_order
    create table tmp.Shk_LostPost_last_order engine = MergeTree() order by shk_id as 
    select *
    from tmp.Shk_LostPost_last_opr_iskl
    order by dt_operation desc
    limit 1 by shk_id
    """)
    # Поиск последнего статуса пеед списанием
    client_CH.execute("""
    -- Shk_LostPost_AS/ShkOnPlace_search
    create table tmp.ShkOnPlace_search engine = MergeTree() order by shk_id as 
    select 
      shk_id,
      state_id,
      dt dt_state_id
    from ShkOnPlaceState_log
    where 1
      and shk_id in (select shk_id from tmp.Shk_LostPost_last_order)
      and state_id not in ('WOG','')
    order by dt_state_id desc
    limit 1 by shk_id
    """)
    # Подготовка таблицы для заливки
    client_CH.execute("""
    -- Shk_LostPost_AS/itog_for_ins
    create table tmp.itog_for_ins engine = MergeTree() order by shk_id as 
    select 
      ord.*,
      sr.state_id,
      sr.dt_state_id
    from tmp.Shk_LostPost_last_order ord
    left join tmp.ShkOnPlace_search sr on sr.shk_id = ord.shk_id
    """)
    # Заливаем подготовленные данные
    client_CH.execute("""
    -- Shk_LostPost_AS/insert into Shk_LostPost_AS
    insert into datamart.Shk_LostPost_AS 
    select *
    from tmp.itog_for_ins
    """)
    # Делаем OPTIMIZE, чтобы не ждать чистки
    client_CH.execute("""
    -- Shk_LostPost_AS/optimize
    optimize table datamart.Shk_LostPost_AS 
    """)


PythonOperator(task_id='Shk_LostPost_AS', python_callable=main, dag=dag)