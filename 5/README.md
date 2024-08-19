## #5

### Данные в кафке

![kafka-topic](./img/kafka-topic.png)

### Данные в клике до запуска

![ch-table-empty](./img/ch-table-empty.png)

### Запуск спарка + в UI

```bash
pip install clickhouse_driver pandas
spark-submit --master spark://spark:8080 --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0 --executor-cores 1 --conf spark.driver.extraJavaOptions="-Divy.cache.dir=/tmp -Divy.home=/tmp" /opt/spark/apps/spark.py
```

![spark-submit](./img/spark-submit.png)
![spark-ui](./img/spark-ui.png)

### Данные в клике после запуска

![ch-table-empty](./img/ch-table.png)
