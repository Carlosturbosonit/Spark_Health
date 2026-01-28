# SPARK_HEALTH

Pipeline de datos de salud (CDC FluView):

1. Ingesta semanal desde la API Delphi (Airflow)
2. Persistencia en raw (JSON)
3. Transformación con Spark (Scala)
4. Escritura en zona clean

Tecnologías:
- Apache Airflow
- Apache Spark (Scala)
- Docker
