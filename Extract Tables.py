# Databricks notebook source
from pyspark.sql.utils import AnalysisException
import os

os.environ['DB_HOST'] = 'psql-mock-database-cloud.postgres.database.azure.com'
os.environ['DB_DATABASE'] = 'ecom1692155331663giqokzaqmuqlogbu'
os.environ['DB_PORT'] = '5432'
os.environ['DB_USERNAME'] = 'eolowynayhvayxbhluzaqxfp@psql-mock-database-cloud'
os.environ['DB_PASSWORD'] = 'hdzvzutlssuozdonhflhwyjm'


# COMMAND ----------
    
host = os.getenv('DB_HOST')
database = os.getenv('DB_DATABASE')
port = os.getenv('DB_PORT')
username = os.getenv('DB_USERNAME')
password = os.getenv('DB_PASSWORD')
url = f"jdbc:postgresql://{host}:{port}/{database}"

properties = {
    "user": username,
    "password": password,
    "driver": "org.postgresql.Driver"
}


# COMMAND ----------


tables_query = "(\
    SELECT \
        table_name \
    FROM \
        information_schema.tables \
    WHERE table_schema = 'public' \
        and table_name not like 'pg_%' \
) AS table_list"
tables_df = spark.read.jdbc(url=url, table=tables_query, properties=properties)
table_names = [row['table_name'] for row in tables_df.collect()]

print(table_names)


# COMMAND ----------

for table in table_names:
    try:
        table_df = spark.read.jdbc(url=url, table=table, properties=properties)
        table_df = table_df.repartition(1)
        delta_path = f"/mnt/join/delta/{table}"  
        table_df.write.format("delta").mode("overwrite").save(delta_path)
        spark.sql(f"CREATE TABLE IF NOT EXISTS {table} USING DELTA LOCATION '{delta_path}'")
        print(f"Tabela {table} exportada para Delta Lake em {delta_path}")
        print(f"Tabela {table} registrada no catálogo com sucesso.")
    except AnalysisException as e:
        print(f"Erro ao exportar a tabela {table}: {e}")
    except Exception as e:
        print(f"Erro geral ao exportar a tabela {table}: {e}")
