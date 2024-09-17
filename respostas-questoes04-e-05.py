# Databricks notebook source
# MAGIC %md
# MAGIC ##Questões 04 e 05

# COMMAND ----------

# DBTITLE 1,imports e tables usadas
import pyspark.sql.functions as f 

# Carregando as tabelas necessárias
customers_df = spark.table("hive_metastore.default.customers")
orders_df = spark.table("hive_metastore.default.orders")
orderdetails_df = spark.table("hive_metastore.default.orderdetails")
products_df = spark.table("hive_metastore.default.products")
product_lines_df = spark.table("hive_metastore.default.product_lines")
employees_df = spark.table("hive_metastore.default.employees")
offices_df = spark.table("hive_metastore.default.offices")

# COMMAND ----------

# DBTITLE 1,Resposta (a): Qual país possui a maior quantidade de itens cancelados?

df_itens_cancelados = orders_df.filter(orders_df.status == "Cancelled") \
    .join(orderdetails_df, "order_number") \
    .join(customers_df, "customer_number") \
    .groupBy("country") \
    .count() \
    .withColumnRenamed("count", "qnt_itens_cancelados") \
    .orderBy(f.col("qnt_itens_cancelados").desc()) \
    .limit(1)

df_itens_cancelados.show()

df_itens_cancelados.write.format("delta").mode("overwrite").save("/mnt/join/delta/itens_cancelados")

# COMMAND ----------

# DBTITLE 1,Qual o faturamento da linha de produto mais vendido, considerando pedidos com status 'Shipped' no ano de 2005?
df_order_shipped = orders_df.filter((orders_df.status == "Shipped") & (f.year(orders_df.order_date) == 2005))

df_faturamento_lp_shipped = df_order_shipped \
    .join(orderdetails_df, "order_number") \
    .join(products_df, "product_code") \
    .join(product_lines_df, "product_line") \
    .groupBy("product_line") \
    .agg(f.sum(f.col("quantity_ordered") * f.col("price_each")).alias("faturamento")) \
    .orderBy(f.col("faturamento").desc()) \
    .limit(1)

df_faturamento_lp_shipped.show()

df_faturamento_lp_shipped.write.format("delta").mode("overwrite").save("/mnt/join/delta/faturamento_lp_shipped")

# COMMAND ----------

# DBTITLE 1,c.	Traga na consulta o Nome, sobrenome e e-mail dos vendedores do Japão, lembrando que o local-part do e-mail deve estar mascarado.
df_vendedores_japao = employees_df \
    .join(offices_df, "office_code") \
    .filter(offices_df.country == "Japan") \
    .select(
        "first_name", 
        "last_name", 
        f.concat(f.lit("*****"), f.substring_index(f.col("email"), "@", -1)).alias("masked_email")
    )

df_vendedores_japao.show()

df_vendedores_japao.write.format("delta").mode("overwrite").save("/mnt/join/delta/vendedores_japao")

