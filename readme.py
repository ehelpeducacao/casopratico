# Databricks notebook source
# MAGIC %md
# MAGIC # Caso Prático JOIN
# MAGIC
# MAGIC ## Visão Geral
# MAGIC
# MAGIC Este caso prático fornece uma solução baseada em PySpark para analisar diversas métricas de negócios usando dados de um banco de dados PostgreSQL. A análise foi realizada no Databricks, utilizando o Delta Lake para armazenamento e manipulação dos dados.
# MAGIC
# MAGIC OS dados extraídos do PostgreSQL estão no notebook `extract` e as respostas para os questionamentos práticos no notebook `respostas-questoes04-e-05`
# MAGIC
# MAGIC ### Objetivos
# MAGIC
# MAGIC 1. Identificar o país com a maior quantidade de itens cancelados.
# MAGIC 2. Calcular o faturamento da linha de produto mais vendida, considerando pedidos com status 'Shipped' e realizados no ano de 2005.
# MAGIC 3. Recuperar o nome, sobrenome e e-mail mascarado dos representantes de vendas do Japão.
# MAGIC
# MAGIC ### Fontes de Dados
# MAGIC
# MAGIC A análise utiliza dados das seguintes tabelas armazenadas no esquema `hive_metastore.default`:
# MAGIC
# MAGIC - `customers`
# MAGIC - `orders`
# MAGIC - `orderdetails`
# MAGIC - `products`
# MAGIC - `product_lines`
# MAGIC - `employees`
# MAGIC - `offices`
# MAGIC
# MAGIC ## Solução
# MAGIC
# MAGIC A solução envolve três consultas principais implementadas utilizando PySpark para lidar eficientemente com grandes volumes de dados. Cada resultado é salvo em formato Delta Lake, proporcionando gerenciamento robusto dos dados.
# MAGIC
# MAGIC ### a. País com a Maior Quantidade de Itens Cancelados
# MAGIC
# MAGIC **Consulta:**
# MAGIC
# MAGIC Para identificar o país com o maior número de itens cancelados, a solução realiza um join entre as tabelas `orders`, `orderdetails` e `customers`, filtrando os pedidos com status "Cancelled". Os resultados são agrupados por país e ordenados para encontrar aquele com a maior contagem.
# MAGIC
# MAGIC **Código PySpark:**
# MAGIC ```python
# MAGIC df_itens_cancelados = orders_df.filter(orders_df.status == "Cancelled") \
# MAGIC     .join(orderdetails_df, "order_number") \
# MAGIC     .join(customers_df, "customer_number") \
# MAGIC     .groupBy("country") \
# MAGIC     .count() \
# MAGIC     .withColumnRenamed("count", "qnt_itens_cancelados") \
# MAGIC     .orderBy(f.col("qnt_itens_cancelados").desc()) \
# MAGIC     .limit(1)
# MAGIC
# MAGIC df_itens_cancelados.show()
# MAGIC
# MAGIC df_itens_cancelados.write.format("delta").mode("overwrite").save("/mnt/join/delta/itens_cancelados")
# MAGIC ```
# MAGIC
# MAGIC ### b. Qual o faturamento da linha de produto mais vendido, considere os pedidos com status 'Shipped', cujo pedido foi realizado no ano de 2005?
# MAGIC
# MAGIC **Consulta:**
# MAGIC
# MAGIC Esta consulta calcula o faturamento das linhas de produto ao juntar as tabelas `orders`, `orderdetails`, `product`s e `product_lines`. Filtra os pedidos enviados em 2005 e calcula o faturamento total para cada linha de produto.
# MAGIC
# MAGIC **Código PySpark:**
# MAGIC ```python
# MAGIC df_order_shipped = orders_df.filter((orders_df.status == "Shipped") & (f.year(orders_df.order_date) == 2005))
# MAGIC
# MAGIC df_faturamento_lp_shipped = df_order_shipped \
# MAGIC     .join(orderdetails_df, "order_number") \
# MAGIC     .join(products_df, "product_code") \
# MAGIC     .join(product_lines_df, "product_line") \
# MAGIC     .groupBy("product_line") \
# MAGIC     .agg(f.sum(f.col("quantity_ordered") * f.col("price_each")).alias("faturamento")) \
# MAGIC     .orderBy(f.col("faturamento").desc()) \
# MAGIC     .limit(1)
# MAGIC
# MAGIC df_faturamento_lp_shipped.show()
# MAGIC
# MAGIC df_faturamento_lp_shipped.write.format("delta").mode("overwrite").save("/mnt/join/delta/faturamento_lp_shipped")
# MAGIC ```
# MAGIC
# MAGIC
# MAGIC ### c. Traga na consulta o Nome, sobrenome e e-mail dos vendedores do Japão, lembrando que o local-part do e-mail deve estar mascarado.
# MAGIC
# MAGIC **Consulta:**
# MAGIC
# MAGIC Para obter os vendedores do Japão e mascarar a parte local dos e-mails (antes do @), a consulta faz join das tabelas `employees` e `offices`, selecionando apenas vendedores que trabalham no Japão.
# MAGIC
# MAGIC **Código PySpark:**
# MAGIC ```python
# MAGIC df_vendedores_japao = employees_df \
# MAGIC     .join(offices_df, "office_code") \
# MAGIC     .filter(offices_df.country == "Japan") \
# MAGIC     .select(
# MAGIC         "first_name", 
# MAGIC         "last_name", 
# MAGIC         f.concat(f.lit("*****"), f.substring_index(f.col("email"), "@", -1)).alias("masked_email")
# MAGIC     )
# MAGIC
# MAGIC df_vendedores_japao.show()
# MAGIC
# MAGIC df_vendedores_japao.write.format("delta").mode("overwrite").save("/mnt/join/delta/vendedores_japao")
# MAGIC ```
# MAGIC
