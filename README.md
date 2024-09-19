# Caso Prático Database Type Ecommerce

## Visão Geral

Este caso prático fornece uma solução baseada em PySpark para analisar diversas métricas de negócios usando dados de um banco de dados PostgreSQL. A análise foi realizada no Databricks, utilizando o Delta Lake para armazenamento e manipulação dos dados.

OS dados extraídos do PostgreSQL estão no notebook `extract tables` e as respostas para os questionamentos práticos no notebook `respostas-questoes04-e-05`

### Objetivos
1. Realizar a ingestão das tabelas do Database Type Ecommerce no formato .parquet (1 arquivo por tabela) usando PySpark
2. Identificar o país com a maior quantidade de itens cancelados.
3. Calcular o faturamento da linha de produto mais vendida, considerando pedidos com status 'Shipped' e realizados no ano de 2005.
4. Recuperar o nome, sobrenome e e-mail mascarado dos representantes de vendas do Japão.

### Fontes de Dados

A análise utiliza dados das seguintes tabelas:

- `customers`
- `orders`
- `orderdetails`
- `products`
- `product_lines`
- `employees`
- `offices`

## Solução

A solução envolve um arquivo de extração que das tabelas do servidor PostgreSQL e envolve três consultas principais implementadas no arquivo de respostas utilizando PySpark para lidar eficientemente com grandes volumes de dados. Cada resultado é salvo em formato Delta, proporcionando gerenciamento robusto dos dados.

###Extração das tabelas em formato parquet.

**Código PySpark:**
```python

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

for table in table_names:
    try:
        table_df = spark.read.jdbc(url=url, table=table, properties=properties)
        table_df = table_df.repartition(1)
        delta_path = f"/mnt/case/delta/{table}"  
        table_df.write.format("delta").mode("overwrite").save(delta_path)
        spark.sql(f"CREATE TABLE IF NOT EXISTS {table} USING DELTA LOCATION '{delta_path}'")
        print(f"Tabela {table} exportada para Delta Lake em {delta_path}")
        print(f"Tabela {table} registrada no catálogo com sucesso.")
    except AnalysisException as e:
        print(f"Erro ao exportar a tabela {table}: {e}")
    except Exception as e:
        print(f"Erro geral ao exportar a tabela {table}: {e}")
```

### a. País com a Maior Quantidade de Itens Cancelados

**Consulta:**

Para identificar o país com o maior número de itens cancelados, a solução realiza um join entre as tabelas `orders`, `orderdetails` e `customers`, filtrando os pedidos com status "Cancelled". Os resultados são agrupados por país e ordenados para encontrar aquele com a maior contagem.

**Código PySpark:**
```python
df_itens_cancelados = orders_df.filter(orders_df.status == "Cancelled") \
    .join(orderdetails_df, "order_number") \
    .join(customers_df, "customer_number") \
    .groupBy("country") \
    .count() \
    .withColumnRenamed("count", "qnt_itens_cancelados") \
    .orderBy(f.col("qnt_itens_cancelados").desc()) \
    .limit(1)

df_itens_cancelados.show()

df_itens_cancelados.write.format("delta").mode("overwrite").save("/mnt/case/delta/itens_cancelados")
```

### b. Qual o faturamento da linha de produto mais vendido, considere os pedidos com status 'Shipped', cujo pedido foi realizado no ano de 2005?

**Consulta:**

Esta consulta calcula o faturamento das linhas de produto ao juntar as tabelas `orders`, `orderdetails`, `product`s e `product_lines`. Filtra os pedidos enviados em 2005 e calcula o faturamento total para cada linha de produto.

**Código PySpark:**
```python
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

df_faturamento_lp_shipped.write.format("delta").mode("overwrite").save("/mnt/case/delta/faturamento_lp_shipped")
```


### c. Traga na consulta o Nome, sobrenome e e-mail dos vendedores do Japão, lembrando que o local-part do e-mail deve estar mascarado.

**Consulta:**

Para obter os vendedores do Japão e mascarar a parte local dos e-mails (antes do @), a consulta faz join das tabelas `employees` e `offices`, selecionando apenas vendedores que trabalham no Japão.

**Código PySpark:**
```python
df_vendedores_japao = employees_df \
    .join(offices_df, "office_code") \
    .filter(offices_df.country == "Japan") \
    .select(
        "first_name", 
        "last_name", 
        f.concat(f.lit("*****"), f.substring_index(f.col("email"), "@", -1)).alias("masked_email")
    )

df_vendedores_japao.show()

df_vendedores_japao.write.format("delta").mode("overwrite").save("/mnt/case/delta/vendedores_japao")
```
