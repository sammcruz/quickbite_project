# Databricks notebook source
# MAGIC %md
# MAGIC ### 3.2 Silver
# MAGIC Princípios: **uma linha por entidade**, **tipos corretos**, **valores padronizados**, e **nada some em silêncio**.
# MAGIC
# MAGIC | Tabela | Grão | Conteúdo | Regras de negócio |
# MAGIC |---|---|---|---|
# MAGIC | `silver_orders` | pedido | id, cliente, e-mail, **cidade de entrega padronizada**, restaurante, entregador, status, `created_at`/`delivered_at` como timestamp, data e hora, **período da refeição**, **minutos de entrega**, nº de itens, quantidade total, **valor total em euros** | Cada pedido uma única vez, mesmo reenviado em outro arquivo. Contas de teste não entram. Minutos de entrega só para entregues. Valor calculado a partir dos itens |
# MAGIC | `silver_order_items` | item do pedido | pedido, posição, prato, quantidade, **preço unitário numérico**, total da linha | Preço sempre em euros, 2 casas |
# MAGIC | `silver_customers` | cliente | id, e-mail, 1º e último pedido, nº de pedidos | Derivada dos pedidos válidos |
# MAGIC | `silver_restaurants`, `silver_couriers` | cadastro | atributos padronizados | Se o cadastro chegar de novo, vale a última versão |
# MAGIC | `silver_ratings` | avaliação | pedido, nota inteira, comentário | Nota de 1 a 5. Uma avaliação por pedido |
# MAGIC | `quarantine_orders` | pedido rejeitado | colunas úteis + **motivo** | |
# MAGIC | `quarantine_ratings` | avaliação rejeitada | colunas úteis + **motivo específico** | Inclui avaliação de pedido ainda desconhecido, de conta de teste, de pedido cancelado |
# MAGIC | `silver_quality_report` | regra de qualidade | etapa, regra, ação (corrigiu, descartou, quarentena, informativo), nº de linhas | Tem de responder "quanto lixo havia?" com uma consulta |

# COMMAND ----------

from pyspark import pipelines as dp
from pyspark.sql import functions as F
from pyspark.sql.types import DecimalType, IntegerType
from pyspark.sql.window import Window

# COMMAND ----------

# DBTITLE 1,Lakeflow: declare materialized view for silver_order_items
def parse_price(col):
    return F.trim(F.regexp_replace(F.regexp_replace(col, "€", ""), ",", ".")).cast(DecimalType(10, 2))

@dp.materialized_view(name="silver_order_items")
def silver_order_items():
    orders_valid = (
        spark.read.table("bronze_orders")
        .filter(~F.col("customer.email").like("%+test%"))
        .filter(F.col("order_id").isNotNull())
    )
    items = orders_valid.select("order_id", F.posexplode("items").alias("item_no", "item"))
    return items.select(
        "order_id",
        "item_no",
        F.col("item.dish").alias("dish"),
        F.col("item.qty").cast(IntegerType()).alias("quantity"),
        parse_price(F.col("item.price")).alias("unit_price"),
        F.round(F.col("item.qty").cast(DecimalType(10, 2)) * parse_price(F.col("item.price")), 2).alias("line_total"),
    )

# COMMAND ----------

# DBTITLE 1,PySpark silver_orders
def parse_price(col):
    return F.trim(F.regexp_replace(F.regexp_replace(col, "€", ""), ",", ".")).cast(DecimalType(10, 2))

@dp.materialized_view(name="silver_orders")
def silver_orders():
    bronze = spark.read.table("bronze_orders")

    orders_valid = (
        bronze
        .filter(~F.col("customer.email").like("%+test%"))
        .filter(F.col("order_id").isNotNull())
    )

    w = Window.partitionBy("order_id").orderBy(F.col("created_at").desc())
    orders_latest = (
        orders_valid
        .withColumn("rn", F.row_number().over(w))
        .filter("rn = 1")
        .drop("rn")
    )

    order_hour = F.hour(F.to_timestamp("created_at"))
    meal_period = (
        F.when(order_hour.between(6, 11), "cafe_manha")
         .when(order_hour.between(12, 17), "almoco")
         .when(order_hour.between(18, 23), "jantar")
         .otherwise("madrugada")
    )
    delivery_minutes = F.when(
        (F.col("status") == "delivered") & F.col("delivered_at").isNotNull(),
        F.round((F.unix_timestamp(F.to_timestamp("delivered_at"))
                 - F.unix_timestamp(F.to_timestamp("created_at"))) / 60.0, 1)
    )

    item_df = orders_latest.select("order_id", F.explode("items").alias("item"))
    order_totals = item_df.groupBy("order_id").agg(
        F.round(F.sum(F.col("item.qty").cast(DecimalType(10, 2)) * parse_price(F.col("item.price"))), 2).alias("order_total"),
        F.sum("item.qty").alias("total_qty"),
        F.count("item.dish").alias("items_count"),
    )

    return (
        orders_latest.join(order_totals, "order_id", "left")
        .select(
            "order_id",
            F.col("customer.id").alias("customer_id"),
            F.col("customer.email").alias("customer_email"),
            F.trim(F.lower(F.col("customer.city"))).alias("delivery_city"),
            "restaurant_id", "courier_id", "status",
            F.to_timestamp("created_at").alias("created_at"),
            F.to_timestamp("delivered_at").alias("delivered_at"),
            F.to_date("created_at").alias("order_date"),
            order_hour.alias("order_hour"),
            meal_period.alias("meal_period"),
            delivery_minutes.alias("delivery_minutes"),
            "items_count", "total_qty", "order_total",
        )
    )