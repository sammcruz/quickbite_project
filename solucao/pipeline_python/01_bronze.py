# ============================================================================
# BRONZE (Python): cópia fiel dos arquivos que chegam no volume landing.
# Sem limpeza. Cada linha guarda o arquivo de origem e o momento da ingestão.
# Parâmetros do pipeline (Configuration): landing_path, sla_minutes
# ============================================================================
from pyspark import pipelines as dp
from pyspark.sql import functions as F

LANDING = spark.conf.get("landing_path")

ORDERS_SCHEMA = (
    "order_id STRING, customer STRUCT<id: STRING, email: STRING, city: STRING>, restaurant_id STRING, "
    "courier_id STRING, items ARRAY<STRUCT<dish: STRING, qty: INT, price: STRING>>, status STRING, "
    "created_at STRING, delivered_at STRING"
)


def com_origem(df):
    return df.select("*", F.col("_metadata.file_path").alias("_source_file"), F.current_timestamp().alias("_ingest_ts"))


@dp.table(comment="Pedidos crus (JSON Lines) como chegaram no landing")
def bronze_orders():
    return com_origem(spark.readStream.format("json").schema(ORDERS_SCHEMA).load(f"{LANDING}/orders/"))


def csv_stream(pasta, schema):
    return com_origem(spark.readStream.format("csv").option("header", "true").schema(schema).load(f"{LANDING}/{pasta}/"))


@dp.table(comment="Avaliações cruas (CSV), tudo como texto")
def bronze_ratings():
    return csv_stream("ratings", "order_id STRING, stars STRING, comment STRING")


@dp.table(comment="Cadastro de restaurantes cru (CSV)")
def bronze_restaurants():
    return csv_stream("restaurants", "restaurant_id STRING, name STRING, city STRING, cuisine STRING")


@dp.table(comment="Cadastro de entregadores cru (CSV)")
def bronze_couriers():
    return csv_stream("couriers", "courier_id STRING, name STRING, vehicle STRING")
