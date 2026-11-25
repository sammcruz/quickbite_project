# ============================================================================
# GOLD (Python): modelo dimensional + KPIs, tudo como materialized views.
# PKs informativas declaradas no schema. As FKs estão na versão SQL (pipeline/sql/03_gold.sql).
# ============================================================================
from pyspark import pipelines as dp
from pyspark.sql import functions as F

SLA_MIN = int(spark.conf.get("sla_minutes"))


@dp.materialized_view(
    comment="Calendário do período dos pedidos",
    schema="date_key DATE NOT NULL, year INT, month INT, day INT, dow_num INT, day_name STRING, week INT, is_weekend BOOLEAN, "
           "CONSTRAINT pk_dim_date PRIMARY KEY (date_key)")
def dim_date():
    o = spark.read.table("silver_orders").agg(F.min("order_date").alias("d0"), F.max("order_date").alias("d1"))
    d = o.select(F.explode(F.sequence("d0", "d1")).alias("date_key"))
    return d.select("date_key", F.year("date_key").alias("year"), F.month("date_key").alias("month"), F.dayofmonth("date_key").alias("day"),
                    F.dayofweek("date_key").alias("dow_num"), F.date_format("date_key", "EEEE").alias("day_name"),
                    F.weekofyear("date_key").alias("week"), F.dayofweek("date_key").isin(1, 7).alias("is_weekend"))


@dp.materialized_view(
    comment="Restaurantes",
    schema="restaurant_id STRING NOT NULL, restaurant_name STRING, city STRING, cuisine STRING, CONSTRAINT pk_dim_restaurant PRIMARY KEY (restaurant_id)")
def dim_restaurant():
    return spark.read.table("silver_restaurants")


@dp.materialized_view(
    comment="Entregadores e tipo de veículo",
    schema="courier_id STRING NOT NULL, courier_name STRING, vehicle STRING, CONSTRAINT pk_dim_courier PRIMARY KEY (courier_id)")
def dim_courier():
    return spark.read.table("silver_couriers")


@dp.materialized_view(
    comment="Clientes reais (sem contas de teste)",
    schema="customer_id STRING NOT NULL, customer_email STRING, first_order_at TIMESTAMP, last_order_at TIMESTAMP, n_orders BIGINT, "
           "CONSTRAINT pk_dim_customer PRIMARY KEY (customer_id)")
def dim_customer():
    return spark.read.table("silver_customers")


@dp.materialized_view(
    comment="Pratos, culinária e preço de tabela",
    schema="dish STRING NOT NULL, cuisine STRING, list_price DECIMAL(10,2), CONSTRAINT pk_dim_dish PRIMARY KEY (dish)")
def dim_dish():
    i = spark.read.table("silver_order_items")
    o = spark.read.table("silver_orders").select("order_id", "restaurant_id")
    r = spark.read.table("silver_restaurants").select("restaurant_id", "cuisine")
    return (i.join(o, "order_id").join(r, "restaurant_id")
             .groupBy("dish").agg(F.max("cuisine").alias("cuisine"), F.max("unit_price").alias("list_price")))


@dp.materialized_view(
    comment="Um registro por pedido. Receita = pedidos entregues, receita perdida = cancelados, within_sla é nulo para não entregues e stars é nulo sem avaliação",
    schema="order_id STRING NOT NULL, order_date DATE, order_hour INT, meal_period STRING, customer_id STRING, restaurant_id STRING, courier_id STRING, "
           "delivery_city STRING, status STRING, created_at TIMESTAMP, delivered_at TIMESTAMP, delivery_minutes DOUBLE, items_count INT, total_qty INT, "
           "order_total DECIMAL(12,2), is_delivered BOOLEAN, is_cancelled BOOLEAN, within_sla BOOLEAN, revenue DECIMAL(12,2), lost_revenue DECIMAL(12,2), "
           "stars INT, comment STRING, CONSTRAINT pk_fact_orders PRIMARY KEY (order_id)")
def fact_orders():
    o = spark.read.table("silver_orders")
    r = spark.read.table("silver_ratings").select("order_id", "stars", "comment")
    j = o.join(r, (o.order_id == r.order_id) & (o.status == "delivered"), "left").drop(r.order_id)
    return j.select(
        "order_id", "order_date", "order_hour", "meal_period", "customer_id", "restaurant_id", "courier_id", "delivery_city",
        "status", "created_at", "delivered_at", "delivery_minutes", "items_count", "total_qty", "order_total",
        (F.col("status") == "delivered").alias("is_delivered"),
        (F.col("status") == "cancelled").alias("is_cancelled"),
        F.when(F.col("status") == "delivered", F.col("delivery_minutes") <= SLA_MIN).alias("within_sla"),
        F.when(F.col("status") == "delivered", F.col("order_total")).otherwise(F.lit(0)).cast("decimal(12,2)").alias("revenue"),
        F.when(F.col("status") == "cancelled", F.col("order_total")).otherwise(F.lit(0)).cast("decimal(12,2)").alias("lost_revenue"),
        "stars", "comment")


@dp.materialized_view(comment="Um registro por item de pedido")
def fact_order_items():
    i = spark.read.table("silver_order_items")
    o = spark.read.table("silver_orders").select("order_id", "order_date", "restaurant_id", "status")
    return i.join(o, "order_id")


@dp.materialized_view(comment="KPIs por dia e cidade. Guarda somas e contagens para recalcular médias de qualquer recorte (não some médias)")
def gold_daily_kpis():
    return (spark.read.table("fact_orders").groupBy("order_date", "delivery_city").agg(
        F.count("*").alias("n_orders"),
        F.sum(F.when(F.col("is_delivered"), 1).otherwise(0)).alias("n_delivered"),
        F.sum(F.when(F.col("is_cancelled"), 1).otherwise(0)).alias("n_cancelled"),
        F.sum("revenue").alias("revenue"), F.sum("lost_revenue").alias("lost_revenue"),
        F.sum(F.when(F.col("is_delivered"), F.col("delivery_minutes"))).alias("delivery_minutes_sum"),
        F.sum(F.when(F.col("within_sla"), 1).otherwise(0)).alias("n_within_sla"),
        F.sum("stars").alias("stars_sum"), F.count("stars").alias("n_ratings")))


@dp.materialized_view(comment="Ranking de restaurantes")
def gold_restaurant_summary():
    f, r = spark.read.table("fact_orders"), spark.read.table("dim_restaurant")
    return (f.join(r, "restaurant_id").groupBy("restaurant_id", "restaurant_name", "city", "cuisine").agg(
        F.count("*").alias("n_orders"), F.sum("revenue").alias("revenue"),
        F.round(100 * F.avg(F.when(F.col("is_cancelled"), 1.0).otherwise(0.0)), 1).alias("cancel_rate_pct"),
        F.round(F.avg("delivery_minutes"), 1).alias("avg_delivery_minutes"),
        F.round(F.avg("stars"), 2).alias("avg_stars"), F.count("stars").alias("n_ratings")))


@dp.materialized_view(comment="Desempenho dos entregadores (só entregas realizadas)")
def gold_courier_summary():
    f, c = spark.read.table("fact_orders").filter("is_delivered"), spark.read.table("dim_courier")
    return (f.join(c, "courier_id").groupBy("courier_id", "courier_name", "vehicle").agg(
        F.count("*").alias("n_deliveries"), F.round(F.avg("delivery_minutes"), 1).alias("avg_delivery_minutes"),
        F.round(100 * F.avg(F.when(F.col("within_sla"), 1.0).otherwise(0.0)), 1).alias("within_sla_pct"),
        F.round(F.avg("stars"), 2).alias("avg_stars")))


@dp.materialized_view(comment="Pratos mais vendidos (só pedidos entregues)")
def gold_dish_ranking():
    i, d = spark.read.table("fact_order_items").filter("status = 'delivered'"), spark.read.table("dim_dish")
    return (i.join(d, "dish").groupBy("dish", "cuisine").agg(
        F.sum("quantity").alias("units"), F.sum("line_total").alias("revenue"), F.countDistinct("order_id").alias("n_orders")))


@dp.materialized_view(comment="Demanda por dia da semana e hora")
def gold_hourly_demand():
    f, d = spark.read.table("fact_orders"), spark.read.table("dim_date")
    return (f.join(d, f.order_date == d.date_key).groupBy("dow_num", "day_name", "order_hour").agg(
        F.count("*").alias("n_orders"), F.sum("revenue").alias("revenue")))


@dp.materialized_view(comment="Comentários das avaliações: quantidade e nota média")
def gold_rating_reasons():
    return (spark.read.table("fact_orders").filter("stars IS NOT NULL")
            .groupBy(F.coalesce("comment", F.lit("(no comment)")).alias("comment"))
            .agg(F.count("*").alias("n_ratings"), F.round(F.avg("stars"), 2).alias("avg_stars")))
