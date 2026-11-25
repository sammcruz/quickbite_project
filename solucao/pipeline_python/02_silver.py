# ============================================================================
# SILVER (Python): limpa, tipada e sem duplicidade.
# Pedidos e avaliações: streaming tables + AUTO CDC para deduplicar.
# Quarentenas, itens, cadastros e relatório: materialized views.
# ============================================================================
from pyspark import pipelines as dp
from pyspark.sql import functions as F
from pyspark.sql.window import Window

PRECO = "CAST(replace(regexp_replace(i.price, '[^0-9,.]', ''), ',', '.') AS DECIMAL(10,2))"


@dp.table(comment="Pedidos tipados e padronizados (ainda pode haver duplicados entre arquivos)")
@dp.expect_or_drop("order_id_not_null", "order_id IS NOT NULL")
@dp.expect_or_drop("not_test_account", "customer_email NOT LIKE '%@quickbite.test'")
@dp.expect_or_drop("valid_status", "status IN ('delivered', 'cancelled')")
@dp.expect("has_items", "items_count > 0")
@dp.expect("positive_total", "order_total > 0")
def silver_orders_clean():
    criado = F.to_timestamp("created_at")
    entregue = F.to_timestamp("delivered_at")
    base = spark.readStream.table("bronze_orders").select(
        F.trim("order_id").alias("order_id"),
        F.col("customer.id").alias("customer_id"),
        F.lower(F.trim("customer.email")).alias("customer_email"),
        F.initcap(F.trim("customer.city")).alias("delivery_city"),
        "restaurant_id", "courier_id", "status",
        criado.alias("created_at"), entregue.alias("delivered_at"),
        criado.cast("date").alias("order_date"),
        F.hour(criado).alias("order_hour"),
        F.when(F.hour(criado).between(11, 15), "lunch").when(F.hour(criado).between(18, 23), "dinner").otherwise("other").alias("meal_period"),
        F.when(F.col("status") == "delivered",
               F.round((F.unix_timestamp(entregue) - F.unix_timestamp(criado)) / 60.0, 1)).alias("delivery_minutes"),
        F.expr(f"transform(items, i -> named_struct('dish', trim(i.dish), 'quantity', CAST(i.qty AS INT), 'unit_price', {PRECO}))").alias("items"),
        "_source_file", "_ingest_ts",
    )
    return (base
        .withColumn("items_count", F.size("items"))
        .withColumn("total_qty", F.expr("aggregate(items, 0, (a, i) -> a + i.quantity)"))
        .withColumn("order_total", F.expr("aggregate(items, CAST(0 AS DECIMAL(12,2)), (a, i) -> CAST(a + i.quantity * i.unit_price AS DECIMAL(12,2)))")))


dp.create_streaming_table("silver_orders", comment="Pedidos limpos, um por order_id, sem contas de teste")
dp.create_auto_cdc_flow(
    target="silver_orders",
    source="silver_orders_clean",
    keys=["order_id"],
    sequence_by=F.struct("_source_file", "_ingest_ts"),
    stored_as_scd_type=1,
)


@dp.table(comment="Avaliações tipadas. Nota fora de 1 a 5 é descartada aqui e explicada em quarantine_ratings")
@dp.expect_or_drop("order_id_not_null", "order_id IS NOT NULL")
@dp.expect_or_drop("stars_in_range", "stars BETWEEN 1 AND 5")
def silver_ratings_clean():
    return spark.readStream.table("bronze_ratings").select(
        F.trim("order_id").alias("order_id"),
        F.expr("try_cast(stars AS INT)").alias("stars"),
        F.expr("nullif(trim(comment), '')").alias("comment"),
        "_source_file", "_ingest_ts",
    )


dp.create_streaming_table("silver_ratings", comment="Avaliações com nota válida, uma por pedido (a ligação com o pedido é feita na gold)")
dp.create_auto_cdc_flow(
    target="silver_ratings",
    source="silver_ratings_clean",
    keys=["order_id"],
    sequence_by=F.struct("_source_file", "_ingest_ts"),
    stored_as_scd_type=1,
)


@dp.materialized_view(comment="Um registro por item de pedido, com preço numérico em euros")
def silver_order_items():
    o = spark.read.table("silver_orders")
    return (o.select("order_id", F.posexplode("items").alias("pos", "item"))
             .select("order_id", (F.col("pos") + 1).alias("line_no"),
                     F.col("item.dish").alias("dish"), F.col("item.quantity").alias("quantity"),
                     F.col("item.unit_price").alias("unit_price"),
                     (F.col("item.quantity") * F.col("item.unit_price")).cast("decimal(12,2)").alias("line_total")))


def ultima_versao(df, chave):
    w = Window.partitionBy(chave).orderBy(F.col("_ingest_ts").desc(), F.col("_source_file").desc())
    return df.withColumn("_rn", F.row_number().over(w)).filter("_rn = 1").drop("_rn")


@dp.materialized_view(comment="Restaurantes, com cidade padronizada")
def silver_restaurants():
    df = spark.read.table("bronze_restaurants").select(
        F.trim("restaurant_id").alias("restaurant_id"), F.trim("name").alias("restaurant_name"),
        F.initcap(F.trim("city")).alias("city"), F.trim("cuisine").alias("cuisine"), "_ingest_ts", "_source_file")
    return ultima_versao(df, "restaurant_id").drop("_ingest_ts", "_source_file")


@dp.materialized_view(comment="Entregadores")
def silver_couriers():
    df = spark.read.table("bronze_couriers").select(
        F.trim("courier_id").alias("courier_id"), F.trim("name").alias("courier_name"),
        F.lower(F.trim("vehicle")).alias("vehicle"), "_ingest_ts", "_source_file")
    return ultima_versao(df, "courier_id").drop("_ingest_ts", "_source_file")


@dp.materialized_view(comment="Clientes derivados dos pedidos válidos")
def silver_customers():
    return (spark.read.table("silver_orders").groupBy("customer_id")
            .agg(F.max("customer_email").alias("customer_email"), F.min("created_at").alias("first_order_at"),
                 F.max("created_at").alias("last_order_at"), F.count("*").alias("n_orders")))


@dp.materialized_view(comment="Pedidos rejeitados e o motivo")
def quarantine_orders():
    df = (spark.read.table("bronze_orders")
          .filter(F.lower(F.trim("customer.email")).like("%@quickbite.test"))
          .select(F.trim("order_id").alias("order_id"), F.col("customer.id").alias("customer_id"),
                  F.col("customer.email").alias("customer_email"), "restaurant_id", "status",
                  F.to_timestamp("created_at").alias("created_at"), F.lit("test_account").alias("reason"),
                  "_source_file", "_ingest_ts"))
    return ultima_versao(df, "order_id").drop("_ingest_ts")


@dp.materialized_view(comment="Avaliações rejeitadas e o motivo. Avaliação de pedido ainda desconhecido sai daqui sozinha quando o pedido chegar")
def quarantine_ratings():
    r = ultima_versao(
        spark.read.table("bronze_ratings").select(
            F.trim("order_id").alias("order_id"), F.expr("try_cast(stars AS INT)").alias("stars"),
            F.expr("nullif(trim(comment), '')").alias("comment"), "_source_file", "_ingest_ts"),
        "order_id")
    s = spark.read.table("silver_orders").select("order_id", F.col("status").alias("o_status"))
    b = spark.read.table("bronze_orders").select(F.trim("order_id").alias("order_id")).distinct().withColumn("in_bronze", F.lit(True))
    j = r.join(s, "order_id", "left").join(b, "order_id", "left")
    reason = (F.when(F.col("stars").isNull() | ~F.col("stars").between(1, 5), "rating_out_of_range")
               .when(F.col("o_status") == "cancelled", "order_not_delivered")
               .when(F.col("o_status").isNotNull(), None)
               .when(F.col("in_bronze"), "order_is_test_account")
               .otherwise("unknown_order"))
    return (j.withColumn("reason", reason).filter(F.col("reason").isNotNull())
             .select("order_id", "stars", "comment", "reason", "_source_file"))


@dp.materialized_view(comment="Quanto lixo havia nos arquivos e o que foi feito com ele")
def silver_quality_report():
    # Só transformações lazy (nada de .count() aqui: o código do pipeline não deve executar ações)
    def linha(step, source, rule, action, agg_df):
        return agg_df.select(F.lit(step).alias("step"), F.lit(source).alias("source"), F.lit(rule).alias("rule"),
                             F.lit(action).alias("action"), F.col("n").cast("long").alias("affected_rows"))
    bo, ro = spark.read.table("bronze_orders"), spark.read.table("bronze_ratings")
    so = spark.read.table("silver_orders")
    qr = spark.read.table("quarantine_ratings")
    linhas = [
        linha(1, "bronze_orders", "rows read", "info", bo.agg(F.count("*").alias("n"))),
        linha(2, "bronze_orders", "duplicate rows (same order_id)", "dropped",
              bo.agg((F.count("*") - F.countDistinct(F.trim("order_id"))).alias("n"))),
        linha(3, "bronze_orders", "test-account orders", "quarantine", spark.read.table("quarantine_orders").agg(F.count("*").alias("n"))),
        linha(4, "bronze_orders", "raw item lines with price outside the 0.00 € format", "fixed",
              bo.select(F.explode("items").alias("i")).filter(~F.col("i.price").rlike(r"^[0-9]+[.][0-9]{2} €$")).agg(F.count("*").alias("n"))),
        linha(5, "silver_orders", "orders loaded", "info", so.agg(F.count("*").alias("n"))),
        linha(6, "silver_orders", "cancelled orders (no courier or delivery time)", "info",
              so.filter("status = 'cancelled'").agg(F.count("*").alias("n"))),
        linha(7, "bronze_ratings", "unique ratings read", "info", ro.agg(F.countDistinct(F.trim("order_id")).alias("n"))),
        (qr.groupBy("reason").agg(F.count("*").alias("n"))
           .select((F.lit(7) + F.row_number().over(Window.orderBy("reason"))).alias("step"),
                   F.lit("bronze_ratings").alias("source"), F.concat(F.lit("rejected: "), "reason").alias("rule"),
                   F.lit("quarantine").alias("action"), F.col("n").cast("long").alias("affected_rows"))),
        linha(20, "silver_ratings", "ratings linked to a delivered order", "info",
              spark.read.table("silver_ratings").join(so.filter("status = 'delivered'"), "order_id").agg(F.count("*").alias("n"))),
    ]
    out = linhas[0]
    for l in linhas[1:]:
        out = out.unionByName(l)
    return out
