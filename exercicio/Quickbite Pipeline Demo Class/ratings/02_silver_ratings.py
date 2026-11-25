# ============================================================================
# RATINGS (Python): bronze_ratings + silver_ratings
# Requer em Settings > Configuration do pipeline: landing_path
# ============================================================================
from pyspark import pipelines as dp
from pyspark.sql import functions as F

@dp.table(comment="Avaliações com nota válida (1 a 5), uma por pedido")
@dp.expect_or_drop("order_id_not_null", "order_id IS NOT NULL")
@dp.expect_or_drop("stars_in_range", "stars BETWEEN 1 AND 5")
def silver_ratings():
    return (
        spark.readStream.table("bronze_ratings")
        .select(
            F.trim("order_id").alias("order_id"),
            F.expr("try_cast(stars AS INT)").alias("stars"),
            F.expr("nullif(trim(comment), '')").alias("comment"),
            "_source_file",
            "_ingest_ts",
        )
        .dropDuplicates(["order_id"])
    )