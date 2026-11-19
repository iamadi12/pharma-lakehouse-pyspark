"""Medallion pipeline (bronze -> silver -> gold) for pharma commercial data.

- Bronze: raw CSVs landed as-is with ingestion metadata
- Silver: typed, de-duplicated, validated, standardised to ZIP3 and month;
          HCP dimension tracked with SCD Type 2
- Gold:   KPIs generated from kpi_config.json - add a KPI without writing code
- Audit:  control totals reconcile TRx across layers
"""
import json

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

RAW, LAKE = "data/raw", "data/lake"


def get_spark() -> tuple[SparkSession, str]:
    builder = SparkSession.builder.appName("pharma-lakehouse")
    try:  # use Delta Lake if delta-spark is installed, otherwise Parquet
        from delta import configure_spark_with_delta_pip
        builder = (builder
                   .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
                   .config("spark.sql.catalog.spark_catalog",
                           "org.apache.spark.sql.delta.catalog.DeltaCatalog"))
        return configure_spark_with_delta_pip(builder).getOrCreate(), "delta"
    except ImportError:
        return builder.getOrCreate(), "parquet"


def save(df: DataFrame, layer: str, name: str, fmt: str) -> None:
    df.write.mode("overwrite").format(fmt).save(f"{LAKE}/{layer}/{name}")


def read_raw(spark: SparkSession, file: str) -> DataFrame:
    return (spark.read.option("header", True).csv(f"{RAW}/{file}")
            .withColumn("_ingested_at", F.current_timestamp())
            .withColumn("_source_file", F.lit(file)))

# ------------------------------------------------------------------ silver

def clean_rx(bronze: DataFrame) -> DataFrame:
    return (bronze
            .withColumn("week_start", F.to_date("week_start"))
            .withColumn("trx", F.col("trx").cast("int"))
            .withColumn("nrx", F.col("nrx").cast("int"))
            .dropDuplicates(["hcp_id", "brand", "week_start"])
            .filter((F.col("trx") >= 0) & (F.col("nrx") >= 0))
            .withColumn("zip3", F.substring("zip", 1, 3))
            .withColumn("month", F.date_format("week_start", "yyyy-MM")))


def clean_calls(bronze: DataFrame) -> DataFrame:
    return (bronze
            .withColumn("week_start", F.to_date("week_start"))
            .withColumn("calls", F.col("calls").cast("int"))
            .withColumn("month", F.date_format("week_start", "yyyy-MM")))


def scd2(current: DataFrame | None, snapshot: DataFrame, key: str, attrs: list[str],
         as_of: str) -> DataFrame:
    """Apply a new snapshot to a Type 2 dimension: changed rows are closed
    (valid_to, is_current=false) and a new current version is inserted."""
    snap = (snapshot.select(key, *attrs)
            .withColumn("attr_hash", F.sha2(F.concat_ws("||", *attrs), 256)))
    new_rows = lambda df: (df.withColumn("valid_from", F.to_date(F.lit(as_of)))
                             .withColumn("valid_to", F.lit(None).cast("date"))
                             .withColumn("is_current", F.lit(True)))
    if current is None:
        return new_rows(snap)

    open_rows = current.filter("is_current")
    closed_rows = current.filter(~F.col("is_current"))
    changed = (open_rows.alias("c").join(snap.alias("s"), key)
               .where(F.col("c.attr_hash") != F.col("s.attr_hash")).select(key))
    expired = (open_rows.join(changed, key, "inner")
               .withColumn("valid_to", F.to_date(F.lit(as_of)))
               .withColumn("is_current", F.lit(False)))
    unchanged = open_rows.join(changed, key, "left_anti")
    brand_new = snap.join(open_rows.select(key), key, "left_anti")
    inserts = new_rows(snap.join(changed, key, "inner").unionByName(brand_new))
    return (closed_rows.unionByName(expired).unionByName(unchanged)
            .unionByName(inserts))

# ------------------------------------------------------------------ gold

def build_kpis(sources: dict[str, DataFrame], dim_hcp: DataFrame, config: dict) -> dict:
    current_hcp = (dim_hcp.filter("is_current")
                   .withColumn("zip3", F.substring("zip", 1, 3)))
    out = {}
    for kpi in config["kpis"]:
        df = sources[kpi["source"]]
        if kpi.get("hcp_columns"):
            df = df.join(current_hcp.select("hcp_id", *kpi["hcp_columns"]), "hcp_id", "left")
        out[kpi["name"]] = (df.groupBy(*kpi["grain"])
                            .agg(F.expr(f"{kpi['agg']}({kpi['measure']})").alias(kpi["measure"])))
    return out


def main() -> None:
    spark, fmt = get_spark()
    with open("kpi_config.json") as f:
        config = json.load(f)

    # Bronze
    bronze_rx, bronze_calls = read_raw(spark, "rx_weekly.csv"), read_raw(spark, "rep_calls.csv")
    save(bronze_rx, "bronze", "rx_weekly", fmt)
    save(bronze_calls, "bronze", "rep_calls", fmt)

    # Silver
    rx, calls = clean_rx(bronze_rx), clean_calls(bronze_calls)
    save(rx, "silver", "rx_weekly", fmt)
    save(calls, "silver", "rep_calls", fmt)

    dim = scd2(None, read_raw(spark, "hcp_master_2026_01.csv"), "hcp_id",
               ["specialty", "zip"], "2026-01-01")
    dim = scd2(dim, read_raw(spark, "hcp_master_2026_04.csv"), "hcp_id",
               ["specialty", "zip"], "2026-04-01")
    save(dim, "silver", "dim_hcp", fmt)

    # Gold
    kpis = build_kpis({"rx": rx, "calls": calls}, dim, config)
    for name, df in kpis.items():
        save(df, "gold", name, fmt)

    # Audit: control totals across layers
    total = lambda df, c: df.agg(F.sum(F.col(c).cast("long"))).first()[0]
    b, s, g = total(bronze_rx, "trx"), total(rx, "trx"), total(kpis["trx_by_zip3_month"], "trx")
    print(f"\nStorage format: {fmt}")
    print(f"TRx control totals  bronze={b}  silver={s}  gold={g}")
    print(f"Rows dropped bronze->silver: {bronze_rx.count() - rx.count()} (duplicates / invalid)")
    print("Gold == silver:", "PASS" if s == g else "FAIL")
    print("\nSCD2 history for HCPs whose specialty changed:")
    changed_ids = dim.filter(~F.col("is_current")).select("hcp_id")
    (dim.join(changed_ids, "hcp_id").orderBy("hcp_id", "valid_from")
        .select("hcp_id", "specialty", "valid_from", "valid_to", "is_current").show())
    spark.stop()


if __name__ == "__main__":
    main()
