# Databricks notebook source
# --- ADLS OAuth via Databricks secrets (scope: adls-oauth) ---
ACCOUNT = "sttaxistorage"  # your storage

spark.conf.set(f"fs.azure.account.auth.type.{ACCOUNT}.dfs.core.windows.net", "OAuth")
spark.conf.set(f"fs.azure.account.oauth.provider.type.{ACCOUNT}.dfs.core.windows.net",
               "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")

spark.conf.set(f"fs.azure.account.oauth2.client.id.{ACCOUNT}.dfs.core.windows.net",
               dbutils.secrets.get("adls-oauth", "app-id"))
spark.conf.set(f"fs.azure.account.oauth2.client.secret.{ACCOUNT}.dfs.core.windows.net",
               dbutils.secrets.get("adls-oauth", "app-secret"))
spark.conf.set(f"fs.azure.account.oauth2.client.endpoint.{ACCOUNT}.dfs.core.windows.net",
               f"https://login.microsoftonline.com/{dbutils.secrets.get('adls-oauth','tenant-id')}/oauth2/token")

STORAGE   = ACCOUNT
CONTAINER = "taxi"
RAW_DIR   = f"abfss://{CONTAINER}@{STORAGE}.dfs.core.windows.net/raw/"
DELTA_BASE= f"abfss://{CONTAINER}@{STORAGE}.dfs.core.windows.net/delta/"

# COMMAND ----------

# ✅ Validation cell — run AFTER your secrets-based Spark config cell
# Checks: RAW file exists/readable, writes BRONZE/SILVER Delta, prints a PASS/FAIL summary.

from pyspark.sql import functions as F

ACCOUNT = "sttaxistorage"
CONTAINER = "taxi"
RAW_DIR    = f"abfss://{CONTAINER}@{ACCOUNT}.dfs.core.windows.net/raw/"
DELTA_BASE = f"abfss://{CONTAINER}@{ACCOUNT}.dfs.core.windows.net/delta/"
bronze_path = DELTA_BASE + "bronze/taxis"
silver_path = DELTA_BASE + "silver/taxis"

def _exists(path: str) -> bool:
    try:
        dbutils.fs.ls(path)
        return True
    except Exception:
        return False

def _list_names(path: str):
    try:
        return [f.name for f in dbutils.fs.ls(path)]
    except Exception:
        return []

# ---- RAW checks ----
raw_files = [n for n in _list_names(RAW_DIR) if n.startswith("taxis_") and n.endswith(".csv")]
raw_ok = len(raw_files) > 0

raw_rows = None
raw_err = None
if raw_ok:
    try:
        raw_df = spark.read.option("header", True).csv(RAW_DIR + "taxis_*.csv")
        raw_rows = raw_df.count()
        raw_ok = raw_rows > 0
    except Exception as e:
        raw_err = str(e)[:300]
        raw_ok = False

# ---- BRONZE ----
bronze_rows, bronze_ok, bronze_err = None, False, None
try:
    if not _exists(bronze_path):
        if raw_rows is None:
            raw_df = spark.read.option("header", True).csv(RAW_DIR + "taxis_*.csv")
            raw_rows = raw_df.count()
        (raw_df.write.mode("overwrite").format("delta").save(bronze_path))
    bronze_rows = spark.read.format("delta").load(bronze_path).count()
    bronze_ok = bronze_rows > 0
except Exception as e:
    bronze_err = str(e)[:300]
    bronze_ok = False

# ---- SILVER ----
silver_rows, silver_ok, silver_err = None, False, None
try:
    if raw_rows is None:
        raw_df = spark.read.option("header", True).csv(RAW_DIR + "taxis_*.csv")
        raw_rows = raw_df.count()
    silver_df = (
        raw_df.select(
            F.col("pickup").alias("pickup_ts"),
            F.col("dropoff").alias("dropoff_ts"),
            F.col("distance").cast("double").alias("trip_distance"),
            F.col("fare").cast("double").alias("fare_amount"),
            F.col("tip").cast("double").alias("tip_amount"),
            F.col("tolls").cast("double").alias("tolls_amount"),
            F.when(F.col("payment").isin("cash","credit"), F.initcap("payment"))
             .otherwise(F.lit("Other")).alias("payment_type")
        ).na.drop(subset=["trip_distance","fare_amount"])
    )
    (silver_df.write.mode("overwrite").format("delta").save(silver_path))
    silver_rows = spark.read.format("delta").load(silver_path).count()
    silver_ok = silver_rows > 0
except Exception as e:
    silver_err = str(e)[:300]
    silver_ok = False

mark = lambda ok: "✅" if ok else "❌"
print(f"{mark(raw_ok)} RAW exists & readable | files={len(raw_files)} rows={raw_rows if raw_rows is not None else 'n/a'}")
print(f"{mark(bronze_ok)} BRONZE Delta written | rows={bronze_rows if bronze_rows is not None else 'n/a'} @ {bronze_path}")
print(f"{mark(silver_ok)} SILVER Delta written | rows={silver_rows if silver_rows is not None else 'n/a'} @ {silver_path}")

overall = raw_ok and bronze_ok and silver_ok
print("\nRESULT:", "✅ PASS" if overall else "❌ CHECK ABOVE")
if not overall:
    if raw_err:    print("RAW error:", raw_err)
    if bronze_err: print("BRONZE error:", bronze_err)
    if silver_err: print("SILVER error:", silver_err)

# COMMAND ----------

# ---- Data Quality (simple) ----
from pyspark.sql import functions as F

checks = {
    "trip_distance_positive":  (F.col("distance").cast("double") > 0),
    "fare_nonnegative":        (F.col("fare").cast("double") >= 0),
    "payment_type_allowed":    (F.col("payment").isin("cash", "credit"))
}

violations = []
for name, expr in checks.items():
    cnt = raw_df.filter(~expr).count()
    if cnt > 0:
        violations.append((name, cnt))

if violations:
    msg = "DQ failed: " + ", ".join([f"{n}={c}" for n,c in violations])
    raise Exception(msg)

print("Data quality ✅ — all checks passed")

# COMMAND ----------

# Create Hive metastore tables
spark.sql("CREATE DATABASE IF NOT EXISTS default")

spark.sql("""
CREATE TABLE IF NOT EXISTS hive_metastore.default.bronze_taxis
USING DELTA
LOCATION 'abfss://taxi@sttaxistorage.dfs.core.windows.net/delta/bronze/taxis'
""")

spark.sql("""
CREATE TABLE IF NOT EXISTS hive_metastore.default.silver_taxis
USING DELTA
LOCATION 'abfss://taxi@sttaxistorage.dfs.core.windows.net/delta/silver/taxis'
""")

print("Hive metastore tables created successfully")

# Updated: trigger deploy workflow test