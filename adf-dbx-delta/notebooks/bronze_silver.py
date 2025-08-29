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

# ---- RAW cleanup + read (no glob) ----
# Clean up any junk names once, then read only clean CSVs
bad = [f.path for f in dbutils.fs.ls(RAW_DIR) if (':bad' in f.name) or ('@{' in f.name)]
for p in bad: dbutils.fs.rm(p, True)

all_files = dbutils.fs.ls(RAW_DIR)
good_paths = [f.path for f in all_files
              if f.name.lower().startswith("taxis_") and f.name.lower().endswith(".csv")]

if not good_paths:
    raise Exception(f"No CSVs in {RAW_DIR}. Run ADF pipeline pl_ingest_transform to land one.")

raw_files = [p.split('/')[-1] for p in good_paths]
raw_ok = True

raw_rows, raw_err = None, None
try:
    raw_df = spark.read.option("header", True).csv(good_paths)
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
            # Fallback to clean read if not already loaded
            all_files = dbutils.fs.ls(RAW_DIR)
            good_paths = [f.path for f in all_files
                          if f.name.lower().startswith("taxis_") and f.name.lower().endswith(".csv")]
            if not good_paths:
                raise Exception(f"No CSVs in {RAW_DIR}. Run ADF pipeline pl_ingest_transform to land one.")
            raw_df = spark.read.option("header", True).csv(good_paths)
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
        all_files = dbutils.fs.ls(RAW_DIR)
        good_paths = [f.path for f in all_files
                      if f.name.lower().startswith("taxis_") and f.name.lower().endswith(".csv")]
        if not good_paths:
            raise Exception(f"No CSVs in {RAW_DIR}. Run ADF pipeline pl_ingest_transform to land one.")
        raw_df = spark.read.option("header", True).csv(good_paths)
        raw_rows = raw_df.count()
    # Silver uses normalized DQ cell below
    silver_rows = None
    silver_ok = True
except Exception as e:
    silver_err = str(e)[:300]
    silver_ok = False

mark = lambda ok: "✅" if ok else "❌"
print(f"{mark(raw_ok)} RAW exists & readable | files={len(raw_files)} rows={raw_rows if raw_rows is not None else 'n/a'}")
print(f"{mark(bronze_ok)} BRONZE Delta written | rows={bronze_rows if bronze_rows is not None else 'n/a'} @ {bronze_path}")
print(f"{mark(silver_ok)} SILVER Delta prepared | rows={silver_rows if silver_rows is not None else 'n/a'} @ {silver_path}")

overall = raw_ok and bronze_ok and silver_ok
print("\nRESULT:", "✅ PASS" if overall else "❌ CHECK ABOVE")
if not overall:
    if raw_err:    print("RAW error:", raw_err)
    if bronze_err: print("BRONZE error:", bronze_err)
    if silver_err: print("SILVER error:", silver_err)

# COMMAND ----------

# ---- Data Quality (normalized, strict/warn toggle) ----
from pyspark.sql import functions as F

# 0) Ensure raw_df is available
try:
    raw_df
except NameError:
    all_files = dbutils.fs.ls(RAW_DIR)
    good_paths = [f.path for f in all_files
                  if f.name.lower().startswith("taxis_") and f.name.lower().endswith(".csv")]
    if not good_paths:
        raise Exception(f"No CSVs in {RAW_DIR}. Run ADF pipeline pl_ingest_transform to land one.")
    raw_df = spark.read.option("header", True).csv(good_paths)

# 1) Normalize & type-cast first
dq_df = (
    raw_df
    .withColumn("distance_d", F.col("distance").cast("double"))
    .withColumn("fare_d",     F.col("fare").cast("double"))
    .withColumn("payment_raw", F.lower(F.trim(F.col("payment"))))
    .withColumn(
        "payment_norm",
        F.when(F.col("payment_raw").like("%credit%"), "credit")
         .when(F.col("payment_raw") == "cash", "cash")
         .otherwise("other")
    )
)

# 2) Checks on normalized columns
checks = {
    "trip_distance_positive": (F.col("distance_d") > 0),
    "fare_nonnegative":       (F.col("fare_d") >= 0),
    "payment_type_allowed":   (F.col("payment_norm").isin("cash","credit","other"))
}

violations = []
for name, expr in checks.items():
    cnt = dq_df.filter(~expr | expr.isNull()).count()
    if cnt > 0:
        violations.append((name, cnt))

# 3) strict fail vs warn
STRICT_DQ = False  # set True to make ADF fail & fire alert
if violations:
    msg = " | ".join([f"{n}:{c}" for n,c in violations])
    if STRICT_DQ:
        raise Exception(f"Data quality failed → {msg}")
    else:
        print(f"Data quality WARN → {msg} (continuing) ✅")
else:
    print("Data quality ✅ — all checks passed")

# 4) Use only valid rows for Silver
valid_df = dq_df.filter(
    (F.col("distance_d") > 0) &
    (F.col("fare_d") >= 0)
)

# Write Silver using normalized columns (idempotent overwrite)
silver_df = (
    valid_df.select(
        F.col("pickup").alias("pickup_ts"),
        F.col("dropoff").alias("dropoff_ts"),
        F.col("distance_d").alias("trip_distance"),
        F.col("fare_d").alias("fare_amount"),
        F.col("tip").cast("double").alias("tip_amount"),
        F.col("tolls").cast("double").alias("tolls_amount"),
        F.initcap(F.col("payment_norm")).alias("payment_type")
    )
)
silver_path = DELTA_BASE + "silver/taxis"
silver_df.write.mode("overwrite").format("delta").save(silver_path)
print("silver rows:", spark.read.format("delta").load(silver_path).count())

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

# ci: trigger deploy on push

