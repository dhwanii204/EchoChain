# Databricks notebook source
# MAGIC %sh
# MAGIC pwd

# COMMAND ----------

df_raw = spark.read.json(
    "/Workspace/Users/dhwanii374@gmail.com/EchoChain/scrapy/echochain_scraper/marketplace_output.json"
)

display(df_raw)

# COMMAND ----------

bronze_path = "/Workspace/Users/dhwanii374@gmail.com/EchoChain/bronze/marketplace_raw"

df_raw.write \
    .format("delta") \
    .mode("overwrite") \
    .save(bronze_path)

# COMMAND ----------

df_bronze = spark.read.format("delta").load(
    "/Workspace/Users/dhwanii374@gmail.com/EchoChain/bronze/marketplace_raw"
)

display(df_bronze)

# COMMAND ----------

bom_df = spark.read.table("workspace.default.bom")

display(bom_df.select("sku").distinct().orderBy("sku"))

# COMMAND ----------

from pyspark.sql.functions import col, trim, regexp_replace, lower, when

df_clean = (
    df_bronze
    .withColumn("title_clean", lower(trim(col("title"))))
    .withColumn("seller_clean", trim(col("seller")))
    .withColumn("condition_clean", trim(col("condition")))
)

display(df_clean.select(
    "title",
    "title_clean",
    "seller",
    "seller_clean",
    "condition",
    "condition_clean"
))

# COMMAND ----------

products_df = spark.read.option("header", True).csv(
    "/Workspace/Users/dhwanii374@gmail.com/EchoChain/data/mock/products.csv"
)

display(products_df)

# COMMAND ----------

from pyspark.sql.functions import regexp_replace, lower, col, when

# Normalize marketplace title and product model
df_titles = df_clean.withColumn(
    "title_normalized",
    regexp_replace(lower(col("title_clean")), "[^a-z0-9]", "")
)

df_products = products_df.withColumn(
    "model_normalized",
    regexp_replace(lower(col("product_model")), "[^a-z0-9]", "")
)

# Extract SKU using product model matching
df_sku = (
    df_titles
    .crossJoin(df_products)
    .filter(
        col("title_normalized").contains(col("model_normalized"))
    )
    .select(
        df_titles["*"],
        df_products["sku"],
        df_products["product_model"]
    )
)

# Handle "EchoBook 14 Pro" → EchoBook Pro 14 / LAP001
df_sku = (
    df_sku
    .unionByName(
        df_titles
        .filter(col("title_normalized").contains("echobook14pro"))
        .withColumn("sku", when(col("title_normalized").contains("echobook14pro"), "LAP001"))
        .withColumn("product_model", when(col("title_normalized").contains("echobook14pro"), "EchoBook Pro 14"))
        .select(
            df_titles["*"],
            "sku",
            "product_model"
        )
    )
    .dropDuplicates(["listing_url"])
)

display(
    df_sku.select(
        "title",
        "product_model",
        "sku"
    )
)

# COMMAND ----------

df_silver = df_sku.select(
    "title",
    "title_clean",
    "sku",
    "product_model",
    "condition",
    "condition_clean",
    "price",
    "seller",
    "seller_clean",
    "marketplace",
    "listing_url",
    "listing_date"
)

display(df_silver)

# COMMAND ----------

silver_path = "/Workspace/Users/dhwanii374@gmail.com/EchoChain/silver/marketplace_clean"

df_silver.write \
    .format("delta") \
    .mode("overwrite") \
    .save(silver_path)

# COMMAND ----------

df_silver_check = spark.read.format("delta").load(
    "/Workspace/Users/dhwanii374@gmail.com/EchoChain/silver/marketplace_clean"
)

display(df_silver_check)

# COMMAND ----------

display(
    spark.read.format("delta")
    .load("/Workspace/Users/dhwanii374@gmail.com/EchoChain/silver/marketplace_clean")
)

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE marketplace_clean
# MAGIC USING DELTA
# MAGIC LOCATION '/Workspace/Users/dhwanii374@gmail.com/EchoChain/silver/marketplace_clean';

# COMMAND ----------

df_silver.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("marketplace_clean")

# COMMAND ----------

print([x for x in dir() if x.startswith("df")])

# COMMAND ----------

df_silver = (
    spark.read
    .format("delta")
    .load("/Workspace/Users/dhwanii374@gmail.com/EchoChain/silver/marketplace_clean")
)

display(df_silver)

# COMMAND ----------

df_silver.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("marketplace_clean")

# COMMAND ----------

display(spark.table("marketplace_clean"))