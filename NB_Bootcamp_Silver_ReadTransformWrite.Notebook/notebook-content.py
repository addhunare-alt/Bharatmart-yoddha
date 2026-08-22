# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "d701b88c-fddd-40f4-9be7-e054679ecfad",
# META       "default_lakehouse_name": "Final_LH_Silver",
# META       "default_lakehouse_workspace_id": "52527b90-6e37-41eb-b962-eabd19edbe03",
# META       "known_lakehouses": [
# META         {
# META           "id": "d701b88c-fddd-40f4-9be7-e054679ecfad"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

#---------------- READ DATA ----------------

# Connect and Read the Bronze Lakehouse Table (Currently not connected to this Notebook)

# 1) Store lakehouse address as string into df variable - Bronze_table
# "abfss://Workspace@onelake.dfs.fabric.microsoft.com/lakehouse Name.Lakehouse/Tables"
bronze_tables = "abfss://Bharatmart_Bootcamp_Adarsh@onelake.dfs.fabric.microsoft.com/Final_LH_Bronze.Lakehouse/Tables"

# 2) Reads the data from the defined table
bronze_customers = spark.read.format("delta").load(f"{bronze_tables}/dbo/dim_customers")
bronze_products = spark.read.format("delta").load(f"{bronze_tables}/dbo/dim_products")
bronze_stores = spark.read.format("delta").load(f"{bronze_tables}/dbo/dim_stores")
bronze_targets = spark.read.format("delta").load(f"{bronze_tables}/dbo/dim_targets")
bronze_sales = spark.read.format("delta").load(f"{bronze_tables}/dbo/fact_sales")

# 3) Enable V-Order for read-heavy Silver writes
spark.conf.set("spark.sql.parquet.vorder.default", "true")

# 4) Validate / show the data
display(bronze_stores)     # displays data in a Table View (truncated)
bronze_customers.show(5)   # displays only 5 rows (truncated)
bronze_products.show(5, truncate=False)   # displays only 5 rows (non-truncated)
bronze_targets.show(5)
bronze_sales.show(5)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#---------------- TRANSFORM DATA ----------------

# bronze_customers → transformation → silver_customers

from pyspark.sql import functions as F      # imports all spark functions into "F" variable like F.trim, F.col, F.todate, F.rownumber 
from pyspark.sql.window import Window       # imports Windows function like Window.orderBy, Window.partitionBy for ranking purpose
from pyspark.sql.types import IntegerType, DoubleType, DateType

# window 1: keep latest row per customer (dedupe)
w_latest = Window.partitionBy("customer_id").orderBy(F.col("updated_at").desc_nulls_last())

# window 2: number the surviving rows -> surrogate key
w_sk = Window.orderBy("customer_id")

# transformations
silver_customers = (
    bronze_customers
    .fillna({"email": "unknown@bharatmart.com"})                                             # fill null emails
    .withColumn("customer_id", F.trim(F.col("customer_id")))                                 # trim key
    .withColumn("_rn", F.row_number().over(w_latest)).filter(F.col("_rn") == 1).drop("_rn")  # dedupe: keep latest
    .withColumn("customer_id", F.col("customer_id").cast(IntegerType()))                     # cast integer
    .withColumn("birth_date",  F.to_date("birth_date", "dd-MM-yyyy").cast(DateType()))       # cast date
    .withColumn("sk_customer", F.row_number().over(w_sk))                                    # surrogate key
)

# write to silver lakehouse
silver_customers.write.mode("overwrite").format("delta").saveAsTable("silver_customers")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#---------------- TRANSFORM DATA ----------------

# bronze_products → transformation → silver_products

from pyspark.sql import functions as F      # imports all spark functions into "F" variable like F.trim, F.col, F.todate, F.rownumber 
from pyspark.sql.window import Window       # imports Windows function like Window.orderBy, Window.partitionBy for ranking purpose
from pyspark.sql.types import IntegerType, DoubleType, DateType

# window 1: number the surviving rows -> surrogate key
w_sk = Window.orderBy("product_id")

# transformations
silver_products = (
    bronze_products
    .withColumn("product_name", F.trim(F.col("product_name")))                      # trim name
    .fillna({"category": "Unknown"})                                                # null category -> Unknown
    .withColumn("product_id", F.trim(F.col("product_id")))
    .dropDuplicates(["product_id"])                                                 # dedupe (no recency col)
    .withColumn("product_id", F.col("product_id").cast(IntegerType()))              # casts
    .withColumn("price",      F.col("price").cast(DoubleType()))                    # casts
    .withColumn("list_price", F.col("list_price").cast(DoubleType()))               # casts
    .withColumn("sk_product", F.row_number().over(w_sk))                            # surrogate key
)

# write to silver lakehouse
silver_products.write.mode("overwrite").format("delta").saveAsTable("silver_products")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#---------------- TRANSFORM DATA ----------------

# bronze_stores → transformation → silver_stores

from pyspark.sql import functions as F      # imports all spark functions into "F" variable like F.trim, F.col, F.todate, F.rownumber 
from pyspark.sql.window import Window       # imports Windows function like Window.orderBy, Window.partitionBy for ranking purpose
from pyspark.sql.types import IntegerType, DoubleType, DateType

# window 1: number the surviving rows -> surrogate key
w_sk = Window.orderBy("store_id")

# transformations
silver_stores = (
    bronze_stores
    .withColumn("zone", F.upper(F.trim(F.col("zone"))))                                 # normalise first
    .withColumn("zone",
        F.when(F.col("zone").isin("N", "NORTH"), "N")
         .when(F.col("zone").isin("S", "SOUTH"), "S")
         .when(F.col("zone").isin("E", "EAST"),  "E")
         .when(F.col("zone").isin("W", "WEST"),  "W")
         .otherwise("Unknown"))                                                         # standardise N/S/E/W
    .withColumn("store_id", F.trim(F.col("store_id")))
    .dropDuplicates(["store_id"])
    .withColumn("store_id",  F.col("store_id").cast(IntegerType()))                     # casts
    .withColumn("open_date", F.to_date("open_date", "dd-MM-yyyy").cast(DateType()))
    .withColumn("sk_store",  F.row_number().over(w_sk))                                 # surrogate key
)

# write to silver lakehouse
silver_stores.write.mode("overwrite").format("delta").saveAsTable("silver_stores")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#---------------- TRANSFORM DATA ----------------

# bronze_targets → transformation → silver_targets

from pyspark.sql import functions as F      # imports all spark functions into "F" variable like F.trim, F.col, F.todate, F.rownumber 
from pyspark.sql.window import Window       # imports Windows function like Window.orderBy, Window.partitionBy for ranking purpose
from pyspark.sql.types import IntegerType, DoubleType, DateType

# NOTE: dim_targets is really a store-month FACT; SK added in actually not required.

# window 1: number the surviving rows -> surrogate key
w_sk = Window.orderBy("target_id", "year", "month")

# transformations
silver_targets = (
    bronze_targets
    .withColumn("target_id",      F.col("target_id").cast(IntegerType()))
    .withColumn("month",         F.col("month").cast(IntegerType()))
    .withColumn("year",          F.col("year").cast(IntegerType()))
    .withColumn("target_amount", F.col("target_amount").cast(DoubleType()))
    .filter(F.col("month").between(1, 12))                                          # validate month 1-12
    .dropDuplicates(["target_id", "year", "month"])
    .withColumn("sk_target", F.row_number().over(w_sk))
)

# write to silver lakehouse
silver_targets.write.mode("overwrite").format("delta").saveAsTable("silver_targets")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#---------------- TRANSFORM DATA ----------------

# bronze_sales → transformation → silver_sales

from pyspark.sql import functions as F      # imports all spark functions into "F" variable like F.trim, F.col, F.todate, F.rownumber 
from pyspark.sql.window import Window       # imports Windows function like Window.orderBy, Window.partitionBy for ranking purpose
from pyspark.sql.types import IntegerType, DoubleType, DateType

# capture the grain before any join so we can prove it is unchanged afterward.
# 47,320 is the guardrail for the whole chain.
before = bronze_sales.count()

# transformations
silver_sales_clean = (
    bronze_sales
    .withColumn("amount",    F.col("amount").cast(DoubleType()))                        # casts
    .withColumn("quantity",       F.col("quantity").cast(IntegerType()))
    .withColumn("sale_date", F.to_date("sale_date", "dd-MM-yyyy").cast(DateType()))
    .withColumn("sale_id",    F.col("sale_id").cast(IntegerType()))
    .withColumn("customer_id", F.col("customer_id").cast(IntegerType()))
    .withColumn("product_id",  F.col("product_id").cast(IntegerType()))
    .withColumn("store_id",    F.col("store_id").cast(IntegerType()))
    .na.drop(subset=["customer_id", "store_id"])                                        # drop rows missing keys
)

# the final step that turns cleaned data into a proper star schema.
# re-key: swap natural keys for surrogate keys via LEFT joins to the Silver dims
silver_sales = (
    silver_sales_clean
    .join(silver_customers.select("customer_id", "sk_customer"), "customer_id", "left")
    .join(silver_products.select("product_id",  "sk_product"),  "product_id",  "left")
    .join(silver_stores.select("store_id",      "sk_store"),    "store_id",    "left")
    .drop("customer_id", "product_id", "store_id")                                      # keep only sk FKs + measures
)

# write to silver lakehouse
silver_sales.write.mode("overwrite").format("delta").saveAsTable("silver_sales")


# validate grain + referential integrity
# 1) validate grain
print(f"fact rows: {before} -> {silver_sales.count()} (dropped {before - silver_sales.count()} null-key rows)")

# 2) because you used left joins, verify nothing broke referential integrity:
for sk in ["sk_customer", "sk_product", "sk_store"]:
    print(sk, "orphans:", silver_sales.filter(F.col(sk).isNull()).count())   # expect 0


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
