# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "a6b19f06-c7b6-4b87-9216-218490d6355d",
# META       "default_lakehouse_name": "BharatmartTrainingBronze",
# META       "default_lakehouse_workspace_id": "52527b90-6e37-41eb-b962-eabd19edbe03",
# META       "known_lakehouses": [
# META         {
# META           "id": "a6b19f06-c7b6-4b87-9216-218490d6355d"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

# Read a registered Delta table (types are clean) 
Customers_br_tbl = spark.read.table("BharatMartTrainingBronze.dbo.dim_customers") 
    


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# …or read raw files (everything comes in as string) 
Customers_br_files = spark.read.option("header","true").csv("Files/Raw data/dim_customers.csv")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

Customers_br_tbl.printSchema()        # always inspect types first 
Customers_br_tbl.show(5, truncate=False)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

Customers_br_tbl.count()
Customers_br_tbl.show(Customers_br_tbl.count())

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

Customers_br_tbl.count()
Customers_br_tbl.show(Customers_br_tbl.count(),truncate=False)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

Customers_br_tbl = Customers_br_tbl.fillna({"email":"unknown@bharatmart.in"})



# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

Customers_br_tbl = Customers_br_tbl.fillna({"email":"unknown@bharatmart.in"})
Customers_br_tbl.show(Customers_br_tbl.count(),truncate=False)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************


# CELL ********************

# dim_customers 
# Fill null emails; 
# Trim_Customer_id
# Remove duplicatededupe on customer_id (keep latest); 
# cast birth_date = DateType 

from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import IntegerType, DateType
 
# 1) Fill null emails
Customers_br_tbl = Customers_br_tbl.fillna({"email": "unknown@bharatmart.com"})
 
# 2) Trim the key so "1002" and "1002 " group together
Customers_br_tbl = Customers_br_tbl.withColumn("customer_id", F.trim(F.col("customer_id")))
 
# 3) Dedupe on customer_id, KEEP LATEST:
#    rank each customer's rows newest-first, keep rank 1
w = Window.partitionBy("customer_id").orderBy(F.col("updated_at").desc_nulls_last())
Customers_br_tbl = (Customers_br_tbl
      .withColumn("_rn", F.row_number().over(w))
      .filter(F.col("_rn") == 1)
      .drop("_rn"))
 
# 4) Cast types (bad dates -> null, not an error)
Customers_br_tbl = (Customers_br_tbl
      .withColumn("customer_id", F.col("customer_id").cast(IntegerType()))
      .withColumn("birth_date",  F.to_date(F.col("birth_date"), "yyyy-MM-dd").cast(DateType())))


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

Customers_br_tbl.count()


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

Customers_br_tbl.count()
Customers_br_tbl.printSchema()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

Customers_br_tbl = spark.read.table("BharatmartTrainingBronze.dbo.dim_customers") # lakehouse.schema.table
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import IntegerType, DateType
 
# rank each customer's rows newest-first using the real recency column: updated_at
w = Window.partitionBy("customer_id").orderBy(F.col("updated_at").desc_nulls_last())
 
Customers_Silver_tbl = (
    Customers_br_tbl
    .fillna({"email": "unknown@bharatmart.com"})                # 1) fill null emails
    .withColumn("customer_id", F.trim(F.col("customer_id")))     # 2) trim key
    .withColumn("_rn", F.row_number().over(w))                   # 3) dedupe: keep latest
    .filter(F.col("_rn") == 1)
    .drop("_rn")
    .withColumn("customer_id", F.col("customer_id").cast(IntegerType()))          # 4) cast
    .withColumn("birth_date",  F.to_date(F.col("birth_date"), "dd-MM-yyyy").cast(DateType()))
)
 
Customers_Silver_tbl.show(Customers_Silver_tbl.count(), truncate=False)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

Customers_br_tbl.orderBy("customer_id").show(truncate=False)



# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

Product_br_tbl = spark.read.table("BharatMartTrainingBronze.dbo.dim_products")
Stores_br_tbl = spark.read.table("BharatMartTrainingBronze.dbo.dim_stores")
Target_br_tbl = spark.read.table("BharatMartTrainingBronze.dbo.dim_targets")
Sales_br_tbl = spark.read.table("BharatMartTrainingBronze.dbo.fact_sales")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import IntegerType, DateType, DoubleType
 
Product_Silver_tbl = (
Product_br_tbl
.withColumn("product_name", F.trim(F.col("product_name")))  # to trim product_name
.fillna({"category": "Unknown"}) # to replace "null" with "UnKnown"
.withColumn("price", F.col("price").cast(DoubleType())) # Change data-type to DoubleType
.withColumn("product_id", F.col("product_id").cast(IntegerType())) # Change data-type to Integer Type
)
 
Product_Silver_tbl.orderBy("product_id").show(Product_Silver_tbl.count(), truncate=False)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

Product_Silver_tbl.printSchema()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql import functions as F
from pyspark.sql.types import DateType, IntegerType
 
Stores_Silver_tbl = (
    Stores_br_tbl                                                  # source = dim_stores, not df
    # 1) normalise first: trim + uppercase so "north", " North " and "N" all match
    .withColumn("zone", F.upper(F.trim(F.col("zone"))))
    # 2) map every variant to a single standard code
    .withColumn("zone",
        F.when(F.col("zone").isin("N", "NORTH"), "N")
         .when(F.col("zone").isin("S", "SOUTH"), "S")
         .when(F.col("zone").isin("E", "EAST"),  "E")
         .when(F.col("zone").isin("W", "WEST"),  "W")
         .otherwise("Unknown"))                                    # anything unexpected is flagged
    # 3) cast open_date -> DateType (bad -> null)
    .withColumn("open_date", F.to_date(F.col("open_date"), "dd-MM-yyyy").cast(DateType()))
    .withColumn("store_id", F.col("store_id").cast(IntegerType())) # Change data-type to Integer Type
)
 
Stores_Silver_tbl.orderBy("store_id").show(truncate=False)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType

Targets_Silver_tbl = (
    Target_br_tbl
    # Fill null target_amount
    .fillna({"target_amount": 0})

    # Trim target_id
    .withColumn("target_id", F.trim(F.col("target_id")))

    # Cast target_id to Integer
    .withColumn("target_id", F.col("target_id").cast(IntegerType()))

    # Remove duplicate records
    .dropDuplicates(["target_id"])
)

Targets_Silver_tbl.show(truncate=False)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType, DoubleType

Fact_Sales_Silver_tbl = (
    Sales_br_tbl

    # Trim ID columns
    .withColumn("sale_id", F.trim(F.col("sale_id")))
    .withColumn("customer_id", F.trim(F.col("customer_id")))
    .withColumn("product_id", F.trim(F.col("product_id")))
    .withColumn("store_id", F.trim(F.col("store_id")))

    # Cast ID columns
    .withColumn("sale_id", F.col("sale_id").cast(IntegerType()))
    .withColumn("customer_id", F.col("customer_id").cast(IntegerType()))
    .withColumn("product_id", F.col("product_id").cast(IntegerType()))
    .withColumn("store_id", F.col("store_id").cast(IntegerType()))

    # Convert sale_date to Date
    .withColumn("sale_date", F.to_date(F.col("sale_date"), "dd-MM-yyyy"))

    # Cast numeric columns
    .withColumn("quantity", F.col("quantity").cast(IntegerType()))
    .withColumn("unit_price", F.col("unit_price").cast(DoubleType()))
    .withColumn("amount", F.col("amount").cast(DoubleType()))

    # Remove duplicate records
    .dropDuplicates(["sale_id"])
)

Fact_Sales_Silver_tbl.show(truncate=False)


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
