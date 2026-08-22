# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "b5feeaeb-4539-47bb-b4e4-6dce9a85ca02",
# META       "default_lakehouse_name": "Bharatmart_Bronze_DLH",
# META       "default_lakehouse_workspace_id": "52527b90-6e37-41eb-b962-eabd19edbe03",
# META       "known_lakehouses": [
# META         {
# META           "id": "b5feeaeb-4539-47bb-b4e4-6dce9a85ca02"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

for t in ["dim_products","dim_stores","dim_targets"]:
    df = spark.read.option("header","true").option("inferSchema","true") \
              .csv(f"Files/{t}.csv")
    df.write.mode("overwrite").format("delta").saveAsTable(t)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
