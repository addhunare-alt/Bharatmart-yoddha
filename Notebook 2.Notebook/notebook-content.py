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

for t in ["fact_sales","dim_customers","dim_products","dim_stores","dim_targets"]:
    df = spark.read.option("header","true").option("inferSchema","true") \
              .csv(f"Files/{t}.csv")
    df.write.mode("overwrite").format("delta").saveAsTable(t)



# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
