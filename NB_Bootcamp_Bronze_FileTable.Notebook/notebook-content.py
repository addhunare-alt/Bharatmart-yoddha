# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "ce5c9a8b-70ca-4182-a467-a250c775f409",
# META       "default_lakehouse_name": "Final_LH_Bronze",
# META       "default_lakehouse_workspace_id": "52527b90-6e37-41eb-b962-eabd19edbe03",
# META       "known_lakehouses": [
# META         {
# META           "id": "ce5c9a8b-70ca-4182-a467-a250c775f409"
# META         }
# META       ]
# META     },
# META     "warehouse": {
# META       "known_warehouses": []
# META     }
# META   }
# META }

# CELL ********************

for t in ["fact_sales","dim_customers","dim_products","dim_stores","dim_targets"]: 
    df = (spark.read
    .option("header","true")
    .option("inferSchema","true") 
    .csv(f"Files/{t}.csv")) 
    
    df.write.mode("overwrite").format("delta").saveAsTable(t)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
