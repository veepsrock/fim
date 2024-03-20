# Databricks notebook source
import requests
import pandas as pd
import json
import pyspark.pandas as ps
from pyspark.sql import functions as F
from pyspark.sql.window import Window

# COMMAND ----------

def convert_to_float(obj):
    for obj in json_lists:    
        for key, value in obj.items():
            try:
                obj[key] = float(value)
            except ValueError:
                pass
    return obj

# COMMAND ----------

json_lists=[]
for i in range(0, 2_000_000, 5000):
    url = "https://data.cms.gov/data-api/v1/dataset/14d8e8a9-7e9b-4370-a044-bf97c46b4b44/data?size=5000&offset=" + str(i)
    response = requests.get(url)
    json_obj =response.json()
    json_lists.extend(json_obj)
    print(str(i))
for obj in json_lists:
    if isinstance(obj, dict):
            for key, value in obj.items():
                try:
                    obj[key] = float(value)
                except ValueError:
                    pass

#json_lists = convert_to_float(json_lists)
df = pd.DataFrame(json_lists)

# COMMAND ----------

# Write the data to a table.
table_name = "medicare_part_d_providers"
spark_df = spark.createDataFrame(df)
spark_df.write.format("delta").saveAsTable(table_name)

# COMMAND ----------

# MAGIC %md
# MAGIC # Read Data

# COMMAND ----------

spark_df =spark.table("medicare_part_d_providers").cache() # cache will save as much in memory as possible
display(spark_df)

# COMMAND ----------

int_cols = [col for col in spark_df.columns if col.endswith("_Cnt")]
float_cols = [col for col in spark_df.columns if "Tot_" in col] + [col for col in spark_df.columns if col.endswith("_Rate")]
fix_cols = int_cols + float_cols
pass_cols = [col for col in spark_df.columns if col not in fix_cols]

# COMMAND ----------

medicare_df = spark_df.select(
    *[F.col(col) for col in pass_cols],
    *[F.col(col).cast("int") for col in integer_cols],
    *[F.col(col).cast("float") for col in float_cols],
    F.col("Bene_Avg_Risk_Scre").cast("float").alias("Bene_Ave_Risk_Scre_F")
)

# COMMAND ----------

medicare_df.write.format("delta").saveAsTable("medicare_plan_d_clean")

# COMMAND ----------

medicare_df =spark.table("medicare_plan_d_clean").cache() # cache will save as much in memory as possible

# COMMAND ----------

display(medicare_df)

# COMMAND ----------

# MAGIC %md 
# MAGIC # Pyspark notes

# COMMAND ----------

# Import the necessary libraries
import matplotlib.pyplot as plt

# Convert the column to a Pandas DataFrame for visualization
age_distribution = medicare_df.select("Bene_Avg_Age").toPandas()

# Plot the distribution
plt.hist(age_distribution, bins=20, alpha=0.5, color='b')
plt.xlabel("Age")
plt.ylabel("Frequency")
plt.title("Distribution of Bene_Avg_Age")
plt.show()

# COMMAND ----------

display(medicare_df.where(F.col("Bene_Age_LT_65_Cnt").isNotNull()))

# COMMAND ----------

display(
    integer_col_df.groupBy("PRSCRBR_NPI").agg(
        *[F.count_if(F.col(col).isNull()).alias(f"{col}_nulls") for col in integer_cols]
    )
)

# COMMAND ----------

display(
    spark_df.groupBy("Bene_Avg_Risk_Scre")
    .agg(F.count("*").alias("count")) # F.lit(1) is counting the constant 1 (same as COUNT * in SQL)
    .orderBy(F.col("count").desc())
)

# COMMAND ----------

display(
    spark_df.select(
        "PRSCRBR_NPI",
        F.col("Bene_Avg_Risk_Scre").cast("float"),
        F.concat_ws(",", "Prscrbr_Last_Org_Name", "Prscrbr_First_Name").alias("Name")
    )
)

# COMMAND ----------

# write a query that converts all the columns I want and then write to a new table
# Create a list of columns that are fine as is pass_through_cols
# Select individual column like avg_score
# Consider column order at the end

# COMMAND ----------

#spark_df = spark.read.table("medicare_part_d")
#df = spark_df.toPandas()

# COMMAND ----------

def eda(df):
    nulls = pd.DataFrame(df.isnull().sum()).T # Check for nulls
    datatypes = pd.DataFrame(df.dtypes).T # Check datatypes
    summary = pd.concat([nulls, datatypes], keys = ["nulls", "datatypes"]) # Create pandas dataframe, because I think it's easier to read
    return summary