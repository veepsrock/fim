# Databricks notebook source
import requests
import pandas as pd
import json
import pyspark.pandas as ps
import numpy as np
from pyspark.sql import functions as F
from pyspark.sql.window import Window

# COMMAND ----------

# MAGIC %md
# MAGIC # Read Data

# COMMAND ----------

medicare_df =spark.table("medicare_plan_d_clean").cache() # cache will save as much in memory as possible
df = medicare_df.toPandas()

# COMMAND ----------

# Displaying medicare_df and grouping by "PRSCRBR_NPI" and summing the values of "Bene_Feml_Cnt" and "Bene_Male_Cnt"
display(
    medicare_df.groupBy("PRSCRBR_NPI").agg(
            F.sum(F.col("Bene_Feml_Cnt") + F.col("Bene_Male_Cnt")).alias("Total_Count")
        )
    )


# COMMAND ----------

# Displaying medicare_df and grouping by "PRSCRBR_NPI" and summing the values of "Bene_Feml_Cnt" and "Bene_Male_Cnt"
display(
    medicare_df.select(
        "PRSCRBR_NPI",
        "Bene_Feml_Cnt",
        "Bene_Male_Cnt", 
        (F.col("Bene_Feml_Cnt") + F.col("Bene_Male_Cnt")).alias("Bene_total_Count"),
        )
    )


# COMMAND ----------

display(
    medicare_df.select(
        "PRSCRBR_NPI",
        "Bene_Feml_Cnt",
        "Bene_Male_Cnt")
        .groupBy("PRSCRBR_NPI").agg(
            F.sum(F.col("Bene_Feml_Cnt") + F.col("Bene_Male_Cnt")).alias("Total_Count")
        ),
    )


# COMMAND ----------

def eda(df):
    nulls = pd.DataFrame(df.isnull().sum()).T # Check for nulls
    datatypes = pd.DataFrame(df.dtypes).T # Check datatypes
    summary = pd.concat([nulls, datatypes], keys = ["nulls", "datatypes"]) # Create pandas dataframe, because I think it's easier to read
    return summary

# COMMAND ----------

eda(df)

# COMMAND ----------

df["Bene_Avg_Age"].mean()

# COMMAND ----------

df["Bene_Avg_Risk_Scre_Num"].mean()

# COMMAND ----------

df.columns

# COMMAND ----------

pa = df[df['Prscrbr_State_Abrvtn'] == 'PA']

# COMMAND ----------

avg_risk_score = pa.groupby("Prscrbr_State_FIPS")["Bene_Ave_Risk_Scre_F"].mean()

# COMMAND ----------

avg_risk_score = df.groupby("Prscrbr_State_Abrvtn")["Bene_Ave_Risk_Scre_F"].median()

# COMMAND ----------

risk = avg_risk_score.reset_index()

# COMMAND ----------

risk[risk["Bene_Ave_Risk_Scre_F"] > 1.4]

# COMMAND ----------

fim_states = ["AR", "CA", "MA", "NC", "NJ", "NY", "OR", "WA", "DE", "IL", "NM", "VA"]

# COMMAND ----------

filtered_risk = risk[risk["Prscrbr_State_Abrvtn"].isin(fim_states)]

# COMMAND ----------

filtered_risk

# COMMAND ----------

display(avg_risk_score.plot(kind='bar'))

# COMMAND ----------

pa.shape

# COMMAND ----------



# COMMAND ----------

import matplotlib.pyplot as plt

df["Bene_Avg_Risk_Scre_Num"].plot(kind='hist', bins=20)
plt.xlabel('Bene_Avg_Risk_Scre_Num')
plt.title('Distribution of Bene_Avg_Risk_Scre_Num')
plt.show()

# COMMAND ----------

df.columns

# COMMAND ----------

import plotly.express as px

fig = px.histogram(df, x='Bene_Avg_Risk_Scre_Num', color='Prscrbr_State_Abrvtn', nbins=20)
fig.update_layout(
    xaxis_title='Bene_Avg_Risk_Scre_Num',
    yaxis_title='Count',
    title='Distribution of Bene_Avg_Risk_Scre_Num by Prscrbr_State_Abrvtn'
)
fig.show()

# COMMAND ----------

import plotly.express as px

ca_data = df[df['Prscrbr_State_Abrvtn'] == 'CA']
fig = px.histogram(ca_data, x='Bene_Avg_Risk_Scre_Num', nbins=20)

fig.update_layout(
    xaxis_title='Bene_Avg_Risk_Scre_Num',
    yaxis_title='Count',
    title='Distribution of Bene_Avg_Risk_Scre_Num in California'
)

fig.show()

# COMMAND ----------

csp = pd.read_csv("cspuf2021.csv")

# COMMAND ----------

csp.shape

# COMMAND ----------

csp.head()

# COMMAND ----------

