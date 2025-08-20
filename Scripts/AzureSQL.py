# Databricks notebook source
dbutils.secrets.listScopes()

# COMMAND ----------

dbutils.secrets.list(scope="scope-azuresql")

# COMMAND ----------

scope = "scope-azuresql"
sqluser_key = "sql-user"
sqlpassword_key = "sql-password"

# COMMAND ----------

import getpass

# COMMAND ----------

jdbcHostName = "sqlfinalproject.database.windows.net"
jdbcDatabase = "adbsql-finalproject"
jdbcPort = 1433
jdbcUsername = dbutils.secrets.get(scope = scope, key = sqluser_key)
jdbcPassword = dbutils.secrets.get(scope= scope, key = sqlpassword_key)

# COMMAND ----------

 # URL JDBC
jdbcUrl = f"jdbc:sqlserver://{jdbcHostName}:{jdbcPort};database={jdbcDatabase};encrypt=true;trustServerCertificate=false;hostNameInCertificate=*.database.windows.net;loginTimeout=30;"

# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW TABLES IN catalog_dev.gold

# COMMAND ----------

df_summary_customer_segment = spark.table("catalog_dev.gold.summary_customer_segment")
df_summary_product_category = spark.table("catalog_dev.gold.summary_product_category")

# COMMAND ----------

df_summary_customer_segment.write \
    .format("jdbc") \
    .option("url", jdbcUrl) \
    .option("dbtable", "Summary_Customer_Segment") \
    .option("user", jdbcUsername) \
    .option("password", jdbcPassword) \
    .mode("overwrite") \
    .save()

# COMMAND ----------

df_summary_product_category.write \
    .format("jdbc") \
    .option("url", jdbcUrl) \
    .option("dbtable", "Summary_Product_Category") \
    .option("user", jdbcUsername) \
    .option("password", jdbcPassword) \
    .mode("overwrite") \
    .save()

# COMMAND ----------

