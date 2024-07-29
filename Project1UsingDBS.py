# Databricks notebook source
dbutils.widgets.dropdown("time_period","Weekly",["Weekly","monthly"])

# COMMAND ----------

from datetime import date,timedelta,datetime
from pyspark.sql.functions import * 

time_period= dbutils.widgets.get("time_period")
print(time_period)
today = date.today()

if time_period=='Weekly':
    start_date=today-timedelta(days=today.weekday(),weeks=1)-timedelta(days=1)
    end_date=start_date+timedelta(days=6)

else:
    first=today.replace(day=1)
    end_date=first-timedelta(days=1)
    start_date=first-timedelta(days=end_date.day)
print(start_date,end_date)

# COMMAND ----------

df = spark.read.csv("/FileStore/tables/superstore.csv",header=True,inferSchema=True)
display(df)

# COMMAND ----------

# MAGIC %md
# MAGIC Business Rules

# COMMAND ----------

#Converting the df into table
df.createOrReplaceTempView("sample")

# COMMAND ----------

# DBTITLE 1,Total Number Of Customers
# MAGIC %sql select count(distinct customer_id) from sample

# COMMAND ----------

# MAGIC %sql select count(distinct customer_id ) from sample where order_date between '2024-06-01' and '2024-06-30'

# COMMAND ----------

# DBTITLE 1,Total Number of Customers
display(spark.sql(f'''select count(distinct customer_id ) from sample where order_date between '{start_date}' and '{end_date}' '''))

# COMMAND ----------

# DBTITLE 1,Total Number of order
display(spark.sql(f'''select count(distinct order_id) from sample where order_date between '{start_date}' and '{end_date}' '''))

# COMMAND ----------

# DBTITLE 1,Total sales and profit
# MAGIC %sql select sum(sales) ,sum(profit) from sample

# COMMAND ----------

# DBTITLE 1,Top sales by country
# MAGIC %sql select sum(sales),country from sample group by 2

# COMMAND ----------

# DBTITLE 1,Most Profitable region and country
# MAGIC %sql select sum(sales),country , region from sample group by 2,3
# MAGIC order by 1 desc

# COMMAND ----------

# DBTITLE 1,Top Sales Category Product
# MAGIC %sql select sum(sales),category from sample group by 2
# MAGIC order by 1 desc

# COMMAND ----------

# DBTITLE 1,Top 10 sales sub category
# MAGIC %sql select sum(sales),sub_category from sample group by 2
# MAGIC order by 1 desc limit 10

# COMMAND ----------

# DBTITLE 1,Most ordered category Product
# MAGIC %sql select sum(quantity),product_name from sample group by 2
# MAGIC order by 1 desc

# COMMAND ----------

# DBTITLE 1,Top Customer based on sales and City
# MAGIC %sql select sum(sales),customer_name,city from sample group by 2,3
# MAGIC order by 1 desc

# COMMAND ----------


