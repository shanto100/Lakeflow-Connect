# Databricks notebook source
# MAGIC %md
# MAGIC This is the original load of a customer and a product table to generate the fact table that gets continuously updated.

# COMMAND ----------

df = spark.read.format("json").load('/Volumes/cmoore_customer_demos/pubsec_demo/demo_source_files/mock_data.json')
df = df.selectExpr("id as customer_id", "first_name || ' ' || last_name as name", "email")
df = df.dropna()
display(df)

# COMMAND ----------

#df.write.saveAsTable('cmoore_customer_demos.demo_cdc.customer')

# COMMAND ----------

# df1 = spark.read.format("csv").option("header","true").load('dbfs:/home/cary.moore@databricks.com/mock_product_data.csv')
# df1 = df1.selectExpr("product_id", "Product" )
# display(df1)

# COMMAND ----------

# df1.write.saveAsTable('cmoore_customer_demos.demo_cdc.product')

# COMMAND ----------

jdbcUsername = dbutils.secrets.get(scope = "oneenvkeys", key = "dbuser")
jdbcPassword = dbutils.secrets.get(scope = "oneenvkeys", key = "dbpassword")

# COMMAND ----------

jdbcHostname = "oneenvsql.database.windows.net"
jdbcPort = 1433
jdbcDatabase = "oneenvsqldb"
jdbcProperties = {
                   "user": jdbcUsername,
                   "password": jdbcPassword
                 }

tableName = "sys.tables"

# Create the JDBC URL without passing in the user and password parameters.
jdbcUrl = "jdbc:sqlserver://{}:{};database={}".format(
  jdbcHostname, jdbcPort, jdbcDatabase
)

jdbcUrl

# COMMAND ----------

# df1.write \
#     .format("jdbc") \
#     .option("url", jdbcUrl) \
#     .option("user", jdbcUsername) \
#     .option("password", jdbcPassword) \
#     .option("dbtable", "cdc_product") \
#     .save()

# COMMAND ----------

# df.write \
#     .format("jdbc") \
#     .option("url", jdbcUrl) \
#     .option("user", jdbcUsername) \
#     .option("password", jdbcPassword) \
#     .option("dbtable", "cdc_customer") \
#     .save()

# COMMAND ----------

# MAGIC %md
# MAGIC Here is where I load the Customer and the Product data from the database.

# COMMAND ----------

df = spark.read \
    .format("jdbc") \
    .option("url", jdbcUrl) \
    .option("user", jdbcUsername) \
    .option("password", jdbcPassword) \
    .option("dbtable", "cdc_customer") \
    .load()

# COMMAND ----------

df1 = spark.read \
    .format("jdbc") \
    .option("url", jdbcUrl) \
    .option("user", jdbcUsername) \
    .option("password", jdbcPassword) \
    .option("dbtable", "cdc_product") \
    .load()

# COMMAND ----------

df.createOrReplaceTempView("customer")
df1.createOrReplaceTempView("product")

# COMMAND ----------

# MAGIC %md
# MAGIC Here is where I generate random purchases for the customers and products with the current date and time.

# COMMAND ----------

#generate random ids to select random customer and product combinations.
from pyspark.sql.functions import *
df_rand = spark.range(1,1001)
df_rand = df_rand.withColumn("random", floor(rand()*100))
df_rand.createOrReplaceTempView("random_ids")

# COMMAND ----------

#create a purchase data frame to append to the table that's already there.
df_purchase = spark.sql("""select a.customer_id, c.product_id, now() as purchase_date
from customer a
join random_ids b
on a.customer_id = b.id
join product c
on b.random = c.product_id""")
df_purchase = df_purchase.withColumn("qty", floor(rand()*10+1) )
df_purchase = df_purchase.withColumn("amount", (rand()*10+1)*col("qty"))
#display(df_purchase)

# COMMAND ----------

#This was only necessary to create the table the first time. 

# df_purchase.write \
#     .format("jdbc") \
#     .option("url", jdbcUrl) \
#     .option("user", jdbcUsername) \
#     .option("password", jdbcPassword) \
#     .option("dbtable", "cdc_purchase") \
#     .save()

# COMMAND ----------

# Here I'm appending the new records to the current table, the new records should get replicated by Arcion.

df_purchase.write \
    .format("jdbc") \
    .mode("append") \
    .option("url", jdbcUrl) \
    .option("user", jdbcUsername) \
    .option("password", jdbcPassword) \
    .option("dbtable", "cdc_purchase") \
    .save()

# COMMAND ----------

#Just to show that the new records are inserted.

df_purchase_current = spark.read \
    .format("jdbc") \
    .option("url", jdbcUrl) \
    .option("user", jdbcUsername) \
    .option("password", jdbcPassword) \
    .option("dbtable", "cdc_purchase") \
    .load()
df_purchase_current.count()
