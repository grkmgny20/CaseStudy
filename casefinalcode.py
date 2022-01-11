#!/usr/bin/env python
# coding: utf-8

# In[72]:


import pyspark
import boto3
import pandas as pd


# In[155]:


from pyspark.sql import Row


# In[97]:


from pyspark.sql.functions import desc


# In[118]:


from pyspark.sql.functions import col


# In[117]:


from pyspark.sql.types import StructType,StructField 
from pyspark.sql.types import StringType, IntegerType, ArrayType


# In[112]:


import pyspark.sql.functions as F


# In[73]:


from pyspark import SparkContext
from pyspark.sql import SQLContext
from pyspark.sql.types import *


# In[74]:


spark = SparkSession    .builder    .appName("gorkem_spark_app")    .getOrCreate()


# In[128]:


s3 = boto3.client("s3", aws_access_key_id = "AKIARG26JGKLIBHTYA57", aws_secret_access_key= "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")


# In[40]:


obj1 = s3.get_object(Bucket= "datalake-volt", Key= "olist_order_items_dataset.csv")
obj2 = s3.get_object(Bucket= "datalake-volt", Key= "olist_orders_dataset.csv")
obj3 = s3.get_object(Bucket= "datalake-volt", Key= "olist_sellers_dataset.csv")


# In[41]:


order_item_dataset_df = pd.read_csv(obj1['Body']) # 'Body' is a key word


# In[42]:


orders_dataset_df= pd.read_csv(obj2['Body'])


# In[43]:


sellers_dataset_df=pd.read_csv(obj3['Body'])


# In[20]:


import pyspark
from pyspark.sql import SparkSession


# In[44]:


df1=spark.createDataFrame(order_item_dataset_df)


# In[47]:


from pyspark.sql.types import *


# In[66]:


df2_schema=StructType([StructField("order_id",StringType(),True),StructField("customer_id",StringType(),True),StructField("order_status",StringType(),True),StructField("order_purchase_timestamp",StringType(),True),StructField("order_approved_at",StringType(),True),StructField("order_delivered_carrier_date",StringType(),True),StructField("order_delivered_customer_date",StringType(),True),StructField("order_estimated_delivery_date",StringType(),True)])
df2=spark.createDataFrame(orders_dataset_df,schema=df2_schema)


# In[45]:


df3=spark.createDataFrame(sellers_dataset_df)


# In[51]:


df1.createOrReplaceTempView("orderitem")


# In[52]:


df2.createOrReplaceTempView("orderdataset")


# In[53]:


df3.createOrReplaceTempView("sellerdataset")


# In[71]:


df1=spark.createDataFrame(order_item_dataset_df)


# In[26]:


df1.createOrReplaceTempView("orderitem")


# In[106]:


df4=df1.join(df2, df1.order_id == df2.order_id, 'outer')


# In[110]:


df5=df4.select("seller_id","shipping_limit_date","order_delivered_customer_date")


# In[115]:


df5.withColumn('result',F.when(F.col("order_delivered_customer_date") <= F.col("shipping_limit_date"),1).otherwise(2)).show()


# In[121]:


df6=df5.withColumn('result',F.when(F.col("order_delivered_customer_date") <= F.col("shipping_limit_date"),"Done").otherwise("No"))


# In[152]:


df7=df6.filter(col("result") == "No")
df8=df7.na.drop("any")


# In[156]:


df8_final= df8.dropDuplicates()


# In[181]:


df8_final.createOrReplaceTempView("nan_deneme")


# In[175]:


df10=df8_final.dropna(how='any' )


# In[186]:


df_10 = spark.sql("select * from nan_deneme where order_delivered_customer_date != 'NaN'")


# In[188]:


df_10.coalesce(1).write.csv("/home/jovyan/work/resultcsv")


# In[189]:


type(df_10)


# In[190]:


pandas_df = df_10.toPandas()


# In[191]:


pandas_df.to_csv("caseresult.csv")


# In[193]:


s3 = boto3.client("s3", aws_access_key_id = "AKIARG26JGKLIBHTYA57", aws_secret_access_key= "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")

s3.put_object(
    Body=open("caseresult.csv").read(),
    Bucket="datalake-volt",
    Key="caseresult.csv"
)


# In[ ]:




