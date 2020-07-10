from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, date_format, udf
from pyspark.sql.types import (DateType, IntegerType, FloatType, StringType,
                               StructField, StructType, TimestampType)


import pyspark.sql.functions as func




spark = SparkSession.builder.appName("Read Transactions").getOrCreate()

csv_schema = StructType([StructField('customer_id', IntegerType()),
                         StructField('amount', FloatType()),
                         StructField('purchased_at', TimestampType()),
                         ])

dataframe = spark.read.csv("transactions.csv",
                           schema=csv_schema,
                           header=True)


# Add a new column by formatting the original date

formatted_df = dataframe.withColumn("date_string",
                                    date_format(col("purchased_at"),
                                                'MM/dd/yyyy'))

# Create a user defined function
string_to_date = \
    udf(lambda text_date: datetime.strptime(text_date, '%m/%d/%Y'),
        DateType())

typed_df = formatted_df.withColumn(
    "date", string_to_date(formatted_df.date_string))


# Group By and Select the data already aggregated
sum_df = typed_df.groupBy("customer_id", "date").sum()
#sum_df.show()

stats_df = \
    sum_df.select(
        col('customer_id'),
        col('date'),
        col('sum(amount)').alias('amount'))



# Load separate file where we store user names...
name_schema = StructType([StructField("id", IntegerType()),
                          StructField("name", StringType()),
                          StructField("currency", StringType()),
                          StructField("rate", FloatType())])

names_df = spark.read.csv('names.csv',
                          schema=name_schema,
                          header=True)

#currency schema
currency_schema = StructType([StructField("currencyCode", StringType()),
                          StructField("exchange", FloatType())])


# ...and join to the aggregates
joint_df = stats_df.join(names_df, stats_df.customer_id == names_df.id)
#joint_df.show()


#will cambios //////////////////////////////////////////////////

currency_df = spark.read.csv('exchange_rates.csv',
                          schema=currency_schema,
                          header=True)
                          

joint_Currency = joint_df.join(currency_df, joint_df.currency == currency_df.currencyCode)


joint_Currency = joint_Currency.withColumn("Total_Dollar",
                                    col("amount") *  col("exchange"))


joint_Currency = joint_Currency.withColumn("Total_Dollar", func.round(joint_Currency["Total_Dollar"], 2))

#joint_Currency.show()

cols = ['customer_id','name','date','amount','currency','exchange','Total_Dollar']
joint_Currency.select(*cols).show()

#/////////////////////////////////////////////////////////////////////////

