import pyspark
from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F
from pyspark.sql.functions import col, lit, concat, desc, first
from itertools import chain

VendorMapping = {
    '1': 'Creative Mobile Technologies, LLC',
    '2': 'VeriFone Inc.'
}

PaymentTypeMapping = {
    '1': 'Credit card',
    '2': 'Cash',
    '3': 'No charge',
    '4': 'Dispute',
    '5': 'Unknown',
    '6': 'Voided trip'
}

spark = SparkSession.builder.appName("Avenga test").getOrCreate()
df = spark.read.format("csv").option("header", "true").load("taxi_tripdata.csv")

report = (

)
# ID to name
df = df.withColumn("Vendor", F.create_map([lit(x) for x in chain(*VendorMapping.items())])[col('VendorID')])
df = df.withColumn("PaymentType",
                   F.create_map([lit(x) for x in chain(*PaymentTypeMapping.items())])[col('payment_type')])

# PaymentRate per type and vendor
group_two = df.groupBy("VendorID", "payment_type") \
    .agg(
    F.sum("total_amount").alias("payment_total"),
    F.sum("passenger_count").alias("passengers_count")
)

df = df.join(group_two, ["VendorID", "payment_type"])
df = df.withColumn("PaymentRate", col("payment_total") / col("passengers_count"))

# Next rate
over_rate = Window.partitionBy("VendorID").orderBy("PaymentRate").rowsBetween(1, Window.unboundedFollowing)
df = df.withColumn("NextPaymentRate", first("PaymentRate").over(over_rate))

# Max rate per vendor
group_vendor = df.groupBy("VendorID") \
    .agg(F.max("PaymentRate").alias("MaxPaymentRate"))
df = df.join(group_vendor, ["VendorID"])

# Percents to next rate --- nextpaymentrate / paymentrate

grow = (col("NextPaymentRate") / col("PaymentRate"))*100-100
df = df.withColumn("%ToNextRate", grow)
df = df.withColumn("%ToNextRate", concat(col("%ToNextRate"), lit('%')).cast("string"))

# output
df.coalesce(1).select('Vendor', 'PaymentType', 'PaymentRate', 'NextPaymentRate', 'MaxPaymentRate', '%ToNextRate') \
    .dropDuplicates().na.fill(value=0, subset=["NextPaymentRate"]).filter("PaymentRate != NextPaymentRate") \
    .write.format("csv").option("header", "true").save("./report", mode="overwrite")

