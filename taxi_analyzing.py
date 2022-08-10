import pyspark
from pyspark.sql import SparkSession, DataFrame, Window
from pyspark.sql import functions as F
from pyspark.sql.functions import col, lit, concat, first
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


def read_csv(spark_template: SparkSession, path) -> DataFrame:
    return spark_template.read.format("csv").option("header", "true").load(path)


def save_to_csv(df: DataFrame) -> None:
    df.write.format("csv").option("header", "true").save("./csv_report", mode="overwrite")


def save_to_json(dataf: DataFrame) -> None:
    dataf.write.format("json").option("header", "true").save("./json_report", mode="overwrite")


# Getting starting data, by mapping Id to Names/Types and Calculating and providing payment rate
def starting_transform(df: DataFrame) -> DataFrame:
    template = (
        df
        .groupBy("VendorID", "payment_type")
        .agg(
            F.create_map([lit(x) for x in chain(*VendorMapping.items())])[col('VendorID')].alias("Vendor"),
            F.create_map([lit(x) for x in chain(*PaymentTypeMapping.items())])[col('payment_type')].alias(
                "PaymentType"),
            F.sum("total_amount").alias("payment_total"),
            F.sum("passenger_count").alias("passengers_count"))
        .withColumn("PaymentRate", col("payment_total") / col("passengers_count"))
    )
    return template.select("Vendor", "PaymentType", "PaymentRate")


# Determine next payment rate from current payment type
def find_next_element(df: DataFrame) -> DataFrame:
    return df.withColumn("NextPaymentRate", first("PaymentRate").over(
            Window.partitionBy("Vendor").orderBy("PaymentRate").rowsBetween(1, Window.unboundedFollowing)))


# Finding max payment rate for vendor
def find_max_rate(df: DataFrame) -> DataFrame:
    max_rate = df.groupBy("Vendor").agg(F.max("PaymentRate").alias("MaxPaymentRate"))
    return df.join(max_rate, ["Vendor"])


# Calculating percents to achieve nex payment rate
def percents_to_next(df: DataFrame) -> DataFrame:
    return df.withColumn("%ToNextRate", col("NextPaymentRate") / col("PaymentRate") * 100 - 100) \
            .withColumn("%ToNextRate", concat(col("%ToNextRate"), lit('%')).cast("string"))


def main():
    spark = SparkSession.builder.appName("Avenga test").getOrCreate()
    report = read_csv(spark, "./taxi_tripdata.csv")

    report = starting_transform(report)
    report = find_next_element(report)
    report = find_max_rate(report)
    report = percents_to_next(report)

    # report.show()
    # report.printSchema()
    save_to_csv(report)
    save_to_json(report)


main()

# Schema:
# root
#  |-- Vendor: string (nullable = true)
#  |-- PaymentType: string (nullable = true)
#  |-- PaymentRate: double (nullable = true)
#  |-- NextPaymentRate: double (nullable = true)
#  |-- MaxPaymentRate: double (nullable = true)
#  |-- %ToNextRate: string (nullable = true)
