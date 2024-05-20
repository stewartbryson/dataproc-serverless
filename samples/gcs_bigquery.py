from pyspark.sql import SparkSession

spark = (
    SparkSession
    .builder
    .appName('gcs_bigquery_sample')
    .getOrCreate()
)

spark.conf.set('temporaryGcsBucket', "dataproc-sample-temp")

file_path = "gs://dataproc-sample-data/house-price.parquet"
#file_path = "house-price.parquet"

df = (
    spark.read.option("inferSchema", "true")
    .option("header", "true")
    .parquet(file_path)
    .createOrReplaceTempView("temp_housing_model")
)

# Paste in the straight SQL to create the view
df = (
    spark.sql("""
            create or replace temporary view temp_housing_prices as
            select 
                price as unit_price,
                area as total_area,
                bedrooms as number_bedrooms
            from temp_housing_model
            """
    )
    .collect()
)

# Or use the spark shorthand
df = (
    spark.sql("""
            select 
                price as unit_price,
                area as total_area,
                bedrooms as number_bedrooms
            from temp_housing_model
            """
    )
    .createOrReplaceTempView("temp_housing_model2")
)

# Save to BigQuery
df = (
    spark.sql("""
            select 
                distinct *
            from temp_housing_prices
            """
    )
    .write
    .mode("overwrite")
    .format('bigquery')
    .option('table', 'housing.housing_prices')
    .save()
)