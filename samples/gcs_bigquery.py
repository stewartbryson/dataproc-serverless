from pyspark.sql import SparkSession

# Create a Spark session
spark = (
    SparkSession
    .builder
    .appName('gcs_bigquery_sample')
    .getOrCreate()
)

# Add BigQuery stuff
spark.conf.set('temporaryGcsBucket', "dataproc-sample-temp")

file_path = "gs://dataproc-sample-data/house-price.parquet"
#file_path = "house-price.parquet"

# Create a dataframe for the parquet file
# Infer the schema, so no Structs necessary
df = (
    spark.read.option("inferSchema", "true")
    .option("header", "true")
    .parquet(file_path)
    .createOrReplaceTempView("temp_housing_model")
)

# Paste in the straight SQL to create transformation view
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

# Or use the spark shorthand to create the view
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

# Save to BigQuery using the Spark way
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
