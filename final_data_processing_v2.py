from pyspark.sql import SparkSession
from pyspark.sql.functions import col, month, year, to_date
from pyspark.sql.types import StringType

# Create a Spark session
spark = SparkSession.builder.appName("CurrencyAverage").getOrCreate()

# Replace 'your_bucket' with the actual GCS bucket name
gcs_bucket = "your_bucket"
file_path = f"gs://{gcs_bucket}/yourfile.csv"
output_path = f"gs://{gcs_bucket}/new_output_path2/outputname"

# Read data from GCS
df = spark.read.csv(file_path, header=True, inferSchema=True)

# Rename the column
df = df.withColumnRenamed('_c0', 'Date')

# Remove leading and trailing whitespaces and empty strings
df = df.withColumn('Date', col('Date').cast(StringType()))  # Ensure the 'Date' column is StringType
df = df.na.drop()

# Convert to date format
date_format = 'd-MMM-yy'
df = df.withColumn('Date', to_date(col('Date'), date_format))

# Add 'Month' and 'Year' columns to the DataFrame
df = df.withColumn('Month', month(col('Date')))
df = df.withColumn('Year', year(col('Date')))

# Group the data by Year, Month, and calculate the average for each currency
average_rates = df.groupBy('Year', 'Month').agg(
    {'USD': 'avg', 'GBP': 'avg', 'EUR': 'avg', 'JPY100': 'avg', 'CHF': 'avg',
     'AUD': 'avg', 'CAD': 'avg', 'SGD': 'avg', 'HKD100': 'avg', 'THB100': 'avg',
     'PHP100': 'avg', 'TWD100': 'avg', 'KRW100': 'avg', 'IDR100': 'avg',
     'SAR100': 'avg', 'SDR': 'avg', 'CNY': 'avg', 'BND': 'avg'}
)

# Rename columns using alias
for column_name in average_rates.columns:
    new_column_name = column_name.replace("avg(", "avg_").replace(")", "")
    average_rates = average_rates.withColumnRenamed(column_name, new_column_name)

# Repartition to a single partition before writing
average_rates = average_rates.repartition(1)

# Save the results to GCS
average_rates.write.csv(output_path, header=True, mode='overwrite')

# Stop Spark session
spark.stop()

print('complete')
