![Untitled Diagram drawio](https://github.com/soumilshah1995/DeltaHudiTransformations/assets/39345855/2bea3b84-4f4c-47ea-ae41-7f5329ea6d00)

# Step 1 : create two Hudi Tables 
```
"""
--conf spark.serializer=org.apache.spark.serializer.KryoSerializer --conf spark.sql.hive.convertMetastoreParquet=false --conf spark.sql.hive.convertMetastoreParquet=false --conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.hudi.catalog.HoodieCatalog --conf spark.sql.legacy.pathOptionBehavior.enabled=true --conf spark.sql.extensions=org.apache.spark.sql.hudi.HoodieSparkSessionExtension

"""
try:
    import sys, random, uuid
    from pyspark.context import SparkContext
    from pyspark.sql.session import SparkSession
    from awsglue.context import GlueContext
    from awsglue.job import Job
    from awsglue.dynamicframe import DynamicFrame
    from pyspark.sql.types import StructType, StructField, StringType
    from pyspark.sql.functions import col, to_timestamp, monotonically_increasing_id, to_date, when
    from pyspark.sql.functions import *
    from awsglue.utils import getResolvedOptions
    from pyspark.sql.types import *
    from datetime import datetime, date
    import boto3, pandas
    from functools import reduce
    from pyspark.sql import Row
except Exception as e:
    print("Modules are missing : {} ".format(e))

job_start_ts = datetime.now()
ts_format = '%Y-%m-%d %H:%M:%S'

spark = (SparkSession.builder.config('spark.serializer', 'org.apache.spark.serializer.KryoSerializer') \
         .config('spark.sql.hive.convertMetastoreParquet', 'false') \
         .config('spark.sql.catalog.spark_catalog', 'org.apache.spark.sql.hudi.catalog.HoodieCatalog') \
         .config('spark.sql.extensions', 'org.apache.spark.sql.hudi.HoodieSparkSessionExtension') \
         .config('spark.sql.legacy.pathOptionBehavior.enabled', 'true').getOrCreate())

sc = spark.sparkContext
glueContext = GlueContext(sc)
job = Job(glueContext)
logger = glueContext.get_logger()


def upsert_hudi_table(glue_database, table_name, record_id, precomb_key, table_type, spark_df, partition_fields,
                      enable_partition, enable_cleaner, enable_hive_sync, enable_clustering,
                      enable_meta_data_indexing,
                      use_sql_transformer, sql_transformer_query,
                      target_path, index_type, method='upsert', clustering_column='default'):
    """
    Upserts a dataframe into a Hudi table.

    Args:
        glue_database (str): The name of the glue database.
        table_name (str): The name of the Hudi table.
        record_id (str): The name of the field in the dataframe that will be used as the record key.
        precomb_key (str): The name of the field in the dataframe that will be used for pre-combine.
        table_type (str): The Hudi table type (e.g., COPY_ON_WRITE, MERGE_ON_READ).
        spark_df (pyspark.sql.DataFrame): The dataframe to upsert.
        partition_fields this is used to parrtition data
        enable_partition (bool): Whether or not to enable partitioning.
        enable_cleaner (bool): Whether or not to enable data cleaning.
        enable_hive_sync (bool): Whether or not to enable syncing with Hive.
        use_sql_transformer (bool): Whether or not to use SQL to transform the dataframe before upserting.
        sql_transformer_query (str): The SQL query to use for data transformation.
        target_path (str): The path to the target Hudi table.
        method (str): The Hudi write method to use (default is 'upsert').
        index_type : BLOOM or GLOBAL_BLOOM
    Returns:
        None
    """
    # These are the basic settings for the Hoodie table
    hudi_final_settings = {
        "hoodie.table.name": table_name,
        "hoodie.datasource.write.table.type": table_type,
        "hoodie.datasource.write.operation": method,
        "hoodie.datasource.write.recordkey.field": record_id,
        "hoodie.datasource.write.precombine.field": precomb_key,
    }

    # These settings enable syncing with Hive
    hudi_hive_sync_settings = {
        "hoodie.parquet.compression.codec": "gzip",
        "hoodie.datasource.hive_sync.enable": "true",
        "hoodie.datasource.hive_sync.database": glue_database,
        "hoodie.datasource.hive_sync.table": table_name,
        "hoodie.datasource.hive_sync.partition_extractor_class": "org.apache.hudi.hive.MultiPartKeysValueExtractor",
        "hoodie.datasource.hive_sync.use_jdbc": "false",
        "hoodie.datasource.hive_sync.mode": "hms",
    }

    # These settings enable automatic cleaning of old data
    hudi_cleaner_options = {
        "hoodie.clean.automatic": "true",
        "hoodie.clean.async": "true",
        "hoodie.cleaner.policy": 'KEEP_LATEST_FILE_VERSIONS',
        "hoodie.cleaner.fileversions.retained": "3",
        "hoodie-conf hoodie.cleaner.parallelism": '200',
        'hoodie.cleaner.commits.retained': 5
    }

    # These settings enable partitioning of the data
    partition_settings = {
        "hoodie.datasource.write.partitionpath.field": partition_fields,
        "hoodie.datasource.hive_sync.partition_fields": partition_fields,
        "hoodie.datasource.write.hive_style_partitioning": "true",
    }

    hudi_clustering = {
        "hoodie.clustering.execution.strategy.class": "org.apache.hudi.client.clustering.run.strategy.SparkSortAndSizeExecutionStrategy",
        "hoodie.clustering.inline": "true",
        "hoodie.clustering.plan.strategy.sort.columns": clustering_column,
        "hoodie.clustering.plan.strategy.target.file.max.bytes": "1073741824",
        "hoodie.clustering.plan.strategy.small.file.limit": "629145600"
    }

    # Define a dictionary with the index settings for Hudi
    hudi_index_settings = {
        "hoodie.index.type": index_type,  # Specify the index type for Hudi
    }

    # Define a dictionary with the Fiel Size
    hudi_file_size = {
        "hoodie.parquet.max.file.size": 512 * 1024 * 1024,  # 512MB
        "hoodie.parquet.small.file.limit": 104857600,  # 100MB
    }

    hudi_meta_data_indexing = {
        "hoodie.metadata.enable": "true",
        "hoodie.metadata.index.async": "true",
        "hoodie.metadata.index.column.stats.enable": "true",
        "hoodie.metadata.index.check.timeout.seconds": "60",
        "hoodie.write.concurrency.mode": "optimistic_concurrency_control",
        "hoodie.write.lock.provider": "org.apache.hudi.client.transaction.lock.InProcessLockProvider"
    }

    if enable_meta_data_indexing == True or enable_meta_data_indexing == "True" or enable_meta_data_indexing == "true":
        for key, value in hudi_meta_data_indexing.items():
            hudi_final_settings[key] = value  # Add the key-value pair to the final settings dictionary

    if enable_clustering == True or enable_clustering == "True" or enable_clustering == "true":
        for key, value in hudi_clustering.items():
            hudi_final_settings[key] = value  # Add the key-value pair to the final settings dictionary

    # Add the Hudi index settings to the final settings dictionary
    for key, value in hudi_index_settings.items():
        hudi_final_settings[key] = value  # Add the key-value pair to the final settings dictionary

    for key, value in hudi_file_size.items():
        hudi_final_settings[key] = value  # Add the key-value pair to the final settings dictionary

    # If partitioning is enabled, add the partition settings to the final settings
    if enable_partition == "True" or enable_partition == "true" or enable_partition == True:
        for key, value in partition_settings.items(): hudi_final_settings[key] = value

    # If data cleaning is enabled, add the cleaner options to the final settings
    if enable_cleaner == "True" or enable_cleaner == "true" or enable_cleaner == True:
        for key, value in hudi_cleaner_options.items(): hudi_final_settings[key] = value

    # If Hive syncing is enabled, add the Hive sync settings to the final settings
    if enable_hive_sync == "True" or enable_hive_sync == "true" or enable_hive_sync == True:
        for key, value in hudi_hive_sync_settings.items(): hudi_final_settings[key] = value

    # If there is data to write, apply any SQL transformations and write to the target path
    if spark_df.count() > 0:
        if use_sql_transformer == "True" or use_sql_transformer == "true" or use_sql_transformer == True:
            spark_df.createOrReplaceTempView("temp")
            spark_df = spark.sql(sql_transformer_query)

        spark_df.write.format("hudi"). \
            options(**hudi_final_settings). \
            mode("append"). \
            save(target_path)


# Define static mock data for customers
customers_data = [
    ("1", "John Doe", "New York", "john@example.com", "2022-01-01", "123 Main St", "NY"),
    ("2", "Jane Smith", "Los Angeles", "jane@example.com", "2022-01-02", "456 Elm St", "CA"),
    ("3", "Alice Johnson", "Chicago", "alice@example.com", "2022-01-03", "789 Oak St", "IL")
]

# Define schema for customers data
customers_schema = StructType([
    StructField("customer_id", StringType(), nullable=False),
    StructField("name", StringType(), nullable=True),
    StructField("city", StringType(), nullable=False),
    StructField("email", StringType(), nullable=False),
    StructField("created_at", StringType(), nullable=False),
    StructField("address", StringType(), nullable=False),
    StructField("state", StringType(), nullable=False)
])

# Create DataFrame for customers
customers_df = spark.createDataFrame(data=customers_data, schema=customers_schema)

# Define static mock data for orders
orders_data = [
    ("101", "1", "P123", "2", "45.99", "2022-01-02"),
    ("102", "1", "P456", "1", "29.99", "2022-01-03"),
    ("103", "2", "P789", "3", "99.99", "2022-01-01"),
    ("104", "3", "P123", "1", "49.99", "2022-01-02")
]

# Define schema for orders data
orders_schema = StructType([
    StructField("order_id", StringType(), nullable=False),
    StructField("customer_id", StringType(), nullable=False),
    StructField("product_id", StringType(), nullable=False),
    StructField("quantity", StringType(), nullable=False),
    StructField("total_price", StringType(), nullable=False),
    StructField("order_date", StringType(), nullable=False)
])

# Create DataFrame for orders
orders_df = spark.createDataFrame(data=orders_data, schema=orders_schema)

# Show the generated DataFrames
customers_df.show()
orders_df.show()

BUCKET= "<BUCKETNAME>"


# =================================================================
# CUSTOMERS
#============================================================
upsert_hudi_table(
    glue_database="testdb",
    table_name="customers",
    record_id="customer_id",
    precomb_key="created_at",
    table_type='COPY_ON_WRITE',
    partition_fields="state",
    method='upsert',
    index_type='BLOOM',
    enable_partition=True,
    enable_cleaner=False,
    enable_hive_sync=True,
    enable_clustering='False',
    clustering_column='default',
    enable_meta_data_indexing='false',
    use_sql_transformer=False,
    sql_transformer_query='default',
    target_path=f"s3://{BUCKET}/bronze/table_name=customers/",
    spark_df=customers_df,
)


# =================================================================
# ORDERS
#============================================================

print("CUSTOMER complete ")
upsert_hudi_table(
    glue_database="testdb",
    table_name="orders",
    record_id="order_id",
    precomb_key="order_id",
    table_type='COPY_ON_WRITE',
    partition_fields="order_date",
    method='upsert',
    index_type='BLOOM',
    enable_partition=True,
    enable_cleaner=False,
    enable_hive_sync=True,
    enable_clustering='False',
    clustering_column='default',
    enable_meta_data_indexing='false',
    use_sql_transformer=False,
    sql_transformer_query='default',
    target_path=f"s3://{BUCKET}/bronze/table_name=orders/",
    spark_df=orders_df,
)

print("ORDER complete ")
```

# Step 2: upload the Jar files into S3 into folder jar/


* commander-1.78.jar
* hudi-spark3.3-bundle_2.12-0.14.0.jar
* hudi-utilities-slim-bundle_2.12-0.14.0.jar
![1710599139513](https://github.com/soumilshah1995/DeltaHudiTransformations/assets/39345855/ec2ea0d1-240b-4971-a24c-dec2313d25f5)


Link : https://drive.google.com/drive/folders/1Rs9243i-D-jmFHPivlwdtODBRQpG1nys?usp=share_link

# Step 3: Delta Streamer incremental ETL code 
```
import com.amazonaws.services.glue.GlueContext
import com.amazonaws.services.glue.MappingSpec
import com.amazonaws.services.glue.errors.CallSite
import com.amazonaws.services.glue.util.GlueArgParser
import com.amazonaws.services.glue.util.Job
import com.amazonaws.services.glue.util.JsonOptions
import org.apache.spark.SparkContext
import scala.collection.JavaConverters._
import org.apache.spark.sql.SparkSession
import org.apache.spark.api.java.JavaSparkContext
import org.apache.hudi.utilities.streamer.HoodieStreamer
import org.apache.hudi.utilities.streamer.SchedulerConfGenerator
import org.apache.hudi.utilities.UtilHelpers
import com.beust.jcommander.JCommander
import com.beust.jcommander.Parameter

object GlueApp {

  def main(sysArgs: Array[String]) {
    val args = GlueArgParser.getResolvedOptions(sysArgs, Seq("JOB_NAME").toArray)
    println(s"GLue Args $args")

    val BUCKET = "XX"
    val SqlQuery = "SELECT a.order_id, a.customer_id, a.product_id, a.order_date,a.quantity,a.total_price,c.name as customer_name, c.email as customer_email FROM <SRC> a LEFT JOIN testdb.customers c ON a.customer_id = c.customer_id;"

    println(s"SQL QUERY $SqlQuery")

    println(s"Hudi DeltaStreamer Bucket $BUCKET")

    var config = Array(
      "--source-class", "org.apache.hudi.utilities.sources.HoodieIncrSource",
      "--source-ordering-field", "order_date",
      s"--target-base-path", s"s3://$BUCKET/silver/",
      "--target-table", "orders",
      "--table-type", "COPY_ON_WRITE",
      "--transformer-class", "org.apache.hudi.utilities.transform.SqlQueryBasedTransformer",
      "--hoodie-conf", s"hoodie.deltastreamer.transformer.sql=$SqlQuery",
      s"--hoodie-conf", s"hoodie.streamer.source.hoodieincr.path=s3://$BUCKET/bronze/table_name=orders/",
      "--hoodie-conf", "hoodie.streamer.source.hoodieincr.missing.checkpoint.strategy=READ_UPTO_LATEST_COMMIT",
      "--hoodie-conf", "hoodie.datasource.write.recordkey.field=order_id",
      "--hoodie-conf", "hoodie.datasource.write.precombine.field=order_date",
      "--hoodie-conf", "hoodie.datasource.write.partitionpath.field=order_date",

    )

    val cfg = HoodieStreamer.getConfig(config)
    val additionalSparkConfigs = SchedulerConfGenerator.getSparkSchedulingConfigs(cfg)
    val jssc = UtilHelpers.buildSparkContext("delta-streamer-test", "jes", additionalSparkConfigs)
    val spark = jssc.sc

    val glueContext: GlueContext = new GlueContext(spark)
    Job.init(args("JOB_NAME"), glueContext, args.asJava)

    try {
      new HoodieStreamer(cfg, jssc).sync()
    } finally {
      jssc.stop()
    }

    Job.commit()
  }
}


```
# Backfilling jobs

![1710849924713](https://github.com/soumilshah1995/DeltaHudiTransformations/assets/39345855/8b25c6da-16eb-4e7e-aa41-c5a84be04856)

```
import com.amazonaws.services.glue.GlueContext
import com.amazonaws.services.glue.MappingSpec
import com.amazonaws.services.glue.errors.CallSite
import com.amazonaws.services.glue.util.GlueArgParser
import com.amazonaws.services.glue.util.Job
import com.amazonaws.services.glue.util.JsonOptions
import org.apache.spark.SparkContext
import scala.collection.JavaConverters._
import org.apache.spark.sql.SparkSession
import org.apache.spark.api.java.JavaSparkContext
import org.apache.hudi.utilities.streamer.HoodieStreamer
import org.apache.hudi.utilities.streamer.SchedulerConfGenerator
import org.apache.hudi.utilities.UtilHelpers
import com.beust.jcommander.JCommander
import com.beust.jcommander.Parameter

object GlueApp {

  def main(sysArgs: Array[String]) {
    val args = GlueArgParser.getResolvedOptions(sysArgs, Seq("JOB_NAME").toArray)
    println(s"GLue Args $args")

    val BUCKET = "XX"
    val SqlQuery = "SELECT a.order_id, a.customer_id, a.product_id, a.order_date, a.quantity, a.total_price, 'test' AS customer_name, c.email AS customer_email FROM testdb.orders a LEFT JOIN testdb.customers c ON a.customer_id = c.customer_id WHERE a.order_date='2022-01-03' "

    println(s"SQL QUERY $SqlQuery")

    println(s"Hudi DeltaStreamer Bucket $BUCKET")

    var config = Array(
      "--source-class", "org.apache.hudi.utilities.sources.SqlSource",
      "--source-ordering-field", "order_date",
      s"--target-base-path", s"s3://$BUCKET/silver/",
      "--target-table", "orders",
      "--table-type", "COPY_ON_WRITE",
      "--hoodie-conf", s"hoodie.deltastreamer.source.sql.sql.query=$SqlQuery",
      "--hoodie-conf", "hoodie.datasource.write.recordkey.field=order_id",
      "--hoodie-conf", "hoodie.datasource.write.precombine.field=order_date",
      "--hoodie-conf", "hoodie.datasource.write.partitionpath.field=order_date"
    )

    val cfg = HoodieStreamer.getConfig(config)
    val additionalSparkConfigs = SchedulerConfGenerator.getSparkSchedulingConfigs(cfg)
    val jssc = UtilHelpers.buildSparkContext("delta-streamer-test", "jes", additionalSparkConfigs)
    val spark = jssc.sc

    val glueContext: GlueContext = new GlueContext(spark)
    Job.init(args("JOB_NAME"), glueContext, args.asJava)

    try {
      new HoodieStreamer(cfg, jssc).sync()
    } finally {
      jssc.stop()
    }

    Job.commit()
  }
}

```


##### Make sure to add jar path 

![image](https://github.com/soumilshah1995/DeltaHudiTransformations/assets/39345855/69492139-c034-4a07-8c88-8827ac5fb0c2)

# Run the job 



