from pyspark.sql import SparkSession
from pyspark.sql.avro.functions import to_avro
from pyspark.sql.functions import expr, col, from_json, to_json, struct
from pyspark.sql.types import StructType, StructField, StringType, LongType, IntegerType, DoubleType
from lib.logger import Log4j


KAFKA_BROKER = "localhost:9092"


if __name__ == "__main__":
    spark = SparkSession \
        .builder \
        .appName("Split Avro Stream By Value Type") \
        .master("local[3]") \
        .config("spark.streaming.stopGracefullyOnShutdown", "true") \
        .getOrCreate()

    logger = Log4j(spark)

    kafka_source_df = spark \
        .readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_BROKER) \
        .option("subscribe", "sensors") \
        .option("startingOffsets", "earliest") \
        .load()

    schema = StructType([
        StructField("SensorNumber", StringType()),
        StructField("CreatedTime", LongType()),
        StructField("Value", StringType())
    ])

    value_df = kafka_source_df.select(from_json(col("value").cast("string"), schema).alias("value"))
    sensor_data_df = value_df.select("value.SensorNumber", "value.CreatedTime", "value.Value") \
        .selectExpr("*", "CASE WHEN Value == 'True' THEN 'bool' WHEN Value == 'False' THEN 'bool' ELSE '' END as Boolean") \
        .withColumn("DoubleType", col("Value").cast(DoubleType())) \
        .withColumn("IntegerType", col("Value").cast(IntegerType())) \
        .selectExpr("*", "(DoubleType - IntegerType) as Diff") \
        .selectExpr("*", "CASE WHEN Diff == 0 THEN 'int' ELSE '' END AS Integer") \
        .selectExpr("*", "CASE WHEN Diff > 0 THEN 'real' ELSE '' END AS Double") \
        .selectExpr("*", "CASE WHEN Boolean <> '' THEN 'bool' WHEN Integer <> '' THEN 'int' WHEN Double <> '' THEN 'real' ELSE '' END AS ValueType") \
        .select("SensorNumber", "CreatedTime", "Value", "ValueType")

    bool_df = sensor_data_df.filter(col("ValueType") == "bool").select("SensorNumber", "CreatedTime", "Value")
    kafka_bool_df = bool_df.select(expr("SensorNumber as key"), to_avro(struct("SensorNumber", "CreatedTime", "Value")).alias("value"))

    int_df = sensor_data_df.filter(col("ValueType") == "int").select("SensorNumber", "CreatedTime", "Value")
    kafka_int_df = int_df.select(expr("SensorNumber as key"), to_avro(struct("SensorNumber", "CreatedTime", "Value")).alias("value"))

    real_df = sensor_data_df.filter(col("ValueType") == "real").select("SensorNumber", "CreatedTime", "Value")
    kafka_real_df = real_df.select(expr("SensorNumber as key"), to_avro(struct("SensorNumber", "CreatedTime", "Value")).alias("value"))

    bool_writer_query = kafka_bool_df \
        .writeStream \
        .queryName("Bool Value Writer") \
        .format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_BROKER) \
        .option("topic", "bool-sensors") \
        .outputMode("append") \
        .option("checkpointLocation", "chk-point-dir/bool") \
        .start()

    int_writer_query = kafka_int_df \
        .writeStream \
        .queryName("Integer Value Writer") \
        .format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_BROKER) \
        .option("topic", "int-sensors") \
        .outputMode("append") \
        .option("checkpointLocation", "chk-point-dir/int") \
        .start()

    real_writer_query = kafka_real_df \
        .writeStream \
        .queryName("Real Value Writer") \
        .format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_BROKER) \
        .option("topic", "real-sensors") \
        .outputMode("append") \
        .option("checkpointLocation", "chk-point-dir/real") \
        .start()

    logger.info("Waiting for Queries")
    spark.streams.awaitAnyTermination()