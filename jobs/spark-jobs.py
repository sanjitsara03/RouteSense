from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import StructType, StructField, StringType, TimestampType, IntegerType, DoubleType
from pyspark.sql.functions import from_json, col
from config import configuration
import os


def main():
    # Set environment variables for Hadoop authentication
    os.environ['HADOOP_USER_NAME'] = 'spark'
    os.environ['HADOOP_CONF_DIR'] = ''
    os.environ['JAVA_SECURITY_KRB5_CONF'] = ''
    
    spark = SparkSession.builder.appName("RouteSenseStreaming") \
        .config("spark.jars.packages",
                "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.5,"
                "org.apache.hadoop:hadoop-aws:3.3.4,"
                "com.amazonaws:aws-java-sdk-s3:1.11.1034,"
                "com.amazonaws:aws-java-sdk-core:1.11.1034") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .config("spark.hadoop.fs.s3a.access.key", configuration.get('AWS_ACCESS_KEY')) \
        .config("spark.hadoop.fs.s3a.secret.key", configuration.get('AWS_SECRET_KEY')) \
        .config('spark.hadoop.fs.s3a.aws.credentials.provider',
                'org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider') \
        .config("spark.hadoop.security.authentication", "simple") \
        .config("spark.hadoop.security.authorization", "false") \
        .config("spark.hadoop.security.credential.provider.path", "") \
        .config("spark.hadoop.hadoop.security.authentication", "simple") \
        .config("spark.hadoop.hadoop.security.authorization", "false") \
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "true") \
        .config("spark.hadoop.fs.s3a.path.style.access", "false") \
        .config("spark.hadoop.fs.s3a.attempts.maximum", "3") \
        .config("spark.hadoop.fs.s3a.connection.establish.timeout", "5000") \
        .config("spark.hadoop.fs.s3a.connection.timeout", "200000") \
        .config("spark.hadoop.fs.s3a.endpoint", "s3.amazonaws.com") \
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
        .config("spark.driver.extraJavaOptions", "-Djava.security.krb5.conf=") \
        .config("spark.executor.extraJavaOptions", "-Djava.security.krb5.conf=") \
        .config("spark.hadoop.hadoop.security.authentication.use_jaas", "false") \
        .getOrCreate()

    spark.sparkContext.setLogLevel('WARN')

    vehicle = StructType([
        StructField("id", StringType(), True),
        StructField("deviceId", StringType(), True),
        StructField("timestamp", TimestampType(), True),
        StructField("location", StringType(), True),
        StructField("speed", DoubleType(), True),
        StructField("direction", StringType(), True),
        StructField("make", StringType(), True),
        StructField("model", StringType(), True),
        StructField("year", IntegerType(), True),
        StructField("fuelType", StringType(), True),
    ])


    gps = StructType([
        StructField("id", StringType(), True),
        StructField("deviceId", StringType(), True),
        StructField("timestamp", TimestampType(), True),
        StructField("speed", DoubleType(), True),
        StructField("direction", StringType(), True),
        StructField("vehicleType", StringType(), True)
    ])

    traffic = StructType([
        StructField("id", StringType(), True),
        StructField("deviceId", StringType(), True),
        StructField("cameraId", StringType(), True),
        StructField("location", StringType(), True),
        StructField("timestamp", TimestampType(), True),
        StructField("snapshot", StringType(), True)
    ])

    weather = StructType([
        StructField("id", StringType(), True),
        StructField("deviceId", StringType(), True),
        StructField("location", StringType(), True),
        StructField("timestamp", TimestampType(), True),
        StructField("temperature", DoubleType(), True),
        StructField("weatherCondition", StringType(), True),
        StructField("precipitation", DoubleType(), True),
        StructField("windSpeed", DoubleType(), True),
        StructField("humidity", IntegerType(), True),
        StructField("airQualityIndex", DoubleType(), True),
    ])

    emergency = StructType([
        StructField("id", StringType(), True),
        StructField("deviceId", StringType(), True),
        StructField("incidentId", StringType(), True),
        StructField("type", StringType(), True),
        StructField("timestamp", TimestampType(), True),
        StructField("location", StringType(), True),
        StructField("status", StringType(), True),
        StructField("description", StringType(), True),
    ])

    def read_kafka_topic(topic, schema):
        return (spark.readStream
                .format('kafka')
                .option('kafka.bootstrap.servers', 'kafka:9092')
                .option('subscribe', topic)
                .option('startingOffsets', 'earliest')
                .load()
                .selectExpr('CAST(value AS STRING)')
                .select(from_json(col('value'), schema).alias('data'))
                .select('data.*')
                .withWatermark('timestamp', '2 minutes')
                )
    def streamWriter(input: DataFrame, checkpointFolder, output):
        return (input.writeStream
                .format('parquet')
                .option('checkpointLocation', checkpointFolder)
                .option('path', output)
                .outputMode('append')
                .start())

    vehicleDF = read_kafka_topic('vehicle_data', vehicle).alias('vehicle')
    gpsDF = read_kafka_topic('gps_data', gps).alias('gps')
    trafficDF = read_kafka_topic('traffic_data', traffic).alias('traffic')
    weatherDF = read_kafka_topic('weather_data', weather).alias('weather')
    emergencyDF = read_kafka_topic('emergency_data', emergency).alias('emergency')

    query1 = streamWriter(vehicleDF, 's3a://rs-kafka-streaming/checkpoints/vehicle_data',
                          's3a://rs-kafka-streaming/data/vehicle_data')
    query2 = streamWriter(gpsDF, 's3a://rs-kafka-streaming/checkpoints/gps_data',
                          's3a://rs-kafka-streaming/data/gps_data')
    query3 = streamWriter(trafficDF, 's3a://rs-kafka-streaming/checkpoints/traffic_data',
                          's3a://rs-kafka-streaming/data/traffic_data')
    query4 = streamWriter(weatherDF, 's3a://rs-kafka-streaming/checkpoints/weather_data',
                          's3a://rs-kafka-streaming/data/weather_data')
    query5 = streamWriter(emergencyDF, 's3a://rs-kafka-streaming/checkpoints/emergency_data',
                          's3a://rs-kafka-streaming/data/emergency_data')

    query5.awaitTermination()


if __name__ == "__main__":
    main()
