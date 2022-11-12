import sys
from pyspark.sql import SparkSession
from pyspark.sql.types import *
import pandas as pd 

KAFKA_TOPIC = "window-example"

def parse_data_from_kafka_message(df, schema):
    """ take a Spark Streaming df and parse value col based on <schema>, return streaming df cols in schema """
    from pyspark.sql.functions import split
    assert df.isStreaming == True, "DataFrame doesn't receive streaming data"


    #split attributes to nested array in one Column
    col = split(df['value'], ',') 

    # expand col to multiple top-level columns
    for idx, field in enumerate(schema): 
        df = df.withColumn(field.name, col.getItem(idx).cast(field.dataType))
    return df.select([field.name for field in schema])


if __name__ == "__main__":
 
    spark = SparkSession.builder.appName(sys.argv[0])\
            .config("spark.eventLog.enabled", "true")\
            .config("spark.eventLog.dir", "file:///opt/workspace/events")\
            .getOrCreate()

    # Set log-level to WARN to avoid very verbose output
    spark.sparkContext.setLogLevel('WARN')

    # schema for parsing value string passed from Kafka
    testSchema = StructType([ \
            StructField("test_key", StringType()), \
            StructField("test_value", FloatType())])

    df2 = 3
    pandasDF = pd.DataFrame([[df2]], columns = ['value']) 
    #pandasDF = pd.DataFrame(df2, columns = ['Name', 'Age']) 
    print(pandasDF)
    print("makingspark df")
    sparkDF=spark.createDataFrame(pandasDF) 
    print("printing schema")
    sparkDF.printSchema()
    print("showing")
    sparkDF.show()
    
    print("to kafka...!")
    
    sparkDF.selectExpr("to_json(struct(*)) AS value") \
    .write \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "broker:29092") \
    .option("topic", "nicknick") \
    .save()
    print("Done")
    spark.stop()
    
    
