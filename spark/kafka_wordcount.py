from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import explode
from pyspark.sql.functions import split, window, current_timestamp, regexp_replace, desc, lower, col, length, trim
from pyspark.sql.window import Window

from nltk.corpus import stopwords
from kafka import KafkaProducer
import re
import json

def parse_data_from_kafka_message(sdf, schema):
    from pyspark.sql.functions import split, current_timestamp
    assert sdf.isStreaming == True, "DataFrame doesn't receive streaming data"
    col = split(sdf['value'], ',')

    # split attributes to nested array in one Column
    # now expand col to multiple top-level columns
    for idx, field in enumerate(schema):
        col_name = field.name
        split_var = f'"{col_name}":'
        val = split(col.getItem(idx).cast(field.dataType), split_var).getItem(1)
        val = regexp_replace(trim(val), '["\{\}]', '')
        sdf = sdf.withColumn(col_name, val)

    return sdf

def write_mongo_row(df, epoch_id):
    mongoURL = "mongodb://root:root@127.0.0.1:27017/post_count"
    df.write.format("mongo").mode("append").option("uri",mongoURL).option("collection", "post_counts").save()
    pass

if __name__ == "__main__":

    spark = SparkSession.builder \
               .appName("KafkaWordCount") \
               .getOrCreate()

    #Read from Kafka's topic scrapy-output
    df = spark.readStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", "localhost:9092") \
            .option("subscribe", "scrapy-output") \
            .option("startingOffsets", "earliest") \
            .load()

    #Parse the fields in the value column of the message
    lines = df.selectExpr("CAST(value AS STRING)", "timestamp")

    # ===========================================================================================================================
    # Preprocessing of Data
    # ===========================================================================================================================
    """
    In this step, I have modified parse_data_from_kafka_message to extract out the value from each field 
    and put them in the dataframe column.
    """
    # Specify the schema of the fields
    hardwarezoneSchema = StructType([ \
        StructField("topic", StringType()), \
        StructField("author", StringType()), \
        StructField("content", StringType()), 
        ])

    # Use the function to parse the fields
    lines = parse_data_from_kafka_message(lines, hardwarezoneSchema) \
        .select("topic","author","content","timestamp")

    # Remove rows where author or content is null
    lines = lines.filter(~lines.author.contains("null"))
    lines = lines.filter(~lines.content.contains("null"))


    # ===========================================================================================================================
    # Question 1
    # ===========================================================================================================================

    # Group by the window and the author
    author_count = (
        lines
        .groupBy(
            window(lines.timestamp, "2 minutes", "1 minute"), 
            "author"
        )
        .count()
    )

    # Split the window into 2 separate columns, window_start and window_end, and dump the window column
    author_count = author_count.select(
        col('window')['start'].alias('window_start'), 
        col('window')['end'].alias('window_end'), 
        col('author'), 
        col('count')
    ).drop('window')
    
    # I have added the current_timestamp to each batch of streaming dataframe 
    # which I will be using to filter only rows where the current timestamp is after the end of a window. 
    # This would mean that the results which shows the top 10 most frequent authors will all be accounted for. 
    author_count = author_count.withColumn("current_timestamp", current_timestamp())
    author_count = (
        author_count
        .filter(col('window_end') <= col('current_timestamp'))
        .sort(col('window_start').desc(), col('count').desc())
        .limit(10)
    )
        
    # ==================================================================================
    # Assignment 4: Send author counts to message queue in Kafka
    # ==================================================================================
    dashboard_df = author_count.select(['current_timestamp', 'author', 'count'])
    author_count_content =  dashboard_df \
        .selectExpr("CAST(current_timestamp as STRING) AS key", "to_json(struct(*)) AS value") \
        .writeStream \
        .outputMode("complete") \
        .trigger(processingTime='1 minute') \
        .format("kafka") \
        .option("checkpointLocation", "/gerard_tan/spark/stream/checkpoint/author_count") \
        .option("kafka.bootstrap.servers", "localhost:9092") \
        .option("topic", "dashboard_author") \
        .start()
    
    author_count_content.awaitTermination()

    # ==================================================================================
    # Assignment 3 Question 1
    # ==================================================================================
    # Write to console stream for Top 10 Author counts
    # author_count_content =  author_count\
    #     .writeStream \
    #     .queryName("author_count_content") \
    #     .outputMode("complete") \
    #     .trigger(processingTime='1 minute') \
    #     .format("console") \
    #     .option('truncate', 'false') \
    #     .option("checkpointLocation", "/gerard_tan/spark/stream/checkpoint/author_count") \
    #     .start()

    
    # ===========================================================================================================================
    # Assignment 3: Question 2
    # ===========================================================================================================================

    # # Initialise stop words to be removed
    # stop_words = stopwords.words('english')
    # stop_words.append("-") 

    # # Split the content into words on each row
    # words = (
    #     lines
    #     .select(
    #         explode(
    #             split(lines.content, " ")
    #         ).alias("word"),
    #         "timestamp"
    #     )
    # )
    
    # # Filter out words which are only 1 character long, convert all the words to lowercase
    # # and remove stop words
    # words = words.filter(words.word != '')
    # words = words.filter(length(words.word) > 1)
    # words = words.withColumn("lowercase_word", lower(words.word))
    # words = words.filter(~words.lowercase_word.isin(stop_words))

    # # Group by the window and each word
    # word_count = (
    #     words
    #     .groupBy(
    #         window(words.timestamp, "2 minutes", "1 minute"), 
    #         "lowercase_word"
    #     )
    #     .count()
    # )

    # # Split the window into 2 separate columns, window_start and window_end, and dump the window column
    # word_count = word_count.select(
    #     col('window')['start'].alias('window_start'), 
    #     col('window')['end'].alias('window_end'), 
    #     col('lowercase_word'), 
    #     col('count')
    # ).drop('window')
    
    # # I have added the current_timestamp to each batch of streaming dataframe 
    # # which I will be using to filter only rows where the current timestamp is after the end of a window. 
    # # This would mean that the results which shows the top 10 words in that window period will all be accounted for.
    # word_count = word_count.withColumn("current_timestamp", current_timestamp())
    # word_count = (
    #     word_count
    #     .filter(col('window_end') <= col('current_timestamp'))
    #     .sort(col('window_start').desc(), col('count').desc())
    #     .limit(10)
    # )

    # # Write to console stream for Top 10 Word counts
    # word_count_content =  word_count\
    #     .writeStream \
    #     .queryName("word_count_content") \
    #     .outputMode("complete") \
    #     .trigger(processingTime='1 minute') \
    #     .option('truncate', 'false') \
    #     .option("checkpointLocation", "/gerard_tan/spark/stream/checkpoint/word_count") \
    #     .format("console") \
    #     .start()

    # word_count_content.awaitTermination()
    # ==================================================================================
    # Start job for both write streams and wait for incoming messages
    # ==================================================================================
    
    
    