from pyspark.sql import SparkSession
from pyspark.sql.functions import *

spark = SparkSession.builder.appName("streaming_word_count").getOrCreate()

lines_df = spark.readStream.format("socket").option("host", "localhost").option("port", "9999").load()
word_df = lines_df.select(expr("explode(split(value, ' ')) as word"))
count_df = word_df.groupBy("word").count()

word_count_qs = count_df.writeStream.format("console")\
                                    .outputMode("complete")\
                                    .option("checkpointLocation", ".checkpoint")\
                                    .start()    
word_count_qs.awaitTermination()