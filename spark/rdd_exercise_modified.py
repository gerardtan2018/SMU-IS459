import pyspark
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName('RDD Exercise').getOrCreate()

# Q1
# Load CSV file into a data frame
score_sheet_df = spark.read.load('/gerard_tan/spark/quiz.csv', \
    format='csv', sep=';', inferSchema='true', header='true')

# Get RDD from the data frame
score_sheet_rdd = score_sheet_df.rdd

# Project the second column of scores with an additional 1
score_rdd = score_sheet_rdd.map(lambda x: (x[1], 1))
highest_score = score_rdd.max()
lowest_score = score_rdd.min()

# Filter out rows which has the highest and lowest score
score_rdd = score_rdd.filter(lambda x: x != highest_score and x != lowest_score)

# Get the sum and count by reduce
(sum, count) = score_rdd.reduce(lambda x, y: (x[0] + y[0], x[1] + y[1]))
print('Average Score : ' + str(sum/count))


# Q2
# Load Parquet file into a data frame
# posts_df = spark.read.load('/gerard_tan/active_authors/parquet-input/hardwarezone.parquet')
posts_df = spark.read.load('/gerard_tan/spark/quizq2.csv', \
    format='csv', sep=';', inferSchema='true', header='true')

posts_rdd = posts_df.rdd

# Project the author's name and number of words in each post
posts_rdd = (posts_rdd
    .map(lambda x: (x[1], (len(x[2].split(" ")), 1)))
    .reduceByKey(lambda x,y : (x[0] + y[0], x[1] + y[1]))
    .map(lambda x: (x[0], x[1][0]/x[1][1]))
)

# Print out the rows in the rdd
dataColl = posts_rdd.collect()
for row in dataColl:
    print(row)



'''
# Project the author and content columns
author_content_rdd = posts_rdd.map(lambda x: (len(x[2]), 1))
author_content_rdd.first()

# Get sume and count by reduce
(sum, count) = author_content_rdd.reduce(lambda x,y: (x[0]+y[0], x[1]+y[1]))
print('Average post length : ' + str(sum/count))
'''

