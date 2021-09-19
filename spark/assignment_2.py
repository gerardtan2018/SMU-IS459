import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import monotonically_increasing_id
from pyspark.sql import functions as F
from graphframes import *

import re
from nltk.corpus import stopwords

# Q3 Additional Imports
from pyspark.sql.types import StringType
from pyspark.sql.functions import udf, datediff, to_date, lit, lag, col
from pyspark.sql.window import Window

spark = SparkSession.builder.appName('sg.edu.smu.is459.assignment2').getOrCreate()

# Load data
posts_df = spark.read.load('/gerard_tan/spark/hardwarezone.parquet')

# Find distinct users
#distinct_author = spark.sql("SELECT DISTINCT author FROM posts")
author_df = posts_df.select('author').distinct()

print('Author number :' + str(author_df.count()))

# Assign ID to the users
author_id = author_df.withColumn('id', monotonically_increasing_id())
author_id.show()

# Construct connection between post and author
left_df = posts_df.select('topic', 'author') \
    .withColumnRenamed("topic","ltopic") \
    .withColumnRenamed("author","src_author")

right_df =  left_df.withColumnRenamed('ltopic', 'rtopic') \
    .withColumnRenamed('src_author', 'dst_author')

#  Self join on topic to build connection between authors
author_to_author = left_df. \
    join(right_df, left_df.ltopic == right_df.rtopic) \
    .select(left_df.src_author, right_df.dst_author) \
    .distinct()
edge_num = author_to_author.count()
print('Number of edges with duplicate : ' + str(edge_num))

# Convert it into ids
id_to_author = author_to_author \
    .join(author_id, author_to_author.src_author == author_id.author) \
    .select(author_to_author.dst_author, author_id.id) \
    .withColumnRenamed('id','src')

id_to_id = id_to_author \
    .join(author_id, id_to_author.dst_author == author_id.author) \
    .select(id_to_author.src, author_id.id) \
    .withColumnRenamed('id', 'dst')

id_to_id = id_to_id.filter(id_to_id.src >= id_to_id.dst).distinct()

id_to_id.cache()

print("Number of edges without duplciate :" + str(id_to_id.count()))

# Build graph with RDDs
graph = GraphFrame(author_id, id_to_id)

# For complex graph queries, e.g., connected components, you need to set
# the checkopoint directory on HDFS, so Spark can handle failures.
# Remember to change to a valid directory in your HDFS
spark.sparkContext.setCheckpointDir('/gerard_tan/spark/spark-checkpoint')

# The rest is your work, guys
# ......

############################################################################
# Q1a) How large are the communities (connected components)?
############################################################################
print("#" * 80)
print("Q1a) How large are the communities (connected components)?")

# Get number of connected components
connected_components = graph.connectedComponents()

# Get total number of communities
num_communities = connected_components.count()
print("Number of Communities : " + str(num_communities))

# Get number of authors in each community
connected_components_group = connected_components.groupBy(["component"]).agg(F.count("id").alias("CommunitySize"))
print("How large are the communities : ")
connected_components_group.sort(F.desc("CommunitySize")).show(num_communities)

############################################################################
# Q1b) What are the keywords of the community (frequent words)?
############################################################################
print("#" * 80)
print("Q1b) What are the keywords of the community (frequent words)?")

stop_words = stopwords.words('english')
stop_words.append("-") # add "-" as a stop word to be removed

def clean_tokenize_sentence(sentence):
    """
    Function which tokenizes the sentence, remove stop words and numbers

    Args:
        sentence (str): sentence you wish to clean and tokenize

    Returns:
        [list]: list of words from the sentence passed in which has been cleaned
    """
    cleaned_sentence = re.sub("[^-9a-z ]", "" , sentence.lower()).split()
    return [w for w in cleaned_sentence if w not in stop_words]

# Merged the posts dataframe with the connected components (community) generated previously.
# The resulting dataframe will consist of the component ID and the posts content from that community
posts_renamed = posts_df.withColumnRenamed("author", "post_author")
component_topic_df = (posts_renamed
                      .join(connected_components, posts_renamed.post_author == connected_components.author, "left")
                      .select(connected_components.component, posts_renamed.content)
                      .distinct()
                     )

# Converts the dataframe to RDD to perform map and reduce functions
# First, the content will be cleaned by lowercasing it and removing the stopwords. It will then be tokenized
# Second, flatMap is used to flatten the results from the mapping of each of the word to its component
# Third, each word occurrence will be mapped to 1 
# Fourth, it is then reduced by key, which is the compoenent ID and the word, to sum up each word's number of occurrence
# Finally, it is converted to this format for better readability -> [(component_id, ('word_n', frequency_n))]
# e.g. [(0, ('fight', 741)), (0, ('class', 1151))]
component_rdd = component_topic_df.rdd
topic_word_count = (component_rdd
                    .map(lambda x: (x[0], clean_tokenize_sentence(x[1])))
                    .flatMap(lambda x: map(lambda e: (x[0], e), x[1]))
                    .map(lambda x: (x,1))
                    .reduceByKey(lambda x, y: x+y)
                    .map(lambda x: (x[0][0], (x[0][1], x[1])))
                   )

# Sort the RDD we get from above from the highest to lowest word occurrence and group by the component ID
# Output e.g. [(1039382085635, [('idndidnxisnsk', 1), ('wrong', 1), ('thread', 1)]), (0, [('game', 201)])]
topic_word_count_list = (topic_word_count
    .sortBy(lambda x: x[1][1], ascending=False)
    .groupByKey()
    .map(lambda x : (x[0], list(x[1]))).collect()
)

# Retrieve the top n most frequent words in each community
top_n = 5

# Generate the column names for the new dataframe, depending on number of top words
column_names = ["community"]
for i in range(1,top_n + 1):
    word_col = "(keyword_" + str(i) + ", freq_" + str(i) + ")"
    column_names.append(word_col)

# Creates the new row for the dataframe to display
df_rows = []
for row in topic_word_count_list:
    df_row = [row[0]]
    df_row.extend([None] * top_n)
    freq_words = row[1][:top_n]
    df_row[1:len(freq_words)+1] = freq_words
    df_rows.append(df_row)    
    
keyword_df = spark.createDataFrame(df_rows).toDF(*column_names)

print("Key words of each community. Printing the top", top_n, "key words, as well as how many times the word appears")
keyword_df.show(num_communities)


############################################################################
# Q2) How cohesive are the communities (Average # of triangles over every user in a community)?
############################################################################
print("#" * 80)
print("Q2) How cohesive are the communities (Average # of triangles over every user in a community)?")

triangle_count_per_user = graph.triangleCount()
connected_components_renamed = connected_components.withColumnRenamed("id", "cc_id")

triangle_count_component = (triangle_count_per_user
                            .join(connected_components_renamed, triangle_count_per_user.id == connected_components_renamed.cc_id)
                           )

# Get average number of triangles over every user in each community and then sorted in descending order
triangle_count_avg = (triangle_count_component
                      .groupBy("component")
                      .avg("count")
                     )

triangle_count_avg.sort(F.desc("avg(count)")).show(num_communities)

############################################################################
# Q3) Is there any strange community? (Retrieve frequency of posts within each community)
############################################################################
print("#" * 80)
print("Q3) Is there any strange community? (Retrieve frequency of posts within each community)")

# Detailed write out can be found in the word document

posts_community_df = (posts_renamed
    .join(connected_components, posts_renamed.post_author == connected_components.author, "left")
 )

convert_date_udf = udf(lambda post_date: post_date.split("+")[0].replace("T"," "), StringType())

# Convert date to suitable format for pyspark
posts_community_df = posts_community_df.withColumn("date_published", convert_date_udf(posts_community_df.date_published))

# Find average number of days between posts using datediff
posts_community_df = posts_community_df.withColumn("post_freq", datediff(posts_community_df.date_published, lag(posts_community_df.date_published, 1)
    .over(Window.partitionBy("component")
    .orderBy("date_published"))))

avg_post_freq_days_df = (posts_community_df
                         .groupBy("component")
                         .agg(F.round(F.avg("post_freq"), 5).alias("avg_post_freq_days"), 
                              F.count("post_id").alias("number_of_posts"),
                              F.countDistinct("post_author").alias("number_of_authors"),
                              F.min("date_published").alias("first_post_date"),
                              F.max("date_published").alias("last_post_date"),
                              datediff(F.max("date_published"),F.min("date_published")).alias("days_apart")
                             )
                        )
avg_post_freq_days_df = avg_post_freq_days_df.na.fill(0)
avg_post_freq_days_df.sort(F.desc("avg_post_freq_days")).show(num_communities)