import pymongo
import pandas as pd
from pymongo import MongoClient
import pyarrow as pa
import pyarrow.parquet as pq

uri = "mongodb://root:root@localhost:27017/"
client = MongoClient(uri)
db = client['hardwarezone']
posts = db.posts

# extract records from MongoDB without the id field
# post_contents = pd.DataFrame(list(posts.find({},{
#     '_id': False, 
#     'content': False, 
#     'date_published': False, 
#     'post_embedded_url': False,
#     'post_id': False, 
#     'post_images_url': False, 
#     'topic': False, 
#     'post_url': False,
#     'posts_replied_to_detail': False
# })))

post_contents = pd.DataFrame(list(posts.find({},{
    '_id': False, 
    'date_published': False, 
    'post_embedded_url': False,
    'post_id': False, 
    'post_images_url': False, 
    'post_url': False,
    'posts_replied_to_detail': False
})))


# convert to the pyarrow table
table = pa.Table.from_pandas(post_contents)
print("Column names: {}".format(table.column_names))
print("Schema: {}".format(table.schema))

# write to the parquet file
pq.write_table(table, 'hardwarezone.parquet')
