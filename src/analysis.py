from pyspark.sql import SparkSession
from pyspark.sql.functions import split, min, max, col, broadcast, when
from pyspark.sql.types import IntegerType, StringType

# Cria uma sessão Spark
spark = SparkSession.builder \
    .appName("Music_Recommender_System") \
    .getOrCreate()

spark.sparkContext.setLogLevel('WARN')


raw_user_artist_path = "data/user_artist_data.txt" 
raw_user_artist_data = spark.read.text(raw_user_artist_path)
raw_user_artist_data.show(5)

raw_artist_data = spark.read.text("data/artist_data.txt")
raw_artist_data.show(5)

raw_artist_alias = spark.read.text("data/artist_alias.txt")
raw_artist_alias.show(5)


# Preparing the Data

#split lines by space characters

user_artist_df = raw_user_artist_data.withColumn('user', 
                                                 split(raw_user_artist_data['value'], ' ').\
                                                 getItem(0).cast(IntegerType()))

user_artist_df = user_artist_df.withColumn('artist', 
                                           split(raw_user_artist_data['value'], ' ').\
                                            getItem(1).cast(IntegerType()))                                                 

user_artist_df = user_artist_df.withColumn('count', 
                                           split(raw_user_artist_data['value'], ' ').
                                           getItem(2).cast(IntegerType())).drop('value')                                            

user_artist_df.show(5)
user_artist_df.select([min("user"), max("user"), min("artist"),max("artist")]).show()

artist_by_id = raw_artist_data.withColumn('id', split(col('value'), '\s+', 2).
                                          getItem(0).cast(IntegerType())) 

artist_by_id = artist_by_id.withColumn('name', split(col('value'), '\s+', 2).\
                                       getItem(1).cast(StringType())).drop('value') 
artist_by_id.show(5)


artist_alias = raw_artist_alias.withColumn('artist', split(col('value'), '\s+').\
                                           getItem(0).cast(IntegerType())).\
                                withColumn('alias', split(col('value'), '\s+').\
                                           getItem(1).cast(StringType())).\
                                drop('value')  
artist_alias.show(5)

print('\n Same name, different ID')
artist_by_id.filter(artist_by_id.id.isin(1092764, 1000311)).show()

# Building a First Model
train_data = user_artist_df.join(broadcast(artist_alias), 'artist', how='left')
# Get artist’s alias if it exists; otherwise, get original artist
train_data = train_data.withColumn('artist', when(col('alias').isNull(), col('artist')).otherwise(col('alias')))
train_data = train_data.withColumn('artist', col('artist').cast(IntegerType())).drop('alias')
train_data.cache()
print(train_data.count())





spark.stop()