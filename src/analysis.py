from pyspark.sql import SparkSession
from pyspark.sql.functions import split, min, max, col
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

user_artist_df.select([min("user"), max("user"), min("artist"),max("artist")]).show()

artist_by_id = raw_artist_data.withColumn('id', split(col('value'), '\s+', 2).
                                          getItem(0).cast(IntegerType())) 

artist_by_id = artist_by_id.withColumn('name', split(col('value'), '\s+', 2).\
                                       getItem(1).cast(StringType())).drop('value') 
artist_by_id.show(5)

spark.stop()