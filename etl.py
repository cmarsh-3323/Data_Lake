import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col, monotonically_increasing_id
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType


config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID']=config['AWS']['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY']=config['AWS']['AWS_SECRET_ACCESS_KEY']


def create_spark_session():
    """
    Creating a Spark Session.

    Returns:
        SparkSession: Configures a Spark Session instance.
    """
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):
    """
    Processes the song data from S3 and creates the songs table and artists table.

    Args:
        spark (SparkSession): Spark Session instance.
        input_data (str): Path to S3 bucket that contains the song data files.
        output_data (str): Path to S3 to store output parquet files.
    """
    # get filepath to song data file 
    song_data = input_data + 'song_data/*/*/*/*.json'
    
    # read song data file 
    df = spark.read.json(song_data)

    # extract columns to create songs table
    songs_table = df.select(
    StructField("song_id", StringType(), False).alias("song_id"),
    StructField("title", StringType(), True).alias("title"),
    StructField("artist_id", StringType(), False).alias("artist_id"),
    StructField("year", IntegerType(), True).alias("year"),
    StructField("duration", DoubleType(), True).alias("duration")
    )
    
    # write songs table to parquet files partitioned by year and artist
    songs_output_path = output_data + 'songs.parquet'
    songs_table.write.partitionBy("year", "artist_id").parquet(songs_output_path, 'overwrite')

    # extract columns to create artists table
    artists_table = df.select(
    StructField("artist_id", StringType(), False).alias("artist_id"),
    StructField("artist_name", StringType(), True).alias("artist_name"),
    StructField("artist_location", StringType(), True).alias("location"),
    StructField("artist_latitude", DoubleType(), True).alias("latitude"),
    StructField("artist_longitude", DoubleType(), True).alias("longitude")
    ).distinct()
    
    # write artists table to parquet files
    artists_output_path = output_data + 'artists.parquet'
    artists_table.write.parquet(artists_output_path, 'overwrite')


def process_log_data(spark, input_data, output_data):
    """
    Processes the log data from S3 and creates the users, time, and songplays tables.

    Args:
        spark (SparkSession): Spark Session instance.
        input_data (str): Path to S3 bucket that contains the log data files.
        output_data (str): Path to S3 to store output parquet files.
    """
    # get filepath to log data file 
    log_data = input + 'log_data/*/*/*.json'

    # read log data file
    df = spark.read.json(log_data)
    
    # filter by actions for song plays
    songplays_df = df.filter("page = 'NextSong'")

    # extract columns for users table    
    users_table = songplays_df.select(
        StructField("userId", IntegerType(), True).alias("user_id"),
        StructField("firstName", StringType(), True).alias("first_name"),
        StructField("lastName", StringType(), True).alias("last_name"),
        StructField("gender", StringType(), True).alias("gender"),
        StructField("level", StringType(), True).alias("level")
    ).distinct()
        
    # write users table to parquet files
    users_output_path = output_data + 'users.parquet'
    users_table.write.parquet(users_output_path, 'overwrite')

    # create timestamp column from original timestamp column
    get_timestamp = udf(lambda x: str(int(int(x)/1000)))
    timestamp_df = songplays_df.withColumn('timestamp', get_timestamp(songplays_df.ts))
    
    # create datetime column from original timestamp column
    get_datetime = udf(lambda x: str(datetime.fromtimestamp(int(x) / 1000)))
    datetime_df = timestamp_df.withColumn('datetime', get_datetime(timestamp_df.ts))
    
    # extract columns to create time table
    time_table = datetime_df.select(
        df.timestamp.alias("start_time"),
        hour(df.datetime).alias("hour"),
        dayofmonth(df.datetime).alias("day"),
        weekofyear(df.datetime).alias("week"),
        month(df.datetime).alias("month"),
        year(df.datetime).alias("year"),
        dayofweek(df.datetime).alias("weekday")
    ).distinct()
    
    # write time table to parquet files partitioned by year and month
    time_output_path = output_data + 'time.parquet'
    time_table.write.partitionBy("year", "month").parquet(time_output_path, 'overwrite')
    
    # read in song data to use for songplays table
    song_df = spark.read.json(input_data + 'song_data/*/*/*/*.json')
    log_df = datetime_df #for clarity
    
    # aliasing dataframes for songplays_table
    s = song_df
    l = log_df
    
    # extract columns from joined song and log datasets to create songplays table
    songplays_table = l.join(
        s,
        (l.artist == s.artist_name) &
        (l.song == s.title),
        'left'
    ).select(
        monotonically_increasing_id().alias("songplay_id"),
        l.timestamp.alias("start_time"),
        l.userId.alias("user_id"),
        l.level,
        s.song_id,
        s.artist_id,
        l.sessionId.alias("session_id"),
        l.location,
        l.userAgent.alias("user_agent")
    )

    # write songplays table to parquet files partitioned by year and month
    songplays_output_path = output_data + 'songplays.parquet'
    songplays_table.write.partitionBy("year", "month").parquet(songplays_output_path, 'overwrite')


def main():
    spark = create_spark_session()
    input_data = "s3a://udacity-dend/"
    output_data = "s3a://udacity-cm-output-data/"
    
    process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()
