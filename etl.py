import configparser
import os
import pyspark.sql.functions as F

from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col, to_timestamp, monotonically_increasing_id
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format

config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID']=config['AWS']['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY']=config['AWS']['AWS_SECRET_ACCESS_KEY']


def create_spark_session():
    spark = SparkSession.builder \
                        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
                        .getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):
    """
    **Description**
    Loads song_data from S3 and processes it by extracting unique songs and artists
    and loading these back into S3 as songs_table and artists_table.
        
    **Parameters**
    • spark: Spark Session
    • input_data: location of song_data JSON files with the songs metadata
    • output_data: S3 bucket where dimension tables in parquet format will be stored
    """
    # get filepath to song data file
    song_data = os.path.join(input_data, "song-data/A/A/A/*.json")
    
    # read song data file
    print("Reading song data.")
    df = spark.read.json(song_data)

    # extract columns to create songs table
    songs_table = df['song_id', 'title', 'artist_id', 'year', 'duration']
    songs_table = songs_table.dropDuplicates(['song_id'])
    
    # write songs table to parquet files partitioned by year and artist
    print("Writing songs_table.")
    songs_table.write.partitionBy('year', 'artist_id') \
                     .parquet(os.path.join(output_data, 'songs.parquet'), 'overwrite')

    # extract columns to create artists table
    artists_table = df['artist_id', 'artist_name', 'artist_location', \
                       'artist_latitude', 'artist_longitude']
    artists_table = artists_table.dropDuplicates(['artist_id'])
    
    # write artists table to parquet files
    print("Writing artists_table")
    artists_table.write.parquet(os.path.join(output_data, 'artists.parquet'), 'overwrite')


def process_log_data(spark, input_data, output_data):
    """
    **Description**
    This function loads log_data from S3 then extracts songs and artists tables and loads them back to S3.
    The output from process_song_data() is read into song_df in this function.
        
    **Parameters**
    • spark: Spark Session
    • input_data: location of song_data JSON files with the songs metadata
    • output_data: S3 bucket where dimension tables in parquet format will be stored
    """
    # get filepath to log data file
    log_data = os.path.join(input_data,"log_data/*/*/*.json")

    # read log data file
    print("Reading log file.")
    df = spark.read.json(log_data)
    
    # filter by actions for song plays
    songplays_table = df['ts', 'userId', 'level','sessionId', 'location', 'userAgent']

    # extract columns for users table    
    users_table = df['userId', 'firstName', 'lastName', 'gender', 'level']
    users_table = users_table.dropDuplicates(['userId'])
    
    # write users table to parquet files
    users_table.write.parquet(os.path.join(output_data, 'users.parquet'), 'overwrite')
    print("users.parquet completed")
    
    # create datetime column from original timestamp column
    get_datetime = udf(lambda x: str(datetime.fromtimestamp(int(x) / 1000.0)))
    df = df.withColumn("datetime", get_datetime(df.ts))
    
    # extract columns to create time table
    time_table = df.select(
                           col('datetime').alias('start_time'),
                           hour('datetime').alias('hour'),
                           dayofmonth('datetime').alias('day'),
                           weekofyear('datetime').alias('week'),
                           month('datetime').alias('month'),
                           year('datetime').alias('year') 
                           )
    time_table = time_table.dropDuplicates(['start_time'])
    
    # write time table to parquet files partitioned by year and month
    time_table.write.partitionBy('year', 'month').parquet(os.path.join(output_data, 'time.parquet'), 'overwrite')
    print("time.parquet completed")

    # read in song data to use for songplays table
    song_data = os.path.join(input_data, "song-data/A/A/A/*.json")
    song_df = spark.read.json(song_data)

    # extract columns from joined song and log datasets to create songplays table 
    df = df.join(song_df, song_df.title == df.song)
    
    # create songplays_table
    songplays_table = df.select(
                                col('artist_id').alias('artist_id'),                        
                                col('userId').alias('user_id'),
                                col('level').alias('level'),
                                col('song_id').alias('song_id'),
                                col('datetime').alias('start_time'),                        
                                col('sessionId').alias('session_id'),
                                col('location').alias('location'),
                                col('userAgent').alias('user_agent'),
                                month('datetime').alias('month'),
                                year('datetime').alias('year') 
                                )

    songplays_table.select(monotonically_increasing_id().alias('songplay_id')).collect()
    
    # write songplays table to parquet files partitioned by year and month
    songplays_table.write.partitionBy('year', 'month') \
                         .parquet(os.path.join(output_data, 'songplays.parquet'), 'overwrite')
    print("songplays.parquet completed")
    print("process_log_data completed")


def main():
    """
    **Description**
    Extract songs and events data from S3, Transform it into dimensional tables format, and Load it back to S3 in Parquet format
    """
    spark = create_spark_session()
    input_data = "s3a://udacity-dend/"
    output_data = "s3a://june-dend/"
    
    process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()