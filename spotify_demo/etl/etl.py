from pyspark.sql import SparkSession
from pyspark.sql import Row
from pyspark.sql.functions import col, split, udf, when, row_number, to_date, to_timestamp, trim, from_json, decode, encode, arrays_zip, explode
from pyspark.sql.window import Window
from pyspark.sql.types import ArrayType, BooleanType, DateType, DoubleType, IntegerType, StringType, StructType, StructField

import os
from sys import argv


class Args:

    def __init__(self, data_dir):
        self.tracks_160k_path = os.path.join(data_dir, "data.csv")
        self.tracks_1200k_path = os.path.join(data_dir, "tracks_features.csv")
        self.artist_genre_path = os.path.join(data_dir, "data_w_genres.csv")

    def __str__(self):
        return "Args {{\n  tracks_160k_path={},\n  tracks_1200k_path={},\n  artist_genre_path={}\n}}".format(
            self.tracks_160k_path, self.tracks_1200k_path, self.artist_genre_path)


def parse_args(argv=None):
    if "-h" in argv:
        print("usage: spark-submit etl.py <data_dir>")
        exit(0)
    # default
    data_dir = "/data/opt/msc2020/demo/demo2/Spotify"
    if len(argv) > 1:
        data_dir = argv[1]
    args = Args(data_dir)
    return args


def show_distinct(df, *column, skip=False, filter_none=False, count_only=False, limit=None):
    if skip:
        return
    distinct = df.select(*column).distinct()
    if filter_none:
        for c in column:
            distinct = distinct.where("{0} is not null".format(c))
    print("Distinct values{2} of {0}: {1}".format(','.join(column), distinct.count(), "" if filter_none else " (including none)"))
    if count_only:
        return
    if limit:
        distinct.sort(*column).show(limit)
        return
    for row in distinct.sort(*column).collect():
        for c in column[:-1]:
            print(row[c], end=",")
        print(row[column[-1]])


def build_dictionary_table(df, *column, index_column="id"):
    dictionary = df.select(*column).distinct().sort(*column)
    #for c in column:
    #    dictionary = dictionary.where("{0} is not null".format(c))
    w = Window().orderBy(*column)
    dictionary = dictionary.withColumn(index_column, row_number().over(w))
    return dictionary


def tracks_1200k_etl(args):
    print("load {0}".format(args.tracks_1200k_path))
    schema = StructType([
        StructField("id", StringType(), nullable=False),
        StructField("name", StringType()),
        StructField("album", StringType()),
        StructField("album_id", StringType()),
        StructField("artists", StringType()),
        StructField("artist_ids", StringType()),
        StructField("track_number", IntegerType()),
        StructField("disc_number", IntegerType()),
        StructField("explicit", BooleanType()),
        StructField("danceability", DoubleType()),
        StructField("energy", DoubleType()),
        StructField("key", IntegerType()),
        StructField("loudness", DoubleType()),
        StructField("mode", IntegerType()),
        StructField("speechiness", DoubleType()),
        StructField("acousticness", DoubleType()),
        StructField("instrumentalness", DoubleType()),
        StructField("liveness", DoubleType()),
        StructField("valence", DoubleType()),
        StructField("tempo", DoubleType()),
        StructField("duration_ms", IntegerType()),
        StructField("time_signature", DoubleType()),
        StructField("year", IntegerType()),
        StructField("release_date", DateType()),
        StructField("corrupted", StringType())
    ])
    tracks = spark.read.csv(
        args.tracks_1200k_path,
        enforceSchema=False,  # set enforceSchema=True with care, better set to false to check first
        header=True,
        quote='"',
        escape='"',
        schema=schema,
        columnNameOfCorruptRecord="corrupted",
        mode="PERMISSIVE")
    tracks = tracks.withColumn("mode", col("mode") == 1)

    # TODO(etl): deduplicate records with the same id
    print("corrupted records")
    tracks.cache()
    tracks.where("corrupted is not null").show(truncate=5)

    tracks = tracks.drop("corrupted")
    # preview
    tracks.show(truncate=10)
    return tracks


def tracks_160k_etl(args):
    print("load {0}".format(args.tracks_160k_path))
    schema = StructType([
        StructField("acousticness", DoubleType()),
        StructField("artists", StringType()),
        StructField("danceability", DoubleType()),
        StructField("duration_ms", IntegerType()),
        StructField("energy", DoubleType()),
        StructField("explicit", IntegerType()),
        StructField("id", StringType(), nullable=False),
        StructField("instrumentalness", DoubleType()),
        StructField("key", IntegerType()),
        StructField("liveness", DoubleType()),
        StructField("loudness", DoubleType()),
        StructField("mode", IntegerType()),
        StructField("name", StringType()),
        StructField("popularity", IntegerType()),
        StructField("release_date", DateType()),
        StructField("speechiness", DoubleType()),
        StructField("tempo", DoubleType()),
        StructField("valence", DoubleType()),
        StructField("year", IntegerType()),
        StructField("corrupted", StringType())
    ])
    tracks = spark.read.csv(
        args.tracks_160k_path,
        enforceSchema=False,  # set enforceSchema=True with care, better set to false to check first
        header=True,
        quote='"',
        escape='"',
        schema=schema,
        columnNameOfCorruptRecord="corrupted",
        mode="PERMISSIVE")
    tracks = tracks.withColumn("mode", col("mode") == 1)
    tracks = tracks.withColumn("explicit", col("explicit") == 1)

    # TODO(etl): deduplicate records with the same id
    print("corrupted records")
    tracks.cache()
    tracks.where("corrupted is not null").show(truncate=10)

    tracks = tracks.drop("corrupted")
    # preview
    tracks.show(truncate=10)
    return tracks


def merge_tracks_sources(tracks1, tracks2):
    # 1. Union overlapping columns
    overlapping_columns = [c for c in tracks1.columns if c in tracks2.columns]
    print("overlapping_columns", overlapping_columns)
    tracks1_project = tracks1.select(overlapping_columns)
    tracks2_project = tracks2.select(overlapping_columns)
    tracks = tracks1_project.union(tracks2_project)  # .distinct()
    print("tracks", tracks.count(), tracks1.count(), tracks2.count())
    tracks.show(10, truncate=10)
    show_distinct(tracks, "id", count_only=True)

    # 2. Remove duplicate tracks
    w = Window().partitionBy("id").orderBy("name")
    # some tracks appear in both data sources, but their records are not identical, and we select the first record for each id
    tracks = tracks.select('*', row_number().over(w).alias("rank")).where("rank = 1").select(overlapping_columns)

    # 4. Appending extra columns from both data sources
    tracks = tracks.join(tracks1.select('id', *[c for c in tracks1.columns if c not in overlapping_columns]), 'id', 'left')
    tracks = tracks.join(tracks2.select('id', *[c for c in tracks2.columns if c not in overlapping_columns]), 'id', 'left')
    # TODO(etl): remove duplicates due to redundant records in the right-side table of left outer join
    return tracks


def genre_etl(args):
    df = spark.read.csv(args.artist_genre_path, header=True, quote='"', escape='"').select("artists", from_json("genres", ArrayType(StringType(), containsNull=False)).alias("genre_list"), "genres")
    return df


if __name__ == "__main__":
    args = parse_args(argv)
    print(args)
    spark = SparkSession.builder.appName("ETL Demo").enableHiveSupport().getOrCreate()
    spark.sql("create database if not exists spotify_demo;")
    spark.sql("use spotify_demo;")
    """
    Notice that the following ETL flow just serves as an example and is not optimal. Also, better execution time can be achieved by changing the order of different logical steps.
    """
    # Load track data and merge
    tracks = merge_tracks_sources(tracks_160k_etl(args), tracks_1200k_etl(args))
    # preview
    # print("tracks", tracks.count())
    # tracks.show(10, truncate=10)

    # Build artist table
    track_artists = tracks.select("id", from_json("artists", ArrayType(StringType(), containsNull=False), options={"allowBackslashEscapingAnyCharacter": True}).alias("artists"), from_json("artist_ids", ArrayType(StringType(), containsNull=False)).alias("artist_ids"))\
        .select("id", explode(arrays_zip("artists", "artist_ids")).alias("tuple"))\
        .select("id", col("tuple.artists").alias("a_name"), col("tuple.artist_ids").alias("a_spotify_id"))
    artists =  build_dictionary_table(track_artists, "a_spotify_id", "a_name", index_column="a_id")
    artist_genres = genre_etl(args)
    artists = artists.join(artist_genres, artists.a_name==artist_genres.artists, 'left').select("a_id", "a_spotify_id", "a_name", col("genre_list").alias("genres"))
    # preview
    print("artists", artists.count())
    artists.show()
    artists.write.saveAsTable("artist", format="orc", mode="error") # write_mode="ignore"

    track_artists = track_artists.join(artists, (track_artists.a_spotify_id == artists.a_spotify_id) & (track_artists.a_name == artists.a_name), 'left').select(col("id").alias("track_id"), col("a_id").alias("artist_id"))
    track_artists.write.saveAsTable("track_artists", format="orc", mode="error") # write_mode="ignore"

    tracks = tracks.drop("artists", "artist_ids")
    for c in tracks.columns:
      tracks = tracks.withColumnRenamed(c, "t_{0}".format(c))
    tracks.write.saveAsTable("track", format="orc", mode="error")  # write_mode="ignore"
