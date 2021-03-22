from pyspark.sql import SparkSession
from pyspark.ml.feature import StandardScaler, VectorAssembler
from pyspark.ml.clustering import KMeans
from pyspark.sql.functions import col

from sys import argv


class Args:

    def __init__(self, database=None, agg_table_name=None, result_table_name=None):
        self.database = "spotify_demo" if database is None else database
        self.agg_table_name = "artist_measure" if agg_table_name is None else agg_table_name
        self.result_table_name = "artist_cluster" if result_table_name is None else result_table_name
        self.feature_kmeans_table_name = self.result_table_name + "_features"
        self.smoothed_kmeans_table_name = self.result_table_name + "_features_smoothed"

    def __str__(self):
        return "Args {{\n  database={},\n  agg_table_name={},\n  result_table_name={}\n}}".format(self.database, self.agg_table_name,
                                                                                                  self.result_table_name)


def parse_args(argv=None):
    if "-h" in argv:
        print("usage: spark-submit similar_artists.py <database> <agg_table_name> <result_table_name>")
        exit(0)
    args = Args(*argv[1:4]) if len(argv) > 3 else Args()
    return args


if __name__ == "__main__":
    args = parse_args(argv)
    print(args)
    spark = SparkSession.builder.appName("Spotify Similar Artists").enableHiveSupport().getOrCreate()
    spark.sql("use {0};".format(args.database))

    # Compute artist statistics
    spark.sql("""create table if not exists {0} as
select a_id,
  count(t_id) a_track_number,
  avg(case when t_mode then 1 else 0 end) a_mode,
  percentile(t_acousticness, 0.5) a_acousticness,
  percentile(t_danceability, 0.5) a_danceability,
  percentile(t_energy, 0.5) a_energy,
  percentile(t_loudness, 0.5) a_loudness,
  percentile(t_speechiness, 0.5) a_speechiness,
  percentile(t_instrumentalness, 0.5) a_instrumentalness,
  percentile(t_liveness, 0.5) a_liveness,
  percentile(t_valence, 0.5) a_valence,
  percentile(t_tempo, 0.5) a_tempo
from track, track_artists ta, artist
where ta.track_id = track.t_id and artist.a_id = ta.artist_id
group by a_id
    """.format(args.agg_table_name))

    agg_table = spark.table(args.agg_table_name)

    existing_tables = [table.name for table in spark.catalog.listTables()]
    # K-means on artist features
    if args.feature_kmeans_table_name not in existing_tables:
        # normalize features
        va = VectorAssembler(inputCols=[column for column in agg_table.columns if column != "a_id"], outputCol="raw_features")
        feature_table = va.transform(agg_table)
        standard_scaler = StandardScaler(inputCol="raw_features", outputCol="features")
        feature_table = standard_scaler.fit(feature_table).transform(feature_table).select("a_id", "raw_features", "features")
        feature_table.show()

        # k-means
        kmeans = KMeans(k=100)
        model = kmeans.fit(feature_table)
        clustered = model.transform(feature_table).select("a_id", "prediction")
        #clustered.show()
        clustered.write.saveAsTable(args.feature_kmeans_table_name, format="orc", mode="error")

    if args.smoothed_kmeans_table_name not in existing_tables:
        # Compute artist collaboration graph as edge list with self-loop
        collaboration = spark.sql("select a.artist_id node, b.artist_id neighbor from track_artists a, track_artists b where a.track_id = b.track_id") # and a.artist_id != b.artist_id
        collaboration.registerTempTable("collaboration")
        # Smooth the features of artists by averaging over their neighbors. For artist with no collaborator, its features should remain unchanged.
        artist_features = spark.sql("""select node, avg(am.a_track_number) track_number, avg(am.a_mode) modality, avg(am.a_acousticness) acousticness, avg(am.a_danceability) danceability, avg(am.a_energy) energy,
            avg(am.a_loudness) loudness, avg(am.a_speechiness) speechiness, avg(am.a_instrumentalness) instrumentalness, avg(am.a_liveness) liveness, avg(am.a_valence) valence, avg(am.a_tempo) tempo
            from collaboration, {0} am where am.a_id = neighbor
            group by node
        """.format(args.agg_table_name))

        # normalize features
        va = VectorAssembler(inputCols=[column for column in artist_features.columns if column != "node"], outputCol="raw_features")
        feature_table = va.transform(artist_features)
        standard_scaler = StandardScaler(inputCol="raw_features", outputCol="features")
        feature_table = standard_scaler.fit(feature_table).transform(feature_table).select(col("node").alias("a_id"), "features")

        # k-means
        kmeans = KMeans(k=100)
        model = kmeans.fit(feature_table)
        clustered = model.transform(feature_table).select("a_id", "prediction")
        clustered.write.saveAsTable(args.smoothed_kmeans_table_name, format="orc", mode="error")
