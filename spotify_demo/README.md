### ETL

Run `spark-submit etl/etl.py <data_dir>` to carry out ETL on the raw data, and `<data_dir>` is the folder where your Spotify data are located.
_You may use the default data on proj10 by running `spark-submit etl/etl.py` without specifying `<data_dir>`._

### Database

#### Phase 2 (Initial schema, subject to change)

![ER Diagram](https://github.com/TatianaJin/msc_demo/blob/master/spotify_demo/ER.png)

#### Phase 3 (check the `analysis` folder)

Run `spark-submit analysis/similar_artists.py` to use k-means to generate the clusters for artists.

![ER Diagram](https://github.com/TatianaJin/msc_demo/blob/master/spotify_demo/ER_v2.png)

The SQL queries are example analytical queries on the database with varied complexity.
