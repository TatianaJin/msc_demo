use spotify_demo;

set artist_target=Passenger;
set cluster_table=artist_cluster_features_smoothed;

-- show some basic statistics of the given artist
select count(*) track_number, count(distinct t_album_id) album_number, min(t_release_date) first_release, max(t_release_date) last_release, avg(t_popularity) avg_popularity
from track_artists ta, artist, track
where ta.track_id = track.t_id and artist.a_id = ta.artist_id and artist.a_name='${artist_target}'
;

-- show the top 10 popular tracks by the given artist
with all_tracks as (
  select * from track_artists ta, artist, track
  where ta.track_id = track.t_id and artist.a_id = ta.artist_id and artist.a_name='${artist_target}')
SELECT t_name as name,
  CASE WHEN (t_key = 0) THEN 'C'
    WHEN (t_key = 1) THEN 'C#/Db'
    WHEN (t_key = 2) THEN 'D'
    WHEN (t_key = 3) THEN 'D#/Eb'
    WHEN (t_key = 4) THEN 'E'
    WHEN (t_key = 5) THEN 'F'
    WHEN (t_key = 6) THEN 'F#/Gb'
    WHEN (t_key = 7) THEN 'G'
    WHEN (t_key = 8) THEN 'G#/Ab'
    WHEN (t_key = 9) THEN 'A'
    WHEN (t_key = 10) THEN 'A#/Bb'
    WHEN (t_key = 11) THEN 'B'
    ELSE 'Unknown'
  end key,
  case when t_mode then 'Major' else 'Minor' end music_mode,
  t_year year, t_release_date release_date, t_album album, t_track_number track_number, t_disc_number disc_number, t_duration_ms/1000 duration_seconds
from all_tracks
order by t_popularity desc
limit 10
;

-- show statistics by albums
select t_album album, sum(t_acousticness) acousticness, sum(t_danceability) danceability, sum(t_energy) energy
from track_artists ta, artist, track
where ta.track_id = track.t_id and artist.a_id = ta.artist_id and artist.a_name='${artist_target}'
group by album
order by acousticness, danceability, energy

-- show artist features
select a_name name, genres, a_mode modality, am.a_acousticness acousticness, am.a_danceability danceability, am.a_energy energy, am.a_loudness loudness,
  am.a_speechiness speechiness, am.a_instrumentalness instrumentalness, am.a_liveness liveness, am.a_valence valence, am.a_tempo tempo
from artist, artist_measure am
where artist.a_id = am.a_id and artist.a_name = '${artist_target}'
;

-- show similar artists by features
with similar as (
  select b.a_id candidate, genres
  from artist, ${cluster_table} a, ${cluster_table} b
  where artist.a_id = a.a_id and artist.a_name = '${artist_target}' and a.prediction = b.prediction and a.a_id != b.a_id
)
select artist.a_name artist_name, artist.genres, am.a_track_number track_number, am.a_mode modality, am.a_acousticness acousticness, am.a_danceability danceability,
am.a_energy energy, am.a_loudness loudness, am.a_speechiness speechiness, am.a_instrumentalness instrumentalness, am.a_liveness liveness, am.a_valence valence, am.a_tempo tempo
from similar, artist, artist_measure am
where am.a_id = similar.candidate and artist.a_id = similar.candidate and cardinality(array_intersect(artist.genres, similar.genres)) > 0
order by am.a_track_number desc
limit 10;
