use spotify_demo;
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
;
