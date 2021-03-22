use spotify_demo;

-- show the frequency of key
select count(*) track_number, CASE WHEN (t_key = 0) THEN 'C'
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
end key
from track
group by t_key
order by track_number desc
;

-- show mainstream genres
with artist_genre as (
  select a_name, explode(genres) genre
  from artist
  where genres is not null and cardinality(genres) > 0)
select count(*) artist_number, genre
from artist_genre
group by genre
order by artist_number desc
limit 20
;

-- show the most popular artists
select max(t_popularity) popularity, percentile(t_popularity,0.9) popularity90, percentile(t_popularity, 0.5) popularity50, a_name
from track, track_artists ta, artist
where ta.track_id = track.t_id and artist.a_id = ta.artist_id
group by a_name
order by popularity desc
limit 20;
