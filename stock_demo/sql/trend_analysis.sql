use stock_demo;
-- example: run by `spark-sql -d target_year=2020 -d period=30 -f trend_analysis.sql` or uncomment the following 2 commands
-- set target_year=2020;
-- set period=30;

-- For each stock, analyze the trend (accounting for last `period` days) for each day over the last 3 years and find up and down periods
with trends as (
  select h_code, h_date, h_close, ma_short, ma_long, case when ma_short>ma_long then 'up' else 'down' end as indicator 
  from (
    select h_code, h_date, h_close,
      avg(h_close) over (partition by h_code order by h_date rows ${period} preceding) as ma_short,
      avg(h_close) over (partition by h_code order by h_date rows ${period} * 2 preceding) as ma_long
    from history
    where date_part('year', h_date) between ${target_year} - 3 and ${target_year}
    --and h_code = '${target_stock}'
  ) as i where date_part('year', h_date) between ${target_year} - 2 and ${target_year}
),
ordering as (
  select row_number() over (partition by h_code order by h_date) as idx, h_code, h_date, ma_short, indicator from trends
),
change_dates as (
  select b.h_code as stock, a.h_date last_date, a.indicator last_trend, b.h_date change_date, b.indicator change_trend
 -- , b.ma_short change_price, a.ma_short last_price
  from ordering a, ordering b where a.h_code = b.h_code and a.idx = b.idx - 1 and a.indicator != b.indicator
),
report as (
  select stock,
    --last_price - lag(change_price, 1) over (partition by stock order by change_date) price_change,
    datediff(last_date, lag(change_date, 1) over (partition by stock order by change_date)) period,
    lag(change_date, 1) over (partition by stock order by change_date) period_start_date,
    last_date period_end_date, last_trend trend
  from change_dates
)
select * from report where period_start_date is not null order by stock, period_start_date
--limit 100
;
