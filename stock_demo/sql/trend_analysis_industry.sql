use stock_demo;
-- example: run by `spark-sql -d target_year=2020 -d period=30 -f trend_analysis_industry.sql` or uncomment the following 2 commands
-- set target_year=2020;
-- set period=90;

create temporary view  industry_summary as
select p_industry_id, h_date, avg(h_close) as avg_close, sum(h_close) as sum_close
from history inner join profile on h_code = p_code
where p_industry_id is not null
group by p_industry_id, h_date
order by p_industry_id, h_date
;

-- For each industry, analyze the trend (accounting for last `period` days) for each day over the last 3 years and find up and down periods
with trends as (
  select p_industry_id, h_date, avg_close, ma_short, ma_long, case when ma_short>ma_long then 'up' else 'down' end as indicator 
  from (
    select p_industry_id, h_date, avg_close,
      avg(avg_close) over (partition by p_industry_id order by h_date rows ${period} preceding) as ma_short,
      avg(avg_close) over (partition by p_industry_id order by h_date rows ${period} * 2 preceding) as ma_long
    from industry_summary
    where date_part('year', h_date) between ${target_year} - 3 and ${target_year}
  ) as i where date_part('year', h_date) between ${target_year} - 2 and ${target_year}
),
ordering as (
  select row_number() over (partition by p_industry_id order by h_date) as idx, p_industry_id, h_date, indicator from trends
),
change_dates as (
  select b.p_industry_id as industry_id, a.h_date last_date, a.indicator last_trend, b.h_date change_date, b.indicator change_trend
  from ordering a, ordering b where a.p_industry_id = b.p_industry_id and a.idx = b.idx - 1 and a.indicator != b.indicator
),
report as (
  select industry_id,
    datediff(last_date, lag(change_date, 1) over (partition by industry_id order by change_date)) period,
    lag(change_date, 1) over (partition by industry_id order by change_date) period_start_date,
    last_date period_end_date, last_trend trend
  from change_dates
)
select sector, industry.industry, trend, period, period_start_date, period_end_date
from report inner join industry on industry.id = industry_id
where period_start_date is not null order by industry_id, period_start_date
--limit 100
;


create temporary view sector_summary as 
select sector, h_date, avg(h_close) as avg_close
from history inner join profile on h_code = p_code inner join industry on p_industry_id = industry.id
where p_industry_id is not null
group by sector, h_date
order by sector, h_date
;


-- For each sector, analyze the trend (accounting for last `period` days) for each day over the last 3 years and find up and down periods
with trends as (
  select sector, h_date, avg_close, ma_short, ma_long, case when ma_short>ma_long then 'up' else 'down' end as indicator 
  from (
    select sector, h_date,
      lag(avg_close, ${period}) over (partition by sector order by h_date) as avg_close,
      avg(avg_close) over (partition by sector order by h_date rows ${period} preceding) as ma_short,
      avg(avg_close) over (partition by sector order by h_date rows ${period} * 2 preceding) as ma_long
    from sector_summary
    where date_part('year', h_date) between ${target_year} - 3 and ${target_year}
  ) as i where date_part('year', h_date) between ${target_year} - 2 and ${target_year}
),
ordering as (
  select row_number() over (partition by sector order by h_date) as idx, sector, h_date, indicator, avg_close from trends
),
change_dates as (
  select b.sector as sector, a.h_date last_date, a.indicator last_trend, b.h_date change_date, b.indicator change_trend, b.avg_close change_close, a.avg_close last_close
  from ordering a, ordering b where a.sector = b.sector and a.idx = b.idx - 1 and a.indicator != b.indicator
),
report as (
  select sector,
    datediff(lead(last_date, 1) over (partition by sector order by change_date), change_date) period,
    change_date period_start_date,
    lead(last_date, 1) over (partition by sector order by change_date) period_end_date,
    change_trend trend, change_close as period_start_closing_price,
    lead(last_close, 1) over (partition by sector order by change_date) period_end_closing_price
  from change_dates
)
select * from report
where period_end_date is not null order by sector, period_start_date
-- limit 100
;
