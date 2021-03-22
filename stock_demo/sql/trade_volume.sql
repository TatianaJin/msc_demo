use stock_demo;
-- example: run by `spark-sql -d target_year=2020 --conf "spark.hadoop.hive.cli.print.header=true"` or uncomment the following command
-- set target_year=2020;

-- Find top 100 stocks with highest average daily trading volume in the target year
select p_code, avg(h_volume) as avg_volume, industry.industry
from history, profile, industry
where industry.id = p_industry_id and h_code = p_code and date_part('year', h_date)=${target_year}
group by p_code, industry.industry
order by avg_volume desc
limit 100;

-- Analyze the relationship between tweet number and stock price changes and trade volume
set col_a=volume;
-- set col_b=price_difference_percent;
set col_b=n_tweets;
with daily as (
  select h_code stock, h_date trading_date, h_volume volume, (h_close-h_open)/h_open * 100 as price_difference_percent, (h_high-h_low)/h_close * 100 as price_fluctuation_percent, n_tweets
  from history inner join
    (select t_code, date(t_date) as day, count(t_content) as n_tweets from tweets group by t_code,date(t_date)) as p
  on h_code=p.t_code and h_date=date(p.day)
)
select stock,
  ((sum_ab - (sum_a * sum_b / cnt)) / sqrt((sum_a_sq - pow(sum_a, 2.0) / cnt) * (sum_b_sq - pow(sum_b, 2.0) / cnt))) as correlation_${col_a}_${col_b}
from (
  select stock, sum(${col_a}) sum_a, sum(${col_b}) sum_b, sum(${col_a} * ${col_a}) sum_a_sq, sum(${col_b} * ${col_b}) sum_b_sq, sum(${col_a} * ${col_b}) sum_ab, count(*) as cnt
  from daily
  where ${col_a} is not null and ${col_b} is not null
  group by stock
) aux
-- limit 100
;
