use stock_demo;
-- spark-sql -d target_year=2020 -d target_stock=AAPL -d period=50 --conf "spark.hadoop.hive.cli.print.header=true"
-- set target_stock=BAF;
-- set target_year=2020;
-- set period=50;

-- Show basic info
select p_code stock, p_company_name name, p_currency currency, p_ceo ceo, p_ipo_date ipo_date,
  p_exchange_code exchange, p_country country, sector, industry
from profile, industry
where p_industry_id = industry.id and p_code='${target_stock}'
;

-- Compare the closing price and the average closing price over the last `period` days for `target_stock` in `target_year`
select h_date, h_close, avg(h_close) over (partition by h_code order by h_date rows ${period} preceding) as moving_average
from history
where h_code = '${target_stock}' and date_part('year', h_date) = ${target_year}
--limit 100
;

-- Show other companies with the same CEO
select self.p_code me, other.p_code peer, other.p_ceo ceo, other.p_ipo_date peer_ipo_date
from profile other, profile self
where self.p_ceo != 'None' and other.p_ceo != 'None'
  and self.p_code='${target_stock}'  and self.p_ceo = other.p_ceo and other.p_code != '${target_stock}'
order by peer_ipo_date
