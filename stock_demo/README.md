### Code Architecture

```text
|- crawlers: the python scripts for crawling data
|- etl: the spark programs for data cleaning, data preprocessing, and data loading to database
```


### ETL

Run `spark-submit etl/etl.py <data_dir>` to carry out ETL on the raw data, and `<data_dir>` is the folder where your history, profile and tweets data are located.
_You may use the default data on proj10 by running `spark-submit etl/etl.py` without specifying `<data_dir>`._

### Database

#### Phase 2 (Initial schema, subject to change)

![ER Diagram](https://github.com/TatianaJin/msc_demo/blob/master/stock_demo/ER.png)
