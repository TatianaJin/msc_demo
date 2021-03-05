from pyspark.sql import SparkSession
from pyspark.sql import Row
from pyspark.sql.functions import col, input_file_name, split, udf, when, row_number, to_date, to_timestamp, trim
from pyspark.sql.window import Window
from pyspark.sql.types import DateType, DoubleType, IntegerType, StringType, StructType, StructField

import os
from sys import argv
from dictionaries import country_code, format_exchange_name


class Args:

    def __init__(self, data_dir):
        self.profile_dir = os.path.join(data_dir, "profile")
        self.history_dir = os.path.join(data_dir, "history")
        self.tweet_path = os.path.join(data_dir, "Tweet.csv")
        self.profile_fields = {
            "code": 1,
            "company_name": 9,
            "currency": 10,
            "country": 21,
            "state": 26,
            "city": 25,
            "sector": 20,
            "industry": 16,
            "exchange": 14,
            "exchange_code": 15,
            "description": 18,
            "ceo": 19,
            "address": 24,
            "ipo_date": 31
        }

    def __str__(self):
        return "Args {{\n  profile_dir={},\n  history_dir={},\n  tweet_path={}\n}}".format(self.profile_dir, self.history_dir, self.tweet_path)


def parse_args(argv=None):
    if "-h" in argv:
        print("usage: spark-submit etl.py <profile_dir> <history_dir> <tweet_path>")
        exit(0)
    # default
    data_dir = "/data/opt/msc2020/demo/demo1"
    if len(argv) > 1:
        data_dir = argv[1]
    args = Args(data_dir)
    return args


def parse_profile_fields(profiles, name_index_map):
    split_col = split(profiles.value, '\n')
    for name, index in name_index_map.items():
        field = split(split_col.getItem(index), ',').getItem(1)
        # Handle nulls: empty string and N/A
        profiles = profiles.withColumn(name, when(field != '', when(field != 'N/A', field)))
    profiles = profiles.select(*name_index_map.keys())
    return profiles


def parse_tweets_fields(tweets):
    split_col = split(tweets.value, ',', limit=3)
    # https://spark.apache.org/docs/latest/sql-ref-datetime-pattern.html
    tweets = tweets.withColumn("t_code", trim(split_col.getItem(0))).withColumn("t_date", to_timestamp(split_col.getItem(1), format='d/M/yyyy H:mm'))
    # content may contain comma, so we need to use rsplit
    rsplit_get_second = udf(lambda val: None if val is None else val.rsplit(',', 1)[-1].replace('"', ''))
    rsplit_get_first = udf(lambda val: val.rsplit(',', 1)[0].replace('\n', '\\n') if val is not None else None)
    tweets = tweets.withColumn("t_author", rsplit_get_second(split_col.getItem(2)))
    tweets = tweets.withColumn("t_content", trim(rsplit_get_first(split_col.getItem(2))))
    tweets = tweets.where("t_date is not null").select("t_code", "t_date", "t_author", "t_content")
    return tweets


def show_distinct(df, *column, skip=False, filter_none=False, count_only=False):
    if skip:
        return
    distinct = df.select(*column).distinct()
    if filter_none:
        for c in column:
            distinct = distinct.where("{0} is not null".format(c))
    print("Distinct values{2} of {0}: {1}".format(','.join(column), distinct.count(), "" if filter_none else " (including none)"))
    if count_only:
        return
    for row in distinct.sort(*column).collect():
        for c in column[:-1]:
            print(row[c], end=",")
        print(row[column[-1]])


def build_dictionary_table(df, *column, index_column="id"):
    dictionary = df.select(*column).distinct().sort(*column)
    for c in column:
        dictionary = dictionary.where("{0} is not null".format(c))
    w = Window().orderBy(*column)
    dictionary = dictionary.withColumn(index_column, row_number().over(w))
    return dictionary


def tweets_etl(args, write_mode="error"):
    tweets = spark.read.text(args.tweet_path, lineSep='\r\n')
    tweets = parse_tweets_fields(tweets)
    # preview
    print("tweets", tweets.count())
    tweets.show()
    show_distinct(tweets, "t_author", skip=False, count_only=True)
    show_distinct(tweets, "t_author", "t_date", skip=False, count_only=True)
    tweets.write.saveAsTable("tweets", format="orc", mode=write_mode)
    return tweets


def history_etl(args, write_mode="error"):
    schema = StructType([
        StructField("date", DateType(), False),
        StructField("open", DoubleType(), False),
        StructField("high", DoubleType(), False),
        StructField("low", DoubleType(), False),
        StructField("close", DoubleType(), False),
        StructField("volume", DoubleType(), False),
        StructField("dividends", DoubleType(), False),
        StructField("stock splits", DoubleType(), False)
        #,StructField("corrupted", StringType(), True)
    ])
    # some files have different schema and do not have records, we skip them by DROPMALFORMED
    history = spark.read.csv(
        args.history_dir,
        enforceSchema=True,  # set enforceSchema=True with care, better set to false to check first
        header=True,
        schema=schema,  # columnNameOfCorruptRecord="corrupted",
        mode="DROPMALFORMED")
    get_code = udf(lambda name: os.path.basename(name).split("_")[0])
    history = history.select(
        get_code(input_file_name()).alias("h_code"),
        col("date").alias("h_date"),
        col("open").alias("h_open"),
        col("high").alias("h_high"),
        col("low").alias("h_low"),
        col("close").alias("h_close"),
        col("volume").alias("h_volume"),
        col("dividends").alias("h_dividends"),
        col("stock splits").alias("h_stock_splits")
        #,"corrupted"
    )
    # preview
    print("history", history.count())
    history.show()
    print("stocks that have history records:")
    show_distinct(history, "h_code", skip=False, count_only=True)
    history.write.saveAsTable("history", format="orc", mode=write_mode)
    return history


def profile_etl(args, write_mode="error"):
    profiles = spark.read.text(args.profile_dir, wholetext=True)

    ######################### Data cleaning #########################

    # 1. Parse columns and handle nulls
    profiles = parse_profile_fields(profiles, args.profile_fields)

    # 2. Unify variants of the same entity/meaning
    # 2.1. example 1: Use ISO Alpha 2 country code for `country` column (an independent column)
    show_distinct(profiles, "country")
    to_country_code = udf(lambda name: country_code[name] if name in country_code else name)
    # TODO(etl): unify names for state and city
    show_distinct(profiles, "state")
    # format_state = udf(lambda name: /* to state code */ if name is not None else None)
    show_distinct(profiles, "city")
    format_city = udf(lambda name: name.title() if name is not None else None)

    # 2.2. example 2: Format exchange names for `exchange` column (there is correspondence between two columns)
    # TODO(etl): there are mismatches between exchange names and exchange codes
    show_distinct(profiles, "exchange", "exchange_code")
    format_exchange = udf(lambda name: format_exchange_name[name] if name in format_exchange_name else name)
    # TODO(etl): sector and industry columns
    format_sector = format_city
    format_industry = format_city

    # Projection
    profiles = profiles.select("code", "company_name", "currency",
                               to_country_code("country").alias("country"), "state",
                               format_city("city").alias("city"),
                               format_sector("sector").alias("sector"),
                               format_industry("industry").alias("industry"),
                               format_exchange("exchange").alias("exchange"), "exchange_code", "description", "ceo", "address",
                               to_date("ipo_date").alias("ipo_date"))
    # now check the distinct values of cleaned data
    show_distinct(profiles, "country")
    show_distinct(profiles, "currency")
    show_distinct(profiles, "exchange", "exchange_code")
    show_distinct(profiles, "sector", "industry", filter_none=True)
    show_distinct(profiles, "sector")
    show_distinct(profiles, "industry")
    # preview
    profiles.show(5)
    print("profiles", profiles.count())

    ######################### Database normalization #########################

    profiles.registerTempTable("profiles")
    # 1. Build dictionary tables
    #    example: sector-industry
    profiles = profiles.select(
        "code",
        "company_name",
        "currency",
        "description",
        "ceo",
        "address",
        "ipo_date",
        "exchange",
        "exchange_code",
        "country",
        "state",
        "city",
        # use the sector value as industry if industry is null and vice versa
        when(col("sector").isNull(), col("industry")).otherwise(col("sector")).alias("sector"),
        when(col("industry").isNull(), col("sector")).otherwise(col("industry")).alias("industry"))
    industries = build_dictionary_table(profiles, "sector", "industry")

    # 2. Normalize profiles using dictionary tables
    industries.registerTempTable("industries")
    # use left join as we do not want to discard records for which sector and industry are both null
    # TODO(etl): dictionary and normalization for country-state-city
    profiles_normalized = spark.sql(
        "SELECT code as p_code, company_name as p_company_name, currency as p_currency, description as p_description, ceo as p_ceo, address as p_address, ipo_date as p_ipo_date, "
        "exchange as p_exchange, exchange_code as p_exchange_code, country as p_country, state as p_state, city as p_city, industries.id as p_industry_id "
        "FROM profiles "
        "LEFT JOIN industries ON profiles.sector = industries.sector and profiles.industry = industries.industry")
    # preview
    profiles_normalized.show(5)
    print("profiles_normalized", profiles_normalized.count())

    ######################### Write to hive #########################

    industries.write.saveAsTable("industry", format="orc", mode=write_mode)
    # TODO(etl): consider partitioned table as needed
    profiles_normalized.write.saveAsTable("profile", format="orc", mode=write_mode)
    return profiles_normalized


if __name__ == "__main__":
    args = parse_args(argv)
    print(args)
    spark = SparkSession.builder.appName("ETL Demo").enableHiveSupport().getOrCreate()
    spark.sql("create database if not exists stock_demo;")
    spark.sql("use stock_demo;")
    profile_etl(args, write_mode="overwrite")  # write_mode="ignore"
    history_etl(args)
    tweets_etl(args)
