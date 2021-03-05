from pyspark.sql import SparkSession
from pyspark.sql.functions import col




####################### Data ETL with Spark (ETL - Extract and Transform) #######################

# Start a spark session with Hive

spark = SparkSession.builder\
    .appName("Demo")\
    .master("local")\
    .enableHiveSupport()\
    .getOrCreate()\
        
    ########## Profile ###########
    
history_df = spark.read.option("header",True)\
    .csv("C:\\demo\\demo1\\history\\*.csv")
    
          # Rename column to avoid conflicts with system reserved symbols  
          # Also extract columns except dividends and volume as they are not meaningful in price database
          # This two columns are included in stock profile already
                    
history_df = history_df.select(col("Ticket"),col("date").alias("T_date"),col("open"),col("close"),col("high"),col("low"),col("Stock Splits").alias("Splits"))



    ######### Stock List ##########

list_df = spark.read.option("header",True)\
    .csv("C:\\demo\\demo1\\Stock_List.csv")
    
    
    ######### Tweet #############
    
tweet_df = spark.read.option("header", True)\
    .csv("C:\\demo\\demo1\\Tweet.csv")
    
profile_df = spark.read.option("header", True)\
    .csv("C:\\demo\\demo1\\Profile.csv")
    

    
    


#profile_df.registerTempTable("testing")    
#test = spark.sql("SELECT Open, Close FROM testing WHERE Ticket = 'AAPL'")
#test.show()



######################   Store into Hive (ETL - Load)  #######################


history_df.write.mode("overwrite").saveAsTable("History")

list_df.write.mode("overwrite").saveAsTable("Stock_List")

tweet_df.write.mode("overwrite").saveAsTable("Tweet")


