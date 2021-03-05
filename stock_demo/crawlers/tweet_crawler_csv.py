#! /usr/bin/env python3

import tweepy
import csv

# twitter API credentials of our account
# SNSCRAPE could be a better choice for tweet crawling without limitation

consumer_key = "aaaaaaaaaaaa"
consumer_secret = "zzzzzzzzzzz"
access_token = "xxxxxxxxx"
access_token_secret = "yyyyyyyyyy"

data_dir = '//data//opt//users//destiny//resource//'

with open(os.path.join(data_dir, 'Stock_List.csv'), 'r') as stocklist:
    with open(os.path.join(data_dir, 'Tweet.csv'), 'w') as tweetlist:

        # prepare file to be written

        fields = ['Stock', 'Date', 'Content', 'Author']
        tweetwriter = csv.DictWriter(tweetlist, fields)
        tweetwriter.writeheader()

        # read stock list csv and skip its title row
        rstocklist = csv.reader(stocklist)
        next(rstocklist)

        # crawl data for each of the stocks in our list

        for row in rstocklist:

            tweetsPerQry = 100
            maxTweets = 1000000
            print("Searching for $" + row[0])
            hashtag = "$" + row[0]
            new_search = hashtag + " -filter:retweets"

            auth = tweepy.OAuthHandler(consumer_key, consumer_secret)
            auth.set_access_token(access_token, access_token_secret)

            api = tweepy.API(auth, wait_on_rate_limit=(True))

            tweets = tweepy.Cursor(api.search, q=new_search, lang="en", since="2020-01-01").items()

            for tweet in tweets:
                tweetwriter.writerow({'Stock': row[0], 'Date': str(tweet.created_at), 'Content': tweet.text, 'Author': tweet.author.name})
