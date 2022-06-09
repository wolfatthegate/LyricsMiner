'''
Author - Waylon Luo
'''

import pymongo
import logging

tablename = 'tweets_valid'
lyricsMatchTbl = 'lyrics_match'

myclient = pymongo.MongoClient("mongodb://localhost:27017/")
mydb = myclient['LyricsDB']
lyricTbl = mydb['Lyrics']
twitterDataTbl = myclient['tweet_drugs']
tweetTbl = twitterDataTbl[tablename]
lyricsMatchTbl = twitterDataTbl[lyricsMatchTbl]

logFormatter = '%(asctime)s - %(_id)s - %(levelname)s - %(message)s'
logging.basicConfig(filename='logs/'+tablename+'_Logs.log',level=logging.INFO, format='%(asctime)s %(_id)s %(levelname)s %(message)s', datefmt='%m/%d/%Y %I:%M:%S %p')

threshold = 0.65
high_score = 0.49
type = 1
