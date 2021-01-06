import pymongo
import logging

tablename = '2017-07-16-WeeklyTweets'
myclient = pymongo.MongoClient("mongodb://localhost:27017/")
mydb = myclient['LyricsDB']
lyricTbl = mydb['Lyrics']
twitterDataTbl = myclient['TwitterData']
tweetTbl = twitterDataTbl[tablename]

logFormatter = '%(asctime)s - %(_id)s - %(levelname)s - %(message)s'
logging.basicConfig(filename='logs/'+tablename+'_Logs.log',level=logging.DEBUG, format='%(asctime)s %(_id)s %(levelname)s %(message)s', datefmt='%m/%d/%Y %I:%M:%S %p')

threshold = 0.65
mid_score = 0.39
high_score = 0.64
type = 1