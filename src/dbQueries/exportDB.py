import pandas
import pymongo

tablename = '2016-11-Nov-week-1'
myclient = pymongo.MongoClient("mongodb://localhost:27017/")
twitterDataTbl = myclient['TwitterData']
matchScoreGT = 0.49  
myquery = { 'score' : {"$gt": matchScoreGT}}

tweetTbl = twitterDataTbl[tablename]
cursor = tweetTbl.find(myquery)

print(list(cursor))