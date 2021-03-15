import pymongo
import sys
import concurrent.futures
import warnings
warnings.filterwarnings("ignore")
 
def updateTweet(mydoc):
    updatequery = {'_id': mydoc['_id']}
    newvalue = { '$set': {'excluded': 0}}
    lyricTbl.update_one(updatequery, newvalue)
    
myclient = pymongo.MongoClient("mongodb://localhost:27017/")

mydb = myclient['TwitterData']
lyricTbl = mydb['2016-11-Nov-week-1']

myquery = {'excluded': {'$ne': 0}}
noofdoc = lyricTbl.find(myquery).count()

x = 0
inc = 100

while x < noofdoc:
    docs = lyricTbl.find(myquery).skip(x).limit(inc) #find() method returns a list of dictionary
    with concurrent.futures.ProcessPoolExecutor() as executor:
        executor.map(updateTweet, docs)
    x = x + inc 



            
                