import pymongo
import sys
import concurrent.futures
import warnings
warnings.filterwarnings("ignore")
 
def updateTweet(mydoc):
    updatequery = {'_id': mydoc['_id']}
    newvalue = { '$set': {'score': 0, 'suggestions': '', 'type': 0}}
    lyricTbl.update_one(updatequery, newvalue)
    
myclient = pymongo.MongoClient("mongodb://localhost:27017/")

mydb = myclient['TwitterData']
lyricTbl = mydb['2016-11-Nov-week-1']

myquery = {}
noofdoc = lyricTbl.find(myquery).count()

x = 0
inc = 100

docs = lyricTbl.find(myquery) #find() method returns a list of dictionary
with concurrent.futures.ProcessPoolExecutor() as executor:
    executor.map(updateTweet, docs)




            
                