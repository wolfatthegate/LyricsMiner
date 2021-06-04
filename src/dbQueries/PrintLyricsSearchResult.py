import pymongo
import sys

def main():
    tablename = sys.argv[1]
    
    myclient = pymongo.MongoClient("mongodb://localhost:27017/")
    myclient.list_database_names()
    
    twitterDataTbl = myclient['TwitterData']
    twitterDataTbl.list_collection_names()
    
    tweetTbl = twitterDataTbl[tablename]
    myquery = { 'score' : {"$gt": 0.49}}
    myquery = {}
    
    noofdoc = tweetTbl.find(myquery).count()

    x = 0
    y = 50
    
    while x < 1:
        docs = tweetTbl.find(myquery).skip(x).limit(y)
        
        for doc in docs: 
            with open("LyricsResult.txt", "a") as file: 
                file.write(str(doc['tweet']) + '\t' + str(doc['tweetID']) + '\n')
        x = x + y
    
if __name__ == "__main__":
    main()
