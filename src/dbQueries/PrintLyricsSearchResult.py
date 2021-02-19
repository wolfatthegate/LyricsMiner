import pymongo
import sys

def main():
    tablename = sys.argv[1]
    
    myclient = pymongo.MongoClient("mongodb://localhost:27017/")
    myclient.list_database_names()
    
    twitterDataTbl = myclient['TwitterData']
    twitterDataTbl.list_collection_names()
    
    tweetTbl = twitterDataTbl[tablename]
    myquery = { 'score' : {"$gt": 0.05}}
    
    noofdoc = tweetTbl.find(myquery).count()

    x = 0
    y = 50
    
    while x < noofdoc:
        docs = tweetTbl.find(myquery).skip(x).limit(y)
        
        for doc in docs: 
            with open("LyricsResult.txt", "a") as file: 
                file.write(str(doc['tweet'])+'\n')
        x = x + y
    
if __name__ == "__main__":
    main()
