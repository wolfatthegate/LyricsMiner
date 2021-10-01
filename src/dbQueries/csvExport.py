import sys
import pymongo
import pandas as pd

def main(): 
    
    dbname = sys.argv[1] #TwitterData #tweet_drugs
    tablename = sys.argv[2] #2017-01-week-1 #lyrics_match
    filename = sys.argv[3]
    
    myclient = pymongo.MongoClient("mongodb://localhost:27017/")
    twitterDataTbl = myclient[dbname]
    tweetTbl = twitterDataTbl[tablename]
    cursor = tweetTbl.find({'score':{'$gt':0.49}})
    
    df =  pd.DataFrame(list(cursor))
    df.to_csv(filename+'.csv', index=False)

if __name__ == "__main__":
    main()
