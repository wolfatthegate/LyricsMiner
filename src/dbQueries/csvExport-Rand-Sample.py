import random
import sys
import pymongo
import pandas as pd

def main(): 
    tablename = sys.argv[1]
    filename = sys.argv[2]
    
    myclient = pymongo.MongoClient("mongodb://localhost:27017/")
    twitterDataTbl = myclient['TwitterData']
    tweetTbl = twitterDataTbl[tablename]
    cursor = tweetTbl.find({'score':{'$gt':0.49}})
    
    sampleSize = 200
    cursor_list = list(cursor)
    random.shuffle(cursor_list)
    randCursor = random.sample(cursor_list, sampleSize)
    
    df = pd.DataFrame(randCursor)
    df.to_csv(filename+'.csv', index=False)

if __name__ == "__main__":
    main()