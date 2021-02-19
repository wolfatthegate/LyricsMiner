### This program makes a quick scan to a twitter table
### and returns the number of tweets that have been scanned
### Author - Waylon Luo
### 01-06-2021

import pymongo
import sys

def main():
    tablename = sys.argv[1]
    myclient = pymongo.MongoClient("mongodb://localhost:27017/")
    twitterDataTbl = myclient['TwitterData']
        
    tweetTbl = twitterDataTbl[tablename]
    myquery = {'type': 1}
    noofdoc = tweetTbl.find(myquery).count()
    print(noofdoc)

if __name__ == "__main__":
    main()

