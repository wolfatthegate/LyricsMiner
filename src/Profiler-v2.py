### Profiler list the Lyrics tweeters and the followers to measure the influence. 
### Author - Waylon Luo


import pymongo
import sys
import pandas as pd

def main(): 

    ### DB connection

    table_name = sys.argv[1] #'2017-10-week-1'
    myclient = pymongo.MongoClient("mongodb://localhost:27017/")
    twitterDB = myclient['TwitterData']
    tweetTbl = twitterDB[table_name]
    
    ### Query
    
    dataList = tweetTbl.find({'score': {'$gt': 0.49}})
    
    ### initialization
    
    usrlist = ['userid', 'follower', 'tweet']

    for data in dataList:      
        usrlist.append(data['userid'], data['follower'], data['tweet'])
    
    df = pd.DataFrame(usrlist)
    df.to_csv(file_name+'-profiles.csv', index = False)
    
if __name__ == "__main__":
    main()
