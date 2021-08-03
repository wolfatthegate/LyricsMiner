import logging
import pymongo
import concurrent.futures
import LyricsMiner
import DB
from pymongo import UpdateOne
import time

from datetime import datetime

def main():
    
    ### Initialization
    myquery = {'type':{'$not':{'$in':[1]}}}
    
    noofdoc = DB.tweetTbl.find(myquery).count() #find() method returns a list of dictionary
    print('total number of docs in query - {}'.format(noofdoc))
    parallel = 0 ### 0 for serial program ### 1 for parallel program  
    scanned = 0
    y = 200
    
    while noofdoc > 0:
        
        docs = DB.tweetTbl.find(myquery).limit(y)
    
        if parallel == 0: ###   RUN IN SERIAL 
            for doc in docs: 
                LyricsMiner.searchTweet(doc)
            
        else:             ###   RUN IN PARALLEL 
            with concurrent.futures.ProcessPoolExecutor() as executor:
                executor.map(LyricsMiner.searchTweet, docs)
       
        scanned += y
        
        if scanned%(2000) == 0 :  ###   Write a log after every x document
            with open('logs/'+ DB.tablename +'Log.log', 'a') as logfile:
                now = datetime.now()
                dt_string = now.strftime("%m/%d/%Y %H:%M:%S")
                logfile.write('{}: {} scanned {} documents \n'.format(DB.tablename, dt_string, scanned)) 
        
        noofdoc = DB.tweetTbl.find(myquery).count()

if __name__ == "__main__":
    print('start main - scanning {}'.format(DB.tablename))
    start = time.time()
    main()
    stop = time.time()
    print("total time: {}".format(stop-start))
                                

