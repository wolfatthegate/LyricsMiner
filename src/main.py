import logging
import pymongo
import concurrent.futures
import LyricsMiner
import DB
from pymongo import UpdateOne
import time

from datetime import datetime

def main():
    start = time.time()
    ### Initialization
    myquery = {'type': 0}

    noofdoc = DB.tweetTbl.find(myquery).count() #find() method returns a list of dictionary

    parallel = 1 ### 0 for serial program ### 1 for parallel program  
    x = 0
    y = 200
    
    while x < noofdoc:
        docs = DB.tweetTbl.find(myquery).limit(y)
    
        if parallel == 0: ###   RUN IN SERIAL 
            for doc in docs: 
                LyricsMiner.searchTweet(doc)
            
        else:             ###   RUN IN PARALLEL 
            with concurrent.futures.ProcessPoolExecutor() as executor:
                executor.map(LyricsMiner.searchTweet, docs)
       
        if x%(2000) == 0 :  ###   Write a log after every x document
            with open('logs/'+ DB.tablename +'Log.log', 'a') as logfile:
                now = datetime.now()
                dt_string = now.strftime("%m/%d/%Y %H:%M:%S")
                logfile.write('{}: {} scanned {} documents \n'.format(DB.tablename, dt_string, x)) 
        x = x + y

    stop = time.time()
    print("total time: {}".format(stop-start))
    
    
if __name__ == "__main__":
    print('start main')
    main()
                                

