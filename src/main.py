import logging
import pymongo
import concurrent.futures
import LyricsMiner
import DB

from datetime import datetime

def main():
    
    ### Initialization
    myquery = {"type": 2}
    myquery = {}

    noofdoc = DB.tweetTbl.find(myquery).count() #find() method returns a list of dictionary
    
    parallel = 1 ### 0 for serial program ### 1 for parallel program  
    x = 0
    y = 20
    
    while x < noofdoc:
        docs = DB.tweetTbl.find(myquery).skip(x).limit(y)
    
        if parallel == 0: ###   RUN IN SERIAL 
            for doc in docs: 
                LyricsMiner.searchTweet(doc)
            
        else:             ###   RUN IN PARALLEL 
            with concurrent.futures.ProcessPoolExecutor() as executor:
                executor.map(LyricsMiner.searchTweet, docs)
       
        if x%(y*50) == 0 :  ###   Write a log after every x document
            with open('logs/secondaryLog.log', 'a') as logfile:
                now = datetime.now()
                dt_string = now.strftime("%d/%m/%Y %H:%M:%S")
                logfile.write('{}: {} scanned {} documents \n'.format(DB.tablename, dt_string, x)) 
    
        x = x + y
    
if __name__ == "__main__":
    main()
                                

