import logging
import pymongo
import concurrent.futures
import LyricsMiner
import DB

from datetime import datetime

def main():
    
    ### Initialization
    myquery = {"score": {"$gt": 0.49}}

    noofdoc = DB.tweetTbl.find(myquery).count() #find() method returns a list of dictionary
    print(noofdoc)
    quit()
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
       
        if x%(y*100) == 0 :  ###   Write a log after every x document
            with open('logs/'+ DB.tablename +'Log.log', 'a') as logfile:
                now = datetime.now()
                dt_string = now.strftime("%m/%d/%Y %H:%M:%S")
                logfile.write('{}: {} scanned {} documents \n'.format(DB.tablename, dt_string, x)) 
    
        x = x + y
    
if __name__ == "__main__":
    main()
                                

