import logging
import pymongo
import concurrent.futures
import LyricsMinerv2
import DBv2
from pymongo import UpdateOne
import time

from datetime import datetime

def main():
    
    ### Initialization
    ### get the number of document which are already scanned.    
    myquery = {}
    count = 0
     
    lycount = DBv2.lyricsMatchTbl.find().count()
    cursor = DBv2.tweetTbl.find(myquery).skip(lycount) #find() method returns a list of dictionary
    
    while cursor.alive: 
        try: 
            doc = cursor.next()
            LyricsMinerv2.searchTweet(doc)
            count+=1
            
            if count%(500) == 0 :  ###   Write a log after every x document
                with open('logs/'+ DBv2.tablename +'Log.log', 'a') as logfile:
                    now = datetime.now()
                    dt_string = now.strftime("%m/%d/%Y %H:%M:%S")
                    logfile.write('{}: {} scanned {} documents from table {}\n'.format(DBv2.tablename, dt_string, count, DBv2.tablename)) 
                    print('{}: {} scanned {} documents from table {}\n'.format(DBv2.tablename, dt_string, count, DBv2.tablename))    
            
        except StopIteration:
            print('error in mainv2')
            time.sleep(1)
            
    print('total number of docs inserted - {}'.format(count))
    print('done..')

if __name__ == "__main__":
    print('start main - scanning {}'.format(DBv2.tablename))
    start = time.time()
    main()
    stop = time.time()
    print("total time: {}".format(stop-start))
