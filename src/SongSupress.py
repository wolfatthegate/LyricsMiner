### This program is to add artist and year column to existing filtered tweets. 
### Author - Waylon Luo

import pymongo
import sys
import Helper
import nltk
import TextCleaner
import blast
import Normalizer
import DB
from gensim.parsing.preprocessing import remove_stopwords

cleaner = TextCleaner.TextCleaner()
normalizer = Normalizer.Normalizer()
blaster = blast.blast()

def setNewValue(song):
    
    newvalue = { '$set': {'song': song}}
    return newvalue

def main(): 

    ### DB connection
    table_names = []
    with open('TableName.txt', 'r') as f: 
        table_names = f.read().split()
        
    print(table_names)
    
    quit
    # table_name = sys.argv[1] #'2017-10-week-1'
    
    myclient = pymongo.MongoClient("mongodb://localhost:27017/")
    twitterDB = myclient['TwitterData']
    lyricsTbl = myclient['LyricsDB']['Lyrics']  
    
    for table_name in table_names: 
        
        tweetTbl = twitterDB[table_name]
    
        ### Query    
        docs = tweetTbl.find({'score': {'$gt': 0.49}})
        print(docs.count())
        with open(table_name+'_artist_stats.txt', 'a') as file: 
            
            for doc in docs: 
                obj_id = {'_id': doc['_id']}
                song = doc['song']
                tweet = doc['tweet'] 
                tweet = tweet.replace('\n', ' ')
                
                if song.startswith(', '): 
                    song = song.replace(', ', '')
                    tweetTbl.update_one(obj_id, setNewValue(song))               
                
if __name__ == "__main__":
    main()

