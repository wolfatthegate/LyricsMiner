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

def setNewValue(artist, year):
    
    newvalue = { '$set': {'artist': artist, 'year': year, 'type': 2}}
    return newvalue

def findSong(doc, songsInfo):
    
    tweet = doc['tweet']
    filtered_str = remove_stopwords(tweet)
    filtered_str = Helper.remove_stopWords_custom(filtered_str)
    tokenized_str = nltk.word_tokenize(filtered_str)
    
    artist = ''
    year = ''
    go_to_next_tweet = False
    
    for eachlyrics in songsInfo:
        # initialize variables 
        continue_test = False  

        # read each line of lyrics  
        past_line_2 = ''
        past_line_1 = ''
        past_line_0 = ''
        combined_string = ''
        
        maxScore = 0.0 #reset
        maxMatch = 0
  
        for eachline in eachlyrics['lyrics1'].splitlines():
            
            eachline = cleaner.clean(eachline)
            
            past_line_2 = past_line_1
            past_line_1 = past_line_0
            past_line_0 = eachline

            # perform blast Search                    
            result = blaster.SMWalignment(tweet, past_line_1.lower(), DB.threshold)
            maxScore = result[2]
            maxMatch = result[3]
                
            if result[6] == True:
                followupLine = past_line_1 + ' ' + past_line_0
                followUpResult = blaster.SMWalignment(tweet, followupLine.lower(), DB.threshold)
                maxScore = max(result[2], followUpResult[2], maxScore)
                maxMatch = max(result[3], followUpResult[3], maxMatch)
                
            if result[7] == True:
                stepBackLine = past_line_2 + ' ' + past_line_1
                stepBackResult = blaster.SMWalignment(tweet, stepBackLine.lower(), DB.threshold)
                maxScore = max(result[2], stepBackResult[2], maxScore)
                maxMatch = max(result[3], stepBackResult[3], maxMatch)
            
            if maxScore > DB.high_score:
                song = eachlyrics['title']
                artist = eachlyrics['name']
                year = eachlyrics['year']
                go_to_next_tweet = True
                break
             
        if go_to_next_tweet == True:
            break
    
    return artist, year

def main(): 

    ### DB connection
    table_names = []
    with open('Tablename2.txt', 'r') as f: 
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
        docs = tweetTbl.find({'score': {'$gt': 0.49}, 'type': 1})
        print(docs.count())
        with open(table_name+'_artist_stats.txt', 'a') as file: 
            
            for doc in docs: 
                obj_id = {'_id': doc['_id']}
                song = doc['song']
                tweet = doc['tweet'] 
                tweet = tweet.replace('\n', ' ')
                
                if song.startswith(', '): 
                    song = song.replace(', ', '')
                    
                songsInfo = lyricsTbl.find({'title': song})

                if songsInfo.count() == 1:  
                    artist = songsInfo[0]['name']
                    year = songsInfo[0]['year']
                    tweetTbl.update_one(obj_id, setNewValue(artist, year))
                
                else: 
                    artist, year = findSong(doc, songsInfo)
                    tweetTbl.update_one(obj_id, setNewValue(artist, year))
                    ### we have to find the lyrics from multiple results
                    ### and pick only one. 
        print('{} finished '.format(table_name))  

    
if __name__ == "__main__":
    main()

