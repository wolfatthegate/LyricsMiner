
### Improved version of Lyrics-Blast-Live
### The keywords are stored in BST 
### thus the searches will be much faster
### created on Dec 28, 2020
###  Author - Waylon Luo

import TextCleaner
import pymongo
import nltk
import blast
import sys
import logging
import Normalizer
import re
import warnings
import Helper
import DB
warnings.filterwarnings("ignore")

from datetime import datetime
# from spellchecker import SpellChecker

cleaner = TextCleaner.TextCleaner()
normalizer = Normalizer.Normalizer()
blaster = blast.blast()

def printResult(result, songtitle, doc_id):

    logging.info('tweet: ' + result[0], extra={'_id': doc_id})
    logging.info('query: ' + result[1], extra={'_id': doc_id})
    logging.info('Score: ' + str(round(result[2],2)) + '/1.0 (' + str(result[3]) + ' words matched)' + 'in song: ' + songtitle, extra={'_id': doc_id})
    logging.info(' first half score: ' + str(round(result[4], 2)), extra={'_id': doc_id})
    logging.info('second half score: ' + str(round(result[5], 2)), extra={'_id': doc_id})
    
    suggestions = []
    suggestions.append('tweet: ' + result[0])
    suggestions.append('query: ' + result[1])

    suggestions.append('(' + str(result[3]) + ' words matched)')
    
    return suggestions

def setNewValue(score, suggestions, song, type, x_add):
    newvalue = { '$set': {'score': score , \
                          'suggestions': suggestions, \
                          'song': song, \
                          'type': type, \
                          'x_add': x_add}}
    return newvalue

def searchTweet(doc):
    tweet = doc['tweet']
    docID = {'_id': doc['_id']}
    
    if not tweet: 
        now = datetime.now()
        newvalue = setNewValue(0.00, 'empty tweet', '', DB.type, now.strftime("%Y-%m-%d %H:%M:%S"))
        DB.tweetTbl.update_one(docID, newvalue)
        return 0 
    tweet = cleaner.clean(tweet)
    
    '''
    Find the common terms in a smaller database. 
    '''
    
    if Helper.findCommonTerms(tweet.lower()): 
        now = datetime.now()
        newvalue = setNewValue(0.00, 'common term', '', DB.type, now.strftime("%Y-%m-%d %H:%M:%S"))
        DB.tweetTbl.update_one(docID, newvalue)
        return 0         
    
    '''
    Skip searches for tweets less than 4 words
    '''
    
    tokenizedTwt = nltk.word_tokenize(tweet)
    if len(tokenizedTwt) < 5:
        now = datetime.now()
        newvalue = setNewValue(0.00, 'Tweet too short', '', DB.type, now.strftime("%Y-%m-%d %H:%M:%S"))
        DB.tweetTbl.update_one(docID, newvalue)
        return 0 
    
    '''
    ### This part will be skipped as we will not be finding artist name anymore. 
    
    artistFound = Helper.findArtistName(tweet)
    if (artistFound != ''):
        now = datetime.now()
        newvalue = setNewValue(1.00, 'artist found: ' + artistFound, '', DB.type, now.strftime("%Y-%m-%d %H:%M:%S"))
        DB.tweetTbl.update_one(docID, newvalue)
        return 0
    '''
    
    # normalizer simplify the words 
    # that spelling checkers cannot handle.

    tweet = normalizer.normalize(tweet)
    query_list = Helper.findDrugKeywords(tweet)
    
    if not query_list:
        now = datetime.now()
        newvalue = setNewValue(0.01, 'filtered', '', DB.type, now.strftime("%Y-%m-%d %H:%M:%S"))
        DB.tweetTbl.update_one(docID, newvalue)
        return 0
    
    return 0
    
    go_to_next_tweet = False
    titleMatched = False
    savedline = ''
    combined_lines = ''
    song = ''
    suggestions = []
    _suggestions_ = ''
    
    keyword_list_title = []
    keyword_list_lyric = []
    
    for query_word in query_list:
        keyword_list_title.append({"title": {"$regex": query_word, "$options": "-i"}})
        keyword_list_lyric.append({"lyrics1": {"$regex": query_word, "$options": "-i"}})
    
    mytitlequery = {'$and': keyword_list_title}
    mytitle = DB.lyricTbl.find(mytitlequery)
    
    if len(doc['tweet']) > 60:
        temp_str = doc['tweet'][0:60] + ' ... '
    else:
        temp_str = doc['tweet']
    
    logging.info(mytitlequery, extra = {'_id': doc['_id']})
    logging.info('searching for ' + temp_str + ' ' + str(mytitle.count()) + ' possible titles found.', extra = {'_id': doc['_id']})
    
    for eachtitle in mytitle: 
        title = cleaner.clean(eachtitle['title']) 
        
        result = blaster.SMWalignment(tweet.lower(), title.lower(), DB.threshold)
            
        if round(result[2],2) > 0.70: 
            logging.info('title found', extra = {'_id': doc['_id']})
            titleMatched = True
            suggestions = suggestions + printResult(result, eachtitle['title'], doc['_id'])   
            
            for s in suggestions: 
                _suggestions_ = _suggestions_ + '\n' + s
                
            now = datetime.now()
            newvalue = setNewValue(round(result[2],2), _suggestions_, eachtitle['title'], DB.type, now.strftime("%Y-%m-%d %H:%M:%S"))
            DB.tweetTbl.update_one(docID, newvalue)
            break
    
    if titleMatched == True:
        return 0
      
    mylyricquery = {'$and': keyword_list_lyric} # query keyword
    mylyrics = DB.lyricTbl.find(mylyricquery)  #find in lyrics database

    logging.info(mylyricquery, extra= {'_id': doc['_id']})
    logging.info(str(mylyrics.count()) + ' possible lyrics found.', extra= {'_id': doc['_id']})
    
    maxScore = 0.0
    
    for eachlyrics in mylyrics: #loop through mylyrics list
        
        # initialize variables
        go_to_next_song = False  
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
            
            combined_string = past_line_2 + ' ' + past_line_1 + ' ' + past_line_0
            for query_word in query_list:
                if query_word.lower() in combined_string.lower():
                    continue_test = True
                        
            if continue_test == True:

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
                    
                if maxScore > DB.mid_score and maxMatch > 3 or maxMatch > 5: 
                    suggestions = suggestions + printResult(result, eachlyrics['title'], doc['_id'])
                    song = song + ' ' + eachlyrics['title']                   
                    
                    go_to_next_song = True # go to next song
                
                if maxScore > DB.high_score:
                    suggestions = suggestions + printResult(result, eachlyrics['title'], doc['_id'])
                    suggestions.append('found the song')
                    song = song + ' ' + eachlyrics['title']
                    logging.info('found the song', extra= {'_id': doc['_id']})             
                    go_to_next_tweet = True
                    break
            
            if go_to_next_song == True: 
                break
             
        if go_to_next_tweet == True:
            break
        
    suggestions.append('Score: ' + str(round(maxScore,2)) + '/1.0' )
    suggestions = list(dict.fromkeys(suggestions))
    
    for s in suggestions: 
        _suggestions_ = _suggestions_ + '\n' + s 
    
    now = datetime.now()     
    newvalue = setNewValue(round(maxScore,2), _suggestions_, song, DB.type, now.strftime("%Y-%m-%d %H:%M:%S"))
    DB.tweetTbl.update_one(docID, newvalue)
    
