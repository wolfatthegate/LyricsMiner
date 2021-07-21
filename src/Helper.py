import nltk
import re
import BST
import json
import CommonTermsDict as ctd
import blast
from gensim.parsing.preprocessing import remove_stopwords

dkTree = BST.Node(None) #drug keywords in BST
dkList = []
CUSTOMSTOPWORDS = []

with open("keywords/DrugListShort.txt", "r") as file:
    for el in file: 
        dkTree.insert(str(el).strip())
        dkList.append(str(el))

stTree = BST.Node(None) #substracted terms in BST
with open("keywords/SubstractTerms.txt", "r") as file: 
    for el in file: 
        stTree.insert(str(el).strip())
        
ctTree = BST.Node(None) #common terms in BST
with open("keywords/CommonTerms.txt", "r") as file: 
    for el in file: 
        ctTree.insert(str(el).strip())
        
otTree = BST.Node(None) #ommitted terms in BST
with open("keywords/OmittedTerms.txt", "r") as file: 
    for el in file: 
        otTree.insert(str(el).strip())
        
with open("custom_stopwords2.txt", "r", encoding="utf-8") as file:
    for el in file: 
        CUSTOMSTOPWORDS.append(el) 
        
def remove_stopWords_custom(str):
    return " ".join(w for w in str.split() if w not in CUSTOMSTOPWORDS)

def findArtistName(str):
    artistList = ['young thug', 'coke boys']

    for artist in artistList:
        str = str.lower()               
        if (str.find(artist)!=-1):
            return artist
    return ''

def findCommonTerms(tokenized_str, str):
    
    blaster = blast.blast()
    
    for token in tokenized_str: 
        if token in ctd.ctDict:
            for el in ctd.ctDict[token]: 
                # compare el with str )
                result = blaster.SMWalignment(el, str, threshold = 0.80)
                if result[2] > 0.70: 
                    return True           
    return False

def findDrugKeywords(tokenized_str):
    
    ### initialization

    keywordList = []
    commonList = []
    found = False
    count = 0
    
    for tokenized_word in tokenized_str:
        found = dkTree.findval(tokenized_word)
        if found == True: 
            tokenized_word = re.sub(r'ing$', 'in', tokenized_word)
            keywordList.append(tokenized_word.lower())  
             
    if keywordList:                       
        for word_token in tokenized_str: 
            found = stTree.findval(word_token.lower())
            if found == False:  
                word_token = re.sub(r'ing$', 'in', word_token)            
                keywordList.append(word_token.lower()) 
                          
    for word_token in tokenized_str: 
        found = otTree.findval(word_token.lower())
        if found == True:  
            count = count + 1                        
            commonList.append(word_token.lower()) 
    if count > 1: 
        keywordList.extend(commonList)

    keywordList = list(dict.fromkeys(keywordList))

    return keywordList

def findDrugKeywordsForRawTweets(tokenized_str):

    
    for tokenized_word in tokenized_str:
        for dk in dkList: 
            result = blaster.SMWalignment(tokenized_word, dk, threshold = 0.75)
            if result[2] > 0.65: 
                return True 
    return False



