import pymongo
import sys
import Helper
import BST
import TextCleaner
import blast
import logging
import LyricsMiner
import DB
import time

from datetime import datetime


def bstTest():
    bst = BST.Node(None)
    bst.insert('hi')
    bst.insert('Aye')   
    bst.PrintTree()
    if bst.findval('hi'):
        print('found hi')
    if bst.findval('hii'):
        print('found hi')

def minerTest():
    start = time.time()
        
    myquery = {"tweetID":"782100079951745024"}
    docs = DB.tweetTbl.find(myquery)

    for doc in docs: 
        LyricsMiner.searchTweet(doc)

    stop = time.time()
    print("total time: {}".format(stop-start))
    
def main():
    minerTest()
    
if __name__ == "__main__":
    main()