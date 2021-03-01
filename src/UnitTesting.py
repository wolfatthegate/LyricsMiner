import pymongo
import sys
import Helper
import BST
import TextCleaner

def bstTest():
    bst = BST.Node(None)
    bst.insert('hi')
    bst.insert('Aye')
    
    bst.PrintTree()
    if bst.findval('hi'):
        print('found hi')
    if bst.findval('hii'):
        print('found hi')
        
def main():
#     artist = Helper.findArtistName('coke boys')
#     print(artist)
#     
#     keywords = Helper.findDrugKeywords('I do heroine, cocaine and weed. Nigga ')
#     print(keywords)
#     
#     bstTest()
    cleaner = TextCleaner.TextCleaner()
    text = cleaner.clean('Take a shot https://t.co/6iGOyky3Eb')
    if Helper.findCommonTerms(text.lower()): 
        print('found')
    
if __name__ == "__main__":
    main()