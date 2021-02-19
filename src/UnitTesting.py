import pymongo
import sys
import Helper
import BST

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
    
    if Helper.findCommonTerms('this s lit'): 
        print('found')
    
if __name__ == "__main__":
    main()