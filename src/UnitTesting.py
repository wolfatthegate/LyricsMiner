import pymongo
import sys
import Helper

def main():
    artist = Helper.findArtistName('coke boys')
    print(artist)
    
    keywords = Helper.findDrugKeywords('I do heroine, cocaine and weed. ')
    print(keywords)
    
if __name__ == "__main__":
    main()