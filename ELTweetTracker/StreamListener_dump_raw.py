import tweepy
import sys
import pandas as pd
#import numpy as np
import time

tweet=[]
users=[]
ids=[]
id_strs=[]
hash_ts=[]
user_urls=[]
rt_counts=[]
reply_ids=[]
reply_id_strs=[]

consumer_key= 'cVDSZUobYYtTSyZs7kCbpn0Bj'
consumer_secret= 'GPzGGWuqqQ2ldGHr207kx3ARtNDgFeqq9GBY8PKyN4T7GG0ohN'

#Setting connection of app to Twitter
auth = tweepy.OAuthHandler(consumer_key, consumer_secret)


class MyStreamListener(tweepy.StreamListener):
    
    def __init__(self, api=None):
        self.api = api or tweepy.API()
        self.stream_count=0
        #self.tweetStream= open("tweetTrack.csv", "a")


    def on_data(self,data):
        if(self.stream_count<100):
            
            try:
                saveFile = open('twitdb.csv','a')
                saveFile.write(data)
                saveFile.write('\n')
                saveFile.close()
                return True

            except BaseException:
                print('failed on data part')
                time.sleep(7)
            

    def on_error(self, status_code):
        print (status_code)
        if status_code == 420:
            #returning False in on_data disconnects the stream
            return False

        
#Dynamically fetch access token

print ('1!!')

access_token= '162034810-browxnFmu1BwMl98JJC6hOMDmXy9Sgpo1bsvIez4'
access_token_secret= 'AcRXJuIRizc298MLmWD1vWuhzRlMzqnaLiywJcNaTqAtx'

auth.set_access_token(access_token, access_token_secret)
auth.secure = True
print ('2!!')

api = tweepy.API(auth)
print ('3!!')



myStreamListener = MyStreamListener()
myStream = tweepy.Stream(auth = api.auth, languages=["en"], listener=myStreamListener)
print ('4!!')

myStream.filter(track=['Hillary Clinton'])


