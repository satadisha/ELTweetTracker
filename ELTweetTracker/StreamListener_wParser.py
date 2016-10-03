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

    def on_status(self, status):
        if(self.stream_count<10000):               
            tweetText=bytes(str(status.text).encode("utf-8"))
            tweetText = tweetText.decode('utf-8')
           # dbg print(tweetText)
            userName=status.user.screen_name
            id = status.id
            #what is id str?
            id_str = status.id_str
            date = status.created_at
            #hash_t= status.entities.hashtags.text
            user_url = status.user.url
            rt_count= status.retweet_count
            reply_id_str= status.in_reply_to_status_id_str
            
            #print tweet
            tweet.append(tweetText)
            users.append(userName)
            ids.append(id)
            user_urls.append(user_url)
            rt_counts.append(rt_count)
            reply_id_strs.append(reply_id_str)
            
            self.stream_count+=1
            
            return True
        
        else:
            #self.tweetStream.close()
            return False
        
    def on_error(self, status_code):
        print (status_code)
        if status_code == 420:
            #returning False in on_data disconnects the stream
            return False

        
#Dynamically fetch access token
#dbg
print ('1!!')

access_token= '162034810-browxnFmu1BwMl98JJC6hOMDmXy9Sgpo1bsvIez4'
access_token_secret= 'AcRXJuIRizc298MLmWD1vWuhzRlMzqnaLiywJcNaTqAtx'

auth.set_access_token(access_token, access_token_secret)
auth.secure = True
#dbg
print ('2!!')

api = tweepy.API(auth)
#dbg
print ('3!!')



myStreamListener = MyStreamListener()
myStream = tweepy.Stream(auth = api.auth, languages=["en"], listener=myStreamListener)
#dbg
print ('4!!')

myStream.filter(track=['Hillary Clinton'])

d= {'Tweet':tweet,
     'UserName':users,'ID':ids,'User URLS':user_urls,'# RT':rt_counts,'Replier Id Strs':reply_id_strs}
df= pd.DataFrame(d)


df.to_csv('tweetTrack.csv',header=False,mode= 'a')


