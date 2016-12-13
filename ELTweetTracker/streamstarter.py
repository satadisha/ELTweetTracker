import tweepy
import sys
import pandas as pd
#import numpy as np
import time
import re
from TweetMiner import TweetMiner
from tweepy.utils import import_simplejson
json = import_simplejson()
import time
from threading import Thread
import random
from queue  import Queue
thread_processed=0
stream_count=0

queue = Queue()
consumer_key= 'cVDSZUobYYtTSyZs7kCbpn0Bj'
consumer_secret= 'GPzGGWuqqQ2ldGHr207kx3ARtNDgFeqq9GBY8PKyN4T7GG0ohN'

#Setting connection of app to Twitter
auth = tweepy.OAuthHandler(consumer_key, consumer_secret)

##class ProducerThread(Thread):
##    def run(self):
##        nums = range(5)
##        global queue
##        while True:
##            num = random.choice(nums)
##            queue.put(num)
##            print ("Produced"+ num)
##            time.sleep(random.random())

class ConsumerThread(Thread):
    def run(self):
        global queue
        global stream_count
        global thread_processed
        while True:
            if not queue.empty():
                self.tweet_miner = TweetMiner()
                df = queue.get()
                self.tweet_miner.do_process(df,stream_count)
                queue.task_done()
                thread_processed= thread_processed +1
                print(thread_processed)
               # time.sleep(1)



class MyStreamListener(tweepy.StreamListener):
    
    def __init__(self, api=None):
        self.api = api or tweepy.API()
        self.t_end = time.time()+ 60*7
        global thread_processed
        global queue
        self.normal_count=1
        
        self.tweet_miner = TweetMiner()

        #self.begin_index = pd.read_csv('collection.csv', index_col = 0 ,encoding = 'ISO-8859-1',header=None,skiprows=3)
        #self.stream_count= self.begin_index.index.max()
        self.stream_count=1

        
        self.all_of_them= pd.DataFrame(columns=('id','tweetText','userName','date','hash_str','user_url','rt_count','reply_id'))


    def on_status(self, status):
        if (time.time()>self.t_end):
            print ("1 minutes done.")
            print (thread_processed)
            return 
                    
        if(self.normal_count<99999999999999999999999999999999999999):

            
            tweetText=bytes(str(status.text).encode("utf-8"))
            tweetText = tweetText.decode('utf-8')


            #parsing urls.
            tweetTexts = re.split('http[s]?://(?:[a-zA-Z]|[0-9]|[$-_@.&+]|[!*\(\),]|(?:%[0-9a-fA-F][0-9a-fA-F]))+', tweetText)
            tweetText = ''.join(tweetTexts)
                
            #parsing hashtags.
            tweetTextss = re.split(r"#\w+", tweetText)
            tweetText = ''.join(tweetTextss)



            hash_ts=[]
            for hashtag in status.entities ['hashtags']:
                hash_t=hashtag['text']
                if hash_t:
                    hash_ts.append(hash_t)


            user_urls=[]
            for user_url in status.entities ['urls']:
                user_u=user_url['expanded_url']
                if user_u:
                    user_urls.append(user_u)    
                    

                      
            userName=status.user.screen_name
            id = status.id
            id_str = status.id_str
            date = status.created_at
            user_url = status.user.url
            rt_count= status.retweet_count
            reply_id= status.in_reply_to_status_id
            repiled_id_list= None


            
            # if tweet is a retweet.
            if hasattr(status,'retweeted_status'):
                retweeted_status= status.retweeted_status

                tweetText=bytes(str(retweeted_status.text).encode("utf-8"))
                tweetText = tweetText.decode('utf-8')

                #parsing urls.
                tweetTexts = re.split('http[s]?://(?:[a-zA-Z]|[0-9]|[$-_@.&+]|[!*\(\),]|(?:%[0-9a-fA-F][0-9a-fA-F]))+', tweetText)
                tweetText = ''.join(tweetTexts)


                #parsing hashtags.
                tweetTextss = re.split(r"#\w+", tweetText)
                tweetText = ''.join(tweetTextss)

                


                
                id_str = retweeted_status.id_str
                date = retweeted_status.created_at
                user_url = retweeted_status.user.url
                rt_count= retweeted_status.retweet_count
                reply_id= retweeted_status.in_reply_to_status_id
                repiled_id_list= status.user.id
                
                
                hash_ts=[]
                for hashtag in status.entities ['hashtags']:
                    hash_t=hashtag['text']
                    if hash_t:
                        hash_ts.append(hash_t)


                user_urls=[]
                for user_url in status.entities ['urls']:
                    user_u=user_url['expanded_url']
                    if user_u:
                        user_urls.append(user_u) 
                        

            
            
            hash_str = ', '.join(hash_ts)

            url_str = ', '.join(user_urls)
            


            if(len(hash_ts) > 0):
                if(len(user_urls)>0):
                    
                    
                    d= {'ID':id,'Tweet':tweetText,'UserName':userName,'Publication Time':date,'Hashtags':hash_str,'User URLS':url_str,'# RT':rt_count,'Replier Id Strs':reply_id,'Retweeted Tweet User ID':repiled_id_list}
                    df= pd.DataFrame(d,index=[self.stream_count+1])

                else:
                    
                    d= {'ID':id,'Tweet':tweetText,'UserName':userName,'Publication Time':date,'Hashtags':hash_str,'User URLS':None,'# RT':rt_count,'Replier Id Strs':reply_id,'Retweeted Tweet User ID':repiled_id_list}
                    df= pd.DataFrame(d,index=[self.stream_count+1])

                self.all_of_them=self.all_of_them.append(df)


            else:
                if(len(user_urls)>0):
                
                    d= {'ID':id,'Tweet':tweetText,'UserName':userName,'Publication Time':date,'Hashtags':None,'User URLS':url_str,'# RT':rt_count,'Replier Id Strs':reply_id,'Retweeted Tweet User ID':repiled_id_list}
                    df= pd.DataFrame(d,index=[self.stream_count+1])

                else:
                    d= {'ID':id,'Tweet':tweetText,'UserName':userName,'Publication Time':date,'Hashtags':None,'User URLS':None,'# RT':rt_count,'Replier Id Strs':reply_id,'Retweeted Tweet User ID':repiled_id_list}
                    df= pd.DataFrame(d,index=[self.stream_count+1])
                    

               
                # adding tweets into dataframe container.
                self.all_of_them=self.all_of_them.append(df)
                #queue.put(df)


            print(self.normal_count)


            if(self.normal_count%20 == 0):
                self.all_of_them.to_csv('to_sent_satadisha_7.csv',header=False,mode= 'a')


            self.stream_count+=1
            self.normal_count+=1

            
            #self.tweet_miner.do_process(df,self.stream_count)



            return True
        else:
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
print ('4!!')

myStream.filter(track=['trump','presidential election','clinton','paul ryan','bernie sanders','brexit','theresa may','freekashmir','refugee','olympics'],async=True)
print ('5!!')
#ConsumerThread().start()
#ConsumerThread().start()
#ConsumerThread().start()
#ConsumerThread().start()
#ConsumerThread().start()





