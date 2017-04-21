
# coding: utf-8

# In[1]:

import pandas as pd
import numpy as np
from sklearn.ensemble import RandomForestClassifier
from sklearn import svm


class Classifier_SVM():

    def __init__(self,df):

        train = df
        cols = ['candidate','freq', 'length', 'cap', 'start_of_sen','abbrv','all_cap','is_csl','title','has_no','date','is_apostrp','has_inter_punct','ends_verb',
            'ends_adverb','change_in_cap','topic_ind','@mention','z_score']

        colsRes = ['CLASS']
        for col in cols[2:]:
            train[col]=train[col].apply(pd.to_numeric)
        for index,row in train.iterrows():
            for i in range(2,len(cols)-2):
                 row[cols[i]]=float(row[cols[i]])/float(row['freq'])
		
	#train.insert(len(x_test.columns),column_name,pred_class)

        trainArr = train.as_matrix(cols[1:]) #training array
        trainRes = train.as_matrix(colsRes) # training results

        print(trainRes)

        self.clf = svm.SVC(probability=True)
        self.clf.fit(trainArr, trainRes) 


    #output = df with probabilities 
    def test(self,x_test2,g):
        x_test=self.filter(x_test2)
        column_name=str(g)+"th Iteration"
        cols = ['candidate','freq', 'length', 'cap', 'start_of_sen','abbrv','all_cap','is_csl','title','has_no','date','is_apostrp','has_inter_punct','ends_verb',
            'ends_adverb','change_in_cap','topic_ind','@mention','z_score']

        for col in cols[2:]:
            x_test[col]=x_test[col].apply(pd.to_numeric)

        for index,row in x_test.iterrows():
            for i in range(2,len(cols)-2):
                 row[cols[i]]=float(row[cols[i]])/float(row['freq'])


        colsRes = ['CLASS']
        testArr= x_test.as_matrix(cols[1:])
        testRes = x_test.as_matrix(colsRes)
        pred_prob=self.clf.predict_proba(testArr)


        #0.6 rules.
        # pred_class=[]
        # # for idx, cl in enumerate(pred_prob):
        # #     if pred_prob[idx][1]>=0.6:
        # #         class_x=1
        # #     elif pred_prob[idx][1]<0.6:
        # #         class_x=0


        row_holder=[]
        for index, row in x_test.iterrows():
            #print(row['candidate']+" "+str(pred_prob[index][1]))
            row_holder.append([row['candidate'],str(pred_prob[index][1])])


        #print(row_holder)
        #x_test.insert(len(x_test.columns),column_name,pred_class)
        #print(x_test)

        return row_holder

    def filter(self,x_test):
        x_test['z_score']=x_test['z_score'].apply(pd.to_numeric)
        x_test2=x_test.loc[x_test['z_score']> 0.4]
        #print(x_test2['candidate'])
        row_holder=[]
        for index, row in x_test2.iterrows():
            #print(row['candidate']+" "+str(pred_prob[index][1]))
            row_holder.append([row['candidate'],str(1)])
        return row_holder
        #return x_test2
 





# # In[7]:

# pred_prob


# # In[8]:

# pred_class=clf.predict(testArr)
# print(pred_class)


# # In[9]:

# testRes


# # In[10]:

# count=0


# # In[11]:

# for i in range(len(pred_class)):
#     if pred_class[i]==testRes[i]:
#         count+=1


# # In[12]:

# count


# # In[13]:

# len(pred_class)


# # In[14]:

# float(count)/len(pred_class)


# # In[22]:

# prob_holder=[]
# for idx, cl in enumerate(pred_prob):
#     prob_holder.append(pred_prob[idx][1])
# #x_test.insert(len(x_test.columns),'pred_prob',pred_prob[1])
# #print (pred_prob[,1])
# #x_test.insert(1, 'bar', df['one'])


# # In[23]:

# x_test.to_csv("svm_prob.csv", sep=';', encoding='utf-8')



# # In[24]:

# random_forest_logistic=pd.read_csv("random_forest_logistic.csv",delimiter=";")


# # In[25]:

# random_forest_logistic


# # In[26]:

# prob_holder=[]
# for idx, cl in enumerate(pred_prob):
#     prob_holder.append(pred_prob[idx][1])
# #x_test.insert(len(x_test.columns),'pred_prob',pred_prob[1])
# #print (pred_prob[,1])
# #x_test.insert(1, 'bar', df['one'])


# # In[27]:

# random_forest_logistic.insert(len(random_forest.columns),'svm_with_prob',prob_holder)
# print random_forest_logistic


# # In[29]:

# random_forest_logistic.to_csv("random_forest_logistic_svm_FINAL.csv", sep=';', encoding='utf-8')


# # In[34]:

# class_x=0
# TP=0
# TN=0
# FP=0
# FN=0

# for idx, cl in enumerate(pred_prob):
#     #print pred_prob[idx][1]
#     #if pred_prob[idx][1]>0.6:
#        # class_x=1
#     #elif pred_prob[idx][1]<=0.6:
#       #  class_x=0
#     class_x = pred_class[idx]   

#     if (class_x ==testRes[idx]) and class_x==1 :
#         TP+=1
#     elif (class_x ==testRes[idx]) and class_x==0 :
#         TN+=1
#     if class_x ==  1 and testRes[idx]==0:
#         FP+=1
#     if class_x ==  0 and testRes[idx]==1:
#         FN+=1


# # In[35]:

# print TP,TN,FP,FN


# # In[ ]:



