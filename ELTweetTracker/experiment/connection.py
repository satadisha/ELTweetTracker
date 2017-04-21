import SatadishaModule as sm
import Classifier_SVM as cf
import string
from collections import Iterable, OrderedDict
import pandas as pd 
import numpy as np
import csv
from scipy import stats
#tweets.csv #training_new.csv deduplicated_test.csv ,delimiter=';'
df=pd.read_csv('deduplicated_test.csv', header=0, index_col = 0 ,encoding = 'utf-8',delimiter=';')


candidateDB={}
sortedCandidateDB=OrderedDict(sorted(candidateDB.items(), key=lambda t: t[1],reverse=True))
candidateScores={}
#cf.Classifier_SVM()

def writeDict(filename, columns, dictionary):
    with open(filename, 'w') as output_candidate:
        writer = csv.writer(output_candidate)
        writer.writerow(columns)
        for k, v in dictionary.items():
            writer.writerow([k] + v)


def reportScores(scores,g):
    global candidateScores
    for score_row in scores:
        #print(score_row)
        if score_row[0] in candidateScores:
            val=candidateScores[score_row[0]]
            val[g]=score_row[1]
            #print(val)
        else:
            val=[]
            for i in range(13):
                val+=[str(-1)]
            val[g]=score_row[1]
        candidateScores[score_row[0]]=val
    return


def update_row(candidateText, feature_value_list):
    global candidateDB
    key=(((candidateText.lstrip(string.punctuation)).rstrip(string.punctuation)).strip()).lower()
    #key= self.rreplace(self.rreplace(self.rreplace(key,"'s","",1),"’s","",1),"’s","",1)
    if key in candidateDB:
        feature_list=candidateDB[key]
        #feature_list[0]+=1
        for index in range(len(feature_value_list)-1):
            if(index!=1):
                feature_list[index]+=feature_value_list[index]
        feature_list[-1]=feature_value_list[-1]
    else:
        feature_list=[0]*len(feature_value_list)
        #call background process to check for non capitalized occurences
        for index in range(len(feature_value_list)):
            feature_list[index]=feature_value_list[index]
    candidateDB[key] = feature_list
    return

def update_candidate_database(candidateDict):
    global candidateDB
    global sortedCandidateDB
    print("updating database now")
    for key, value in candidateDict.items():
        update_row(key,value)
    #unmerged candidate table and candidateBase
    if(len(sortedCandidateDB)>0):
        sortedCandidateDB.clear()
    print(sortedCandidateDB)
    sortedCandidateDB=OrderedDict(sorted(candidateDB.items(), key=lambda t: t[1],reverse=True))

    #computing z-score of frequency
    frequency_array = np.array(list(map(lambda val: val[0], sortedCandidateDB.values())))
    zscore_array=stats.zscore(frequency_array)
    index=0
    for key in sortedCandidateDB.keys():
        val=sortedCandidateDB[key]+[(str(zscore_array[index])),str(1)]
        index+=1
        sortedCandidateDB[key]=val
        #print(sortedCandidateDB[key])

def todataFrame(sortedCandidateDB):
    row=[]
    row_holder=[]    
    for key, value in sortedCandidateDB.items():
        row=[key]+value
        row_holder.append(row)

    return pd.DataFrame.from_records(row_holder, columns=['candidate','freq','length','cap','start_of_sen','abbrv','all_cap','is_csl','title','has_no','date','is_apostrp','has_inter_punct','ends_verb','ends_adverb','change_in_cap','topic_ind','@mention','z_score','CLASS'])
#,delimiter=";"
train=pd.read_csv("deduplicated_training.csv")

classifier=cf.Classifier_SVM(train)
iterField=[]
for g, batch in df.groupby(np.arange(len(df)) //3000):
    #if(g<2):
    iterField+=['iteration'+str(g)]
    module1 = sm.SatadishaModule(batch)
    candidateDict=module1.extract()
    update_candidate_database(candidateDict)

    #converting dict to dataframe
    sortedCandidateDB_df=todataFrame(sortedCandidateDB)
    #classifier scores=classifier.test(sortedCandidateDB_df,g)
    scores=classifier.filter(sortedCandidateDB_df)
    reportScores(scores,g)
#printing candidate database
fieldnames=['candidate','freq','length','cap','start_of_sen','abbrv','all_cap','is_csl','title','has_no','date','is_apostrp','has_inter_punct','ends_verb','ends_adverb','change_in_cap','topic_ind','@mention','z_score','CLASS']        
writeDict('candidate.csv',fieldnames,sortedCandidateDB)

#printing candidate classification database

scorefieldnames=['candidate']+iterField
writeDict('classification0.4.csv',scorefieldnames,candidateScores)

'''z_scores=[0.2,0.3,0.4,0.5,0.6,0.7,0.8,0.9,1.0]
for z_scoreThreshold in z_scores:
	candidateList=list(filter(lambda key:float(candidateDB[key][-2])>z_scoreThreshold, candidateDB.keys()))
	print (candidateList)'''