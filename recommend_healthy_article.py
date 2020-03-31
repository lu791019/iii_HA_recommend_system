import pandas as pd
import re
import numpy as np
from sklearn.feature_extraction.text import CountVectorizer
from jieba import cut
import pickle


with open('health_all_kmeans_NB.pickle', 'rb') as handle:
    clf = pickle.load(handle)
with open('health_all_countvectorizer.pickle', 'rb') as handle:
    vec = pickle.load(handle)
# 開模型
df = pd.read_csv("healthy_article_all_group.csv")
# 讀出分好群的文章
def article_cut(s):
    string = re.sub("[\s+\.\!\/_,$%^*(+\"\']+|[+——！，。？、~@#￥%……&*（）【／】]+", "",s)  # 取代所有標點
    return " ".join(cut(string))

# 以下兩個function 網路上找的，用於載入stopword
# 網址: https://blog.csdn.net/u012052268/article/details/77825981#52-%E8%BD%BD%E5%85%A5%E5%81%9C%E7%94%A8%E8%AF%8D%E8%A1%A8
def stopwordslist(filepath):  
    stopwords = [line.strip() for line in open(filepath, 'r', encoding='utf-8').readlines()]  
    return stopwords  

def seg_sentence(sentence):  
    sentence_seged = article_cut(sentence)
    stopwords = stopwordslist('stopword.txt')  # 这里加载停用词的路径  
    outstr = ''  
    for word in sentence_seged:  
        if word not in stopwords:  
            if word != '\t':  
                outstr += word  
                outstr += " "  
    return outstr

p = input("您覺得您最近身體狀況如何?")
pcut = article_cut(p)
pcut = seg_sentence(pcut)
pvec = vec.transform([pcut])
# 切割字串，並轉換成向量

pre = clf.predict(pvec)[0]
# 使用模型給定分群

from random import sample
for i in sample(list(df["title"][df.group == pre].index),5):
    print(df["title"][df.group == pre][i])
    print(df["URL"][df.group == pre][i])
# 根據輸入字串推薦五篇文章

