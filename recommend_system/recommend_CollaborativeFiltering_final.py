import pandas as pd
import  pymysql 
from math import sqrt

def get_logs():
    try:
        #conn  =  pymysql.connect(host = '10.120.14.3', user = 'root', passwd = "Passw0rd",db="project_line") 
        conn  =  pymysql.connect(host = '10.120.14.110', user = 'root', passwd = "Qqqq@123",db="DB102",charset='utf8')
        cur  =  conn.cursor() 
        cur.execute( "select lineID, time, tag_g from click_log order by lineID, time desc;" ) 
        df = pd.DataFrame(cur, columns=["lineID", "time", "tag_g"])
        return df
    finally:
        if cur: cur.close() 
        if conn: conn.close()

def weighting(user_dict:dict) -> list:
    """從log算出分數，並編排成預處理的格式"""
    result_list = [] 
    for key in user_dict.keys():
    # 按照點擊文章時間加權(ex: 最新3篇*1, 次新3篇*0.5, 再次三篇*0.2)
    # 相同用戶依照文章tag加總 
        wt_dict = {}
        for i in range(len(user_dict[key])):
            k = user_dict[key][i][0]
            v = (1 if i in [0,1,2] else 0.5 
                    if i in [3,4,5] else 0.2 )
            wt_dict[k] = v if k not in wt_dict else wt_dict[k]+v
            if i == 8: break
        # print(f"user {key}: ",wt_dict)
        for kk in wt_dict.keys():
            result_list.append((key, (kk, wt_dict[kk])))
    return result_list

# 資料整理需要的function
def remove_duplicates(userRatings):
    tag1, rating1 = userRatings[1]
    tag2, rating2 = userRatings[2]
    return tag1 < tag2

def make_tag_pairs(userRatings):
    (tag1, rating1) = userRatings[1]
    (tag2, rating2) = userRatings[2]
    return ((tag1, tag2), (rating1, rating2))

# 資料整理結束，算相似度分數與評分次數
def compute_score(ratingPairsList):
    ratingPairs = ratingPairsList[1]
    numPairs = 0
    sum_xx = sum_yy = sum_xy = 0
    for ratingX, ratingY in ratingPairs:
        sum_xx += ratingX * ratingX
        sum_yy += ratingY * ratingY
        sum_xy += ratingX * ratingY
        numPairs += 1

    numerator = sum_xy
    denominator = sqrt(sum_xx) * sqrt(sum_yy)
    score = 0
    if (denominator):  # 避免分母為0
        score = (numerator / (float(denominator)))
    
    return ratingPairsList[0], (score, numPairs)

# 設定條件，搜尋相似文章
def search_similar(group, tag_pair_with_scores_list, s_threshold=0.85, a_threshold=1):
    similarity_threshold = s_threshold
    appearence_threshold = a_threshold
    tag = group
    result = list(filter(lambda x: (x[0][0] == tag or x[0][1] == tag)
                            and x[1][0] > similarity_threshold 
                            and x[1][1] > appearence_threshold,
                            tag_pair_with_scores_list))
    if len(result) != 0:
        return tuple(i[0][1] for i in result)
    else:
        # print("Similar tags(article type) not found!")
        return None

# out put: 推 6 篇文章，輸出成 dataframe
# 更改相似度要求與最少點擊人數
def get_recommend_article(log_tag, tag_pair_with_scores_list, s=0.85, a=1):
    try:
        #conn  =  pymysql.connect(host = '10.120.14.3', user = 'root', passwd = "Passw0rd",db="project_line")
        conn  =  pymysql.connect(host = '10.120.14.110', user = 'root', passwd = "Qqqq@123",db="DB102",charset='utf8')
        cur  =  conn.cursor() 
        similar = search_similar(log_tag, tag_pair_with_scores_list, s_threshold=s, a_threshold=a)  # 搜尋最相近的文章tag
        if similar is not None:
            if len(similar) == 1:
                cur.execute( f"select * from healthy_group28 where tag_g in ({similar[0]});" ) 
            else:
                cur.execute( f"select * from healthy_group28 where tag_g in {similar};" ) 
        else:
            cur.execute( f"select * from healthy_group28 where tag_g in ({log_tag});" )
        df = pd.DataFrame(cur, columns=["title","URL","Published date","keywords","KW","picture","group"])
        return df
    finally:
        if cur: cur.close() 
        if conn: conn.close()


def recommend_six_article(tag_tag: int):
    # 讀取log
    df = get_logs()

    # 將log 轉換成dict 準備輸入weighting
    user_dict = {}
    for line in range(len(df)):
        userid, logtime, tag= list(df.loc[line])
        if userid not in user_dict:
            user_dict[userid]=[]
        user_dict[userid].append((tag, logtime))
	"""these are for print out pretty info
    # def pretty(d, indent=0):
    #     for key, value in d.items():
    #         print('\t' * indent + str(key))
    #         if isinstance(value, dict):
    #             pretty(value, indent+1)
    #         else:
    #             print('\t' * (indent+1) + str(value))
    # pretty(user_dict, 1)
	"""
    # 得到['id', ('tag','score')]格式
    raw_df = pd.DataFrame(weighting(user_dict))
    raw_df.columns = ['id', 'group & score']
    print(raw_df)
    # 開始資料整理
    self_joined_ratings = raw_df.merge(raw_df, on='id', how='inner')

    self_joined_ratings_list = self_joined_ratings.values.tolist()
    distinct_self_joined_ratings_list = list(filter(remove_duplicates, self_joined_ratings_list))

    tag_pairs_list = list(map(make_tag_pairs, distinct_self_joined_ratings_list))

    # groupByKey
    tag_pairs_list.sort(key=lambda x:x[0])
    tag_pair_ratings_list = []
    new_in_list = []
    prev_a = None
    for t in tag_pairs_list:
        a, b = t[0], t[1]
        if prev_a is None or a == prev_a:
            new_in_list.append(b)
        else:
            tag_pair_ratings_list.append((prev_a, new_in_list))
            new_in_list = [b]
        prev_a = a
    tag_pair_ratings_list.append((a, new_in_list))
    # print(tag_pair_ratings_list)

    # 算分數
    tag_pair_with_scores_list = list(map(compute_score, tag_pair_ratings_list))

    # 得到兩篇推薦的文章 (adf)
    adf = pd.DataFrame(get_recommend_article(tag_tag, tag_pair_with_scores_list)).sample(2)

    # 得到兩篇相同tag 文章 (bdf)
    try:
        #conn  =  pymysql.connect(host = '10.120.14.3', user = 'root', passwd = "Passw0rd",db="project_line")
        conn  =  pymysql.connect(host = '10.120.14.110', user = 'root', passwd = "Qqqq@123",db="DB102",charset='utf8')
        cur  =  conn.cursor() 
        cur.execute( f"select * from healthy_group28 where tag_g in ({tag_tag});" )  
        bdf = pd.DataFrame(cur, columns=["title","URL","Published date","keywords","KW","picture","group"]).sample(2) 
    finally:
        if cur: cur.close() 
        if conn: conn.close()
            
    # 得到兩篇隨機文章 (cdf)
    try:
        #conn  =  pymysql.connect(host = '10.120.14.3', user = 'root', passwd = "Passw0rd",db="project_line")
        conn  =  pymysql.connect(host = '10.120.14.110', user = 'root', passwd = "Qqqq@123",db="DB102",charset='utf8')
        cur  =  conn.cursor() 
        cur.execute( f"select * from healthy_group28;" )  
        cdf = pd.DataFrame(cur, columns=["title","URL","Published date","keywords","KW","picture","group"]).sample(2)
    finally:
        if cur: cur.close() 
        if conn: conn.close()
            
    return (pd.concat([bdf,adf,cdf],axis=0))

if __name__ == "__main__":
    import random
    print(recommend_six_article(random.randint(0,27))[["keywords","group"]])

