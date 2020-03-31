# https://blog.csdn.net/damotiansheng/article/details/44139111
# 重要參考網址


our_rating -> latest_9_click * wt_10_5_2
            [1,0,0,0,3.7,0,0,6.2]

01 e 3.7
01 h 6.2

02 a 2

(a e) (1, 3.7)(2, 4)

    user_movie_ratings = data.map(lambda l: l.split()).map(lambda l: (int(l[0]), (int(l[1]), float(l[2]))))
# (user,(movie, rate))
#　．．．．

# (ID, )

    self_joined_ratings = user_movie_ratings.join(user_movie_ratings)
# (user, ((movie1, rate), (movie1, rate)) )
# (user, ((movie1, rate), (movie2, rate)) )
# (user, ((movie2, rate), (movie1, rate)) )
# ....

    distinct_self_joined_ratings = self_joined_ratings.filter(remove_duplicates)
# (user, ((movie1, rate), (movie2, rate)) )

    movie_pairs = distinct_self_joined_ratings.map(make_movie_pairs)
# ((movie1, movie2), (rating1, rating2))

    movie_pair_ratings = movie_pairs.groupByKey()
# ((movie1, movie2), ((rating1, rating2), (rating1, rating2),..)

    movie_pair_with_scores = movie_pair_ratings.mapValues(compute_score)
# ((movie1, movie2), (score, pairs))

    similarity_threshold = 0.95
    appearence_threshold = 20
    movieID = 12  # 12,Usual Suspects, The (1995)

    result = movie_pair_with_scores.
            filter(lambda x: 
            (x[0][0] == movieID or x[0][1] == movieID) 
            and x[1][0] > similarity_threshold and x[1][1] > appearence_threshold).collect()

    if len(result) != 0:
        for p in result:
            print(p)
    else:
        print("Similar movies not found!")