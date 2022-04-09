import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
from sklearn.decomposition import PCA
from sklearn.preprocessing import StandardScaler
from functools import reduce

# read data
posts = pd.read_csv('/Users/francis/Desktop/data/Posts.csv').dropna(how='all')
posts['CreationDate'] = pd.to_datetime(posts['CreationDate'])
users = pd.read_csv('/Users/francis/Desktop/data/Users.csv').dropna(how='all').reset_index(drop=True)
badges = pd.read_csv('/Users/francis/Desktop/data/Badges.csv').dropna(how='all').reset_index(drop=True)
questions = posts[posts.PostTypeId == 1].rename(columns={'OwnerUserId':'OP'})
answers = posts[posts.PostTypeId == 2]

# data aggregation
average_post_length = posts.groupby(by='OwnerUserId')['PostLength'].mean().rename('Average_Post_Length').to_frame().reset_index()
average_math_ratio = posts.groupby(by='OwnerUserId')['MathRatio'].mean().rename('Average_Math_Ratio').to_frame().reset_index()

answer_counts = answers.groupby(by='OwnerUserId')['Id'].count().rename('Answer_Counts').to_frame().reset_index()
question_counts = questions.groupby(by='OP')['Id'].count().rename('Question_Counts').to_frame().reset_index().rename(columns={'OP':'OwnerUserId'})

OP_reputation = pd.merge(answers[['OwnerUserId', 'ParentId']], questions[['Id', 'OP']], left_on='ParentId', right_on='Id')\
.merge(users[['Id', 'Reputation']], left_on='OP', right_on='Id')
average_op_reputation = OP_reputation.groupby('OwnerUserId')['Reputation'].mean().rename('Average_OP_Reputation').to_frame().reset_index()

answer_question_correspondence = pd.merge(answers[['Id','OwnerUserId','CreationDate','ParentId']], questions[['Id','CreationDate']], left_on='ParentId', right_on='Id')     
answer_question_correspondence['response_time'] = (answer_question_correspondence.CreationDate_x - answer_question_correspondence.CreationDate_y).values.astype(np.int64)    
average_response_time = pd.to_timedelta(answer_question_correspondence.groupby('OwnerUserId')['response_time'].mean()).rename('Average_Response_Time').to_frame().reset_index()
dfs = [answer_counts,question_counts,average_op_reputation,average_response_time, average_post_length,average_math_ratio]

features = reduce(lambda left,right: pd.merge(left,right,on='OwnerUserId',how='outer'), dfs)

table = pd.merge(users[['Id','Reputation']],features,left_on='Id',right_on='OwnerUserId').dropna(how='all').fillna(0)   
table['response_time_seconds'] = table.Average_Response_Time.dt.seconds
np.percentile(table.Reputation, q=99)  # top 1 percent reputation. The result is 2579.

def categorize(v):
    if v >= 2579:
        return 1
    else:
        return 0
table['top_user'] = table['Reputation'].apply(categorize)

# export
table.to_csv('/Users/francis/Desktop/data/table.csv', index=False)