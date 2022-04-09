import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
from matplotlib import rcParams
rcParams['font.family'] = 'sans-serif'
rcParams['font.sans-serif'] = ['Optima']
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.functions import pandas_udf
from pyspark.sql.functions import PandasUDFType
from pyspark.sql.functions import udf
from pyspark.sql.types import *


## read data
users = pd.read_csv('/Users/francis/Desktop/data/Users.csv').dropna(how='all').reset_index(drop=True)
posts = pd.read_csv('/Users/francis/Desktop/data/Posts.csv').dropna(how='all')
votes = pd.read_csv('/Users/francis/Desktop/data/Votes.csv').dropna(how='all')

## Distribution of Reputations
sns.distplot(users['Reputation'], rug=True)  
plt.title('Distribution of Reputations')
plt.tight_layout()
plt.show()

## Average share of upvotes received as a function of time
upvotes = votes[votes['VoteTypeId'] == 2]
answers = posts[posts['PostTypeId'] == 2].reset_index(drop=True)
votes_visual = pd.merge(
            upvotes[['Id', 'PostId', 'CreationDate']].rename(columns={'Id':'VoteId', 'CreationDate':'VoteDate'}), 
            answers[['Id', 'CreationDate']].rename(columns={'Id':'AnswerId','CreationDate':'AnswerDate'}), 
            left_on='PostId', 
            right_on='AnswerId')        
votes_visual['vote_timelapse'] = (pd.to_datetime(votes_visual['VoteDate']).dt.date - pd.to_datetime(votes_visual['AnswerDate']).dt.date).dt.days
votes_visual.to_csv('/Users/francis/Desktop/data/votes_visual.csv', index=False)
votes_visual[['AnswerId','vote_timelapse']].to_csv('/Users/francis/Desktop/data/votes_visual_simplified.csv', index=False)

# use Spark to perform calculation
spark = SparkSession.builder.getOrCreate()
path = '/Users/francis/Desktop/data/votes_visual_simplified.csv'
df = spark.read.csv(path, header=True, inferSchema=True)
def num0day(values):
    return np.sum(values==0)
def num1day(values):
    return np.sum(values==1)
def num2day(values):
    return np.sum(values==2)
def num3day(values):
    return np.sum(values==3)
def num4day(values):
    return np.sum(values==4)
def num5day(values):
    return np.sum(values==5)

functions = [len, num0day, num1day, num2day, num3day, num4day, num5day]

schema = StructType([
    StructField("AnswerId", IntegerType()),
    StructField("len", IntegerType()),
    StructField("num0day", IntegerType()),
    StructField("num1day", IntegerType()),
    StructField("num2day", IntegerType()),
    StructField("num3day", IntegerType()),
    StructField("num4day", IntegerType()),
    StructField("num5day", IntegerType()),
])
@pandas_udf(schema, PandasUDFType.GROUPED_MAP)
def numday_spark(df):
    return df.groupby('AnswerId')['vote_timelapse'].agg(functions).reset_index()

data = df.groupBy('AnswerId').apply(numday_spark)
data.select(F.mean(data['num0day'] / data['len']).alias("num0day"),
            F.mean(data['num1day'] / data['len']).alias("num1day"),
            F.mean(data['num2day'] / data['len']).alias("num2day"),
            F.mean(data['num3day'] / data['len']).alias("num3day"),
            F.mean(data['num4day'] / data['len']).alias("num4day"),
            F.mean(data['num5day'] / data['len']).alias("num5day"),
           ).show() # 0.6469871703299993| 0.11061186589714274|0.022316846422198475|0.011893519571513182|0.007689263502991405|0.005734143321178...


data_points = [64.7, 11.06, 2.23, 1.19, 0.77, 0.57]
plt.plot([0,1,2,3,4,5], data_points, 
         marker='>', lw=3, color='red', 
         markersize=12, markeredgecolor='black',
        markeredgewidth=0.5)
plt.ylabel('Percentage of upvotes received (%)')
plt.xlabel('Days after answer has been posted')
plt.title('Average Share of Upvotes')
plt.tight_layout()
plt.show()