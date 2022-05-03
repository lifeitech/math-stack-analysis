import pandas as pd
import re
from bs4 import BeautifulSoup


"""
Extract Post Data
"""
def handle_none(result): return int(result.group(1)) if result is not None else None

def clean(text):
    if text is None:
        return ''
    for trash in ['&lt;', 'p&gt;', '&gt;', '&#xA;', '&quot;']:
        text = text.replace(trash, '')  
    return text

def process_line_post(line):
        date_match = re.search(' CreationDate="(.*?)" ', line)
        if date_match is not None:
            DateTime = re.search(' CreationDate="(.*?)" ', line).group(1)
        else:
            DateTime = None
            
        body_match = re.search(' Body="(.*?)" ', line)
        if body_match is not None:
            body = clean(body_match.group(1))
            body_length = len(re.findall(r'\w+', body))
            math_ratio = len(''.join(re.findall('\$(.*?)\$', body))) / body_length
        else:
            body = None
            body_length = None
            math_ratio = None
            
        return dict(
        CreationDate = DateTime,
        Id = handle_none(re.search(r' Id="(\d+)" ', line)),
        PostTypeId = handle_none(re.search(' PostTypeId="(\d+)" ', line)),
        Score = handle_none(re.search(' Score="(\d+)" ', line)),
        OwnerUserId = handle_none(re.search(' OwnerUserId="(\d+)" ', line)),
        ParentId = handle_none(re.search(' ParentId="(\d+)" ', line)),
        AnswerCount = handle_none(re.search(' AnswerCount="(\d+)" ', line)),
        CommentCount = handle_none(re.search(' CommentCount="(\d+)" ', line)),
        FavoriteCount = handle_none(re.search(' FavoriteCount="(\d+)" ', line)),
        AcceptedAnswerId = handle_none(re.search(' AcceptedAnswerId="(\d+)" ', line)),
        PostLength = body_length,
        MathRatio = math_ratio,            
        )  


path = '/Users/francis/Desktop/MSE/math/Posts.xml'
with open(path) as f:
    df_posts = pd.DataFrame(list(map(process_line_post, f.readlines())))
    df_posts.to_csv('/Users/francis/Desktop/data/posts.csv', index=False)


"""
Extract User Data
"""
def handle_none2(result): return int(result) if result is not None else None

def extract_user_data(row):
    return dict(
    Id = handle_none2(row.get('id')),
    Reputation = handle_none2(row.get('reputation')),
    CreationDate = row.get('creationdate'),
    DisplayName = row.get('displayname'),
    Views = handle_none2(row.get('views')),
    UpVotes = handle_none2(row.get('upvotes')),
    DownVotes = handle_none2(row.get('downvotes')),
    AccountId = handle_none2(row.get('accountid'))
    )

path = '/Users/francis/Desktop/MSE/math/Users.xml'
docs = BeautifulSoup(open(path), 'lxml')
rows = docs.find_all('row')
df_users = pd.DataFrame(list(map(extract_user_data, rows)))
df_users.to_csv('/Users/francis/Desktop/data/users.csv', index=False)


"""
Extract Badge Data
"""
def extract_badge_data(row):
    return dict(
    Id = int(row.get('id')),
    UserId = int(row.get('userid')),
    Name = row.get('name'),
    Date = row.get('date'),
    Class = int(row.get('class').pop()),
    )
path = '/Users/francis/Desktop/MSE/math/Badges.xml'
docs = BeautifulSoup(open(path), 'lxml')
rows = docs.find_all('row')
df_badges = pd.DataFrame(list(map(extract_badge_data, rows)))
df_badges.to_csv('/Users/francis/Desktop/data/badges.csv', index=False)


"""
Extract Votes Data
"""
def process_line_votes(line):
        date_match = re.search(' CreationDate="(.*?)" ', line)
        if date_match is not None:
            DateTime = re.search(' CreationDate="(.*?)" ', line).group(1)
        else:
            DateTime = None                         
        return dict(
        CreationDate = DateTime,
        Id = handle_none(re.search(r' Id="(\d+)" ', line)),
        PostId = handle_none(re.search(' PostId="(\d+)" ', line)),
        VoteTypeId = handle_none(re.search(' VoteTypeId="(\d+)" ', line)),            
        )  

path = '/Users/francis/Desktop/MSE/math/Votes.xml'
with open(path) as f:
    df_votes = pd.DataFrame(list(map(process_line_votes, f.readlines())))
    df_votes.to_csv('/Users/francis/Desktop/data/votes.csv', index=False)