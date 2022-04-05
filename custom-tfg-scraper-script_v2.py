#!/usr/bin/env python3
# coding: utf-8

# In[1]:

#!/usr/bin/env python3

import requests
import json
import time
from sqlalchemy import create_engine
import pandas as pd
import pymysql
import boto3
from requests_oauthlib import OAuth1
from multiprocessing import Pool
from multiprocessing.dummy import Pool as ThreadPool
import traceback
import logging
from datetime import date
from datetime import timedelta
from datetime import datetime
import gzip
import io
from requests_oauthlib import OAuth1

url = 'https://api.twitter.com/1.1/account/verify_credentials.json'
# Twitter API credentials
consumer_key = 'hQUKQ9WYLQtQKkfKaJeFnGgVB'
consumer_secret = 'cdjbrK256fwABwosF4EdTTB3VE74bljLuMwmrrSMRxYY5ORLNC'
url = 'https://api.twitter.com/1.1/account/verify_credentials.json'
auth = OAuth1(consumer_key , consumer_secret)
test = requests.get(url, auth=auth)
dbId = ''
dbPw = ''


def chunks(l, n):
    """Yield successive n-sized chunks from l."""
    for i in range(0, len(l), n):
        yield l[i:i + n]


def athena_query(athenaClient, params):
    response = athenaClient.start_query_execution(
        QueryString=params["query"],
        QueryExecutionContext={
            'Database': params['database']
        },
        ResultConfiguration={
            'OutputLocation': 's3://' + params['bucket'] + '/' + params['path']
        }
    )
    return response


def athena_to_s3(params, max_execution=5):
    athenaClient = boto3.client('athena')
    execution = athena_query(athenaClient, params)
    execution_id = execution['QueryExecutionId']
    state = 'RUNNING'
    while (max_execution > 0 and state in ['RUNNING']):
        max_execution = max_execution - 1
        response = athenaClient.get_query_execution(QueryExecutionId=execution_id)
        if 'QueryExecution' in response and                 'Status' in response['QueryExecution'] and                 'State' in response['QueryExecution']['Status']:
            state = response['QueryExecution']['Status']['State']
            if state == 'FAILED':
                return False
            elif state == 'SUCCEEDED':
                s3_path = response['QueryExecution']['ResultConfiguration']['OutputLocation']
                return s3_path
        time.sleep(30)
    
    return False


def s3_to_list(bucket, key, header=None):
    s3Client = boto3.client('s3')
    # get key using boto3 client
    obj = s3Client.get_object(Bucket=bucket, Key=key)
    ids = pd.read_csv(io.StringIO(obj['Body'].read().decode('utf-8')))
    return ids


def pushHandlesToList(userList):
    engine = create_engine(
        "mysql+pymysql://" + dbId + ":" + dbPw + "@pa-datasources-cluster.cluster-ctkeuweqhlpx.us-east-1.rds.amazonaws.com/affinity?charset=utf8",
        encoding="utf-8")
    userList.to_sql(name='adhoc_followers', con=engine, index=False, chunksize=1000, if_exists='append')


def call_user_lookup(user_ids):
    try:
        user_id_str_list = []
        for user_id in user_ids:
            user_id_str_list.append(str(user_id))
        columns = ['id', 'screen_name', 'name', 'followers_count', 'friends_count', 'lang', 'location', 'verified','description','profile_image_url']
        url = 'https://api.twitter.com/1.1/users/lookup.json?user_id=' + ','.join(user_id_str_list)
        insertlog('info', 'API CALL MADE FOR call_user_lookup')
        output = requests.get(url, auth=auth)
        if output.status_code == 200:
            users_lookup = []
            output = json.loads(output.text)
            for user in output:
                users_lookup.append(user)
            users_lookup = pd.DataFrame(data=users_lookup, columns=columns)

            users_lookup['profile_image_url'] = [picture_url.replace('_normal','') for picture_url in users_lookup['profile_image_url']]

            engine = create_engine(
                "mysql+pymysql://" + dbId + ":" + dbPw + "@pa-datasources-cluster.cluster-ctkeuweqhlpx.us-east-1.rds.amazonaws.com/affinity?charset=utf8",
                encoding="utf-8")
            insertlog('info', 'Pushing Followers in Database')
            users_lookup.to_sql(name='adhoc_look_up', con=engine, index=False, chunksize=1000, if_exists='append')
        else:
            insertlog('warn', 'API LIMITATION REACHED')
            insertlog('warn', output.text)
            time.sleep(300)
            return call_user_lookup(user_ids)
    
    except Exception:
        insertlog('error', str(traceback.format_exc()))


def doUserLookUp():
    insertlog('info', 'Getting User IDs')
    connection = pymysql.connect(host='pa-datasources-cluster.cluster-ctkeuweqhlpx.us-east-1.rds.amazonaws.com',
                                        user=dbId,
                                        password=dbPw,
                                        db='affinity',
                                        charset='utf8mb4',
                                        cursorclass=pymysql.cursors.DictCursor)
    
    sql = "SELECT distinct user_id from adhoc_followers"
    
    friends_df = pd.read_sql(sql, con=connection)
    
    ids = list(friends_df.user_id)
    
    connection.close()

    partitioned_users = list(chunks(ids, 100))
    insertlog('info', 'USERLOOK UP COLLECTION STARTED!')
    pool = ThreadPool(15)
    pool.map(call_user_lookup, partitioned_users)
    pool.close()
    pool.join()


def get_friends(user_id):
    try:
        friends_df = []
        cursor = '-1'
        compt = 0
        end_bool = False
        while end_bool == False:
            compt += 1
            call = friends_call(user_id, cursor)
            if call[0] != 'empty':
                friends_df.extend(call[0])
                friends_df = list(set(friends_df))
            if call[0] == 'empty':
                end_bool = True
            if call[1] != 'empty':
                cursor = call[1]
            if call[1] == 'empty':
                end_bool = True
            time.sleep(10)
        friends_df = pd.DataFrame(data=friends_df, columns=['friend_id'])
        friends_df['user_id'] = [user_id for _ in range(len(friends_df))]
        #friends_df['user_id'] = [user_id] * len(friends_df)
        friends_df = friends_df[['user_id', 'friend_id']]
        return friends_df
    except Exception:
        insertlog('error', str(traceback.format_exc()))


def friends_call(user_id, cursor):
    ids = 'empty'
    next_cursor = 'empty'
    url = 'https://api.twitter.com/1.1/friends/ids.json?user_id=' + str(user_id) + '&stringify_ids=True&cursor=' + cursor
    output = requests.get(url, auth=auth)
    insertlog('info', 'API CALL MADE FOR friends_call')
    if output.status_code == 200:
        output = json.loads(output.text)
        if 'ids' in output.keys():
            ids = output['ids']
            tmp_df = pd.DataFrame(data=ids, columns=['friends_id'])
            tmp_df['user_id'] = [user_id] * len(tmp_df)
            tmp_df = tmp_df[['user_id', 'friends_id']]
            engine = create_engine(
                "mysql+pymysql://" + dbId + ":" + dbPw + "@pa-datasources-cluster.cluster-ctkeuweqhlpx.us-east-1.rds.amazonaws.com/affinity?charset=utf8",
                encoding="utf-8")
            insertlog('info', 'Pushing Followers in Database')
            query = f'''select distinct friends_id  from adhoc_friend where user_id = '{user_id}'
            '''
            unique_friends_id = pd.read_sql(query, con = engine)['friends_id'].tolist()
            ftr = ~tmp_df['friends_id'].isin(unique_friends_id)
            tmp_df = tmp_df.loc[ftr, :]
            tmp_df.to_sql(name='adhoc_friend', con=engine, index=False, chunksize=1000, if_exists='append')
        if output['next_cursor_str'] != '0':
            next_cursor = output['next_cursor_str']
        return ids, next_cursor
    else:
        if output.text == '{"errors":[{"message":"Rate limit exceeded","code":88}]}':
            insertlog('warn', output.text)
            time.sleep(300)
            return friends_call(user_id, cursor) 
        else: 
            insertlog('warn', 'Private user: ' + str(user_id))
            return 'empty', 'empty'


def doGetFriends():
    insertlog('info', 'Getting UserIds for Friend')
    connection = pymysql.connect(host='pa-datasources-cluster.cluster-ctkeuweqhlpx.us-east-1.rds.amazonaws.com',
                                        user=dbId,
                                        password=dbPw,
                                        db='affinity',
                                        charset='utf8mb4',
                                        cursorclass=pymysql.cursors.DictCursor)
    
    sql = "SELECT * from adhoc_look_up"
    dataset = pd.read_sql(sql, con=connection)
    dataset = dataset[(dataset.friends_count >= 10) & (dataset.friends_count <= 5000)]
    ids = list(dataset.id)
    connection.close()
    
    insertlog('info', 'USER FRIEND UP COLLECTION STARTED!')
    pool = ThreadPool(15)
    pool.map(get_friends, ids)
    pool.close()
    pool.join()


def call_filtered_lookup(user_ids):
    try:
        user_id_str_list = []
        for user_id in user_ids:
            user_id_str_list.append(str(user_id))
        columns = ['id', 'screen_name', 'name', 'followers_count', 'friends_count', 'lang', 'location', 'verified']
        url = 'https://api.twitter.com/1.1/users/lookup.json?user_id=' + ','.join(user_id_str_list)
        insertlog('info', 'API CALL MADE FOR CALL_FILTERED_LOOKUP')
        output = requests.get(url, auth=auth)
        if output.status_code == 200:
            users_lookup = []
            output = json.loads(output.text)
            for user in output:
                users_lookup.append(user)
            users_lookup = pd.DataFrame(data=users_lookup, columns=columns)
            engine = create_engine(
                "mysql+pymysql://" + dbId + ":" + dbPw + "@pa-datasources-cluster.cluster-ctkeuweqhlpx.us-east-1.rds.amazonaws.com/affinity?charset=utf8",
                encoding="utf-8")
            insertlog('info', 'Pushing Followers in Database')
            users_lookup.to_sql(name='adhoc_look_up_filtered', con=engine, index=False, chunksize=1000, if_exists='append')
        else:
            insertlog('warn', 'API LIMITATION REACHED')
            insertlog('warn', output.text)
            time.sleep(300)
            return call_user_lookup(user_ids)
    
    except Exception:
        insertlog('error', str(traceback.format_exc()))


def doGetAggregation():
    insertlog('info', 'Getting final filtered user ids from DB')
    connection = pymysql.connect(host='pa-datasources-cluster.cluster-ctkeuweqhlpx.us-east-1.rds.amazonaws.com',
                                        user=dbId,
                                        password=dbPw,
                                        db='affinity',
                                        charset='utf8mb4',
                                        cursorclass=pymysql.cursors.DictCursor)
    
    sql = "select distinct friends_id from (select title_id, friends_id, count(distinct r.user_id) as app_count from adhoc_followers as f inner join adhoc_friend as r on f.user_id = r.user_id group by title_id, friends_id) as d where app_count > 50"    
    friends_df = pd.read_sql(sql, con=connection)
    
    ids = list(friends_df.friends_id)
    
    connection.close()

    partitioned_users = list(chunks(ids, 100))
    
    pool = ThreadPool(15)
    pool.map(call_filtered_lookup, partitioned_users)
    pool.close()
    pool.join()


def joinFinalOutput():
    insertlog('info', 'Joining Final Output from DB')
    connection = pymysql.connect(host='pa-datasources-cluster.cluster-ctkeuweqhlpx.us-east-1.rds.amazonaws.com',
                                        user=dbId,
                                        password=dbPw,
                                        db='affinity',
                                        charset='utf8mb4',
                                        cursorclass=pymysql.cursors.DictCursor)
    
    sql = "select title_id, friends_id as id,screen_name,name,verified, count(distinct r.user_id) as app_count from adhoc_followers as f inner join adhoc_friend as r on f.user_id = r.user_id inner join adhoc_look_up_filtered as a on a.id = friends_id group by title_id, friends_id"
    dataset = pd.read_sql(sql, con=connection)

    connection.close()
    
    engine = create_engine(
        "mysql+pymysql://" + dbId + ":" + dbPw + "@pa-datasources-cluster.cluster-ctkeuweqhlpx.us-east-1.rds.amazonaws.com/affinity?charset=utf8",
        encoding="utf-8")    
    dataset.to_sql(name='adhoc_final_output', con=engine, index=False, chunksize=1000, if_exists='append')


# Posting to a Slack channel
def send_message_to_slack(text):
    from urllib import request, parse
    import json
 
    post = {"text": "{0}".format(text)}
 
    try:
        json_data = json.dumps(post)
        req = request.Request("https://hooks.slack.com/services/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",
                              data=json_data.encode('ascii'),
                              headers={'Content-Type': 'application/json'}) 
        resp = request.urlopen(req)
    except Exception as em:
        print("EXCEPTION: " + str(em))


def insertlog(level, message):
    if level == 'info':
        logging.info(str(datetime.utcnow()) + ' ' + message)
    if level == 'warn':
        logging.warn(str(datetime.utcnow()) + ' ' + message)
    if level == 'error':
        send_message_to_slack('Something went wrong while running app. see below messgage')
        send_message_to_slack(message)
        logging.error(str(datetime.utcnow()) + ' ' + message)

# In[21]:


def getParams():
    try:
        dynamodbClient = boto3.client('dynamodb')
        response = dynamodbClient.batch_get_item(
            RequestItems={
                'application_config': {
                    'Keys': [
                        {
                            'appName':{'S':'tfg_adhoc'},
                            'configKey':{'S':'titles'}
                        },
                        {
                            'appName':{'S':'tfg_adhoc'},
                            'configKey':{'S':'sample_size'}
                        },
                        {
                            'appName':{'S':'tfg_adhoc'},
                            'configKey':{'S':'month'}
                        },
                        {
                            'appName':{'S':'dblibrary'},
                            'configKey':{'S':'dataaggregation.user'}
                        },
                        {
                            'appName':{'S':'dblibrary'},
                            'configKey':{'S':'dataaggregation.password'}
                        },
                    ]
                }
            }
        )
    
        output = json.loads(json.dumps(response))
        users_json = output['Responses']['application_config']
        params = {
            'titles': '',
            'sample_size': 0,
            'month': 0,
            'dbId': '',
            'dvPw': '',
        }
    
        for user_json in users_json:
            if user_json['configKey']['S'] == 'titles':
                params['titles'] = list(user_json['configValue']['S'].split(','))
            if user_json['configKey']['S'] == 'sample_size':
                params['sample_size'] = int(user_json['configValue']['N'])
            if user_json['configKey']['S'] == 'month':
                params['month'] = int(user_json['configValue']['N'])
            if user_json['configKey']['S'] == 'dataaggregation.user':
                params['dbId'] = user_json['configValue']['S']
            if user_json['configKey']['S'] == 'dataaggregation.password':
                params['dvPw'] = user_json['configValue']['S']
    
        return params
    except Exception:
        insertlog('error', str(traceback.format_exc()))  

# In[ ]:


if __name__ == '__main__':

    #params = getParams()
    send_message_to_slack('TFG job is started')
    logging.basicConfig(filename='tfg_adhoc_scraper.log', level=logging.INFO)
    insertlog('info', 'START!')

    dbId = 'johan'
    dbPw = '3gfh6tgm'   
    
    # Perform User Look Up
    insertlog('info', 'START USER LOOKUP')
    doUserLookUp()
    
    # Get Friends
    insertlog('info', 'START USER FRIENDS')
    doGetFriends()
    
    # Get only significantly called users
    insertlog('info', 'START FILTERED USER CALL')
    doGetAggregation()
    
    insertlog('info', 'JOIN FINAL OUTPUT')
    joinFinalOutput()
    
    insertlog('info', 'ALL DONE!!!!')
    send_message_to_slack('TFG job is all done')

