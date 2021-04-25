import datetime
import os
#MONGO DB CREDENTIALS

#ONGO_PYTHON_DAEMON_USERNAME = os.environ['MONGO_PYTHON_DAEMON_USERNAME']
#MONGO_PYTHON_DAEMON_PASSWORD = os.environ['MONGO_PYTHON_DAEMON_PASSWORD']
#MONGO_INITDB_DATABASE = os.environ['MONGO_INITDB_DATABASE']

#DEV_MODE = os.environ['DEV_MODE']

from pymongo import MongoClient
from api_handler import get_submissions, get_comments_id, get_comments
from config import Config


uri = "mongodb+srv://rdtsl.gakec.mongodb.net/myFirstDatabase?authSource=%24external&authMechanism=MONGODB-X509&retryWrites=true&w=majority"
client = MongoClient(uri,
                     tls=True,
                     tlsCertificateKeyFile='X509-cert.pem')

db = client.reddit_soccer
sub_coll = db['reddit_submissions']
comm_coll = db['reddit_comments']

def get_comments_list(sub):
    if sub['num_comments'] >= 10:
        print("Getting list of comments for submission {}".format(sub['id']))
        #Get list of comments
        r_ids = get_comments_id(Config.URL_COMMENTS_ID, sub['id'] )
        id_list = r_ids.json()['data']
        if id_list:
            l =  len(id_list)
            chunk_size = 200
            ran = range(l)
            steps=list(ran[chunk_size::chunk_size])
            steps.extend([l])
            print("Found {} comments".format(len(id_list)))
            print("Number of steps: {}".format(len(steps)))
            # Inser chunks of the dataframe
            i = 0
            for j in steps:
                r_comments = get_comments(Config.URL_COMMENTS, field_list = Config.COMMENT_FIELD_LIST, id_list = id_list[i:j])
                print("Comments downloaded: {} - progress: {} of {}".format(len(r_comments.json()['data']),j,l))
                comm_coll.insert_many(r_comments.json()['data'])
                i = j

res = sub_coll.find().sort("created_utc", -1)

starting_time = datetime.datetime(2021, 4, 16, 0, 0)

if res.count():
    starting_time = datetime.datetime.fromtimestamp(res[0]['created_utc'])

try:
    while starting_time < datetime.datetime.now():
        print("Fetching data from {}".format(starting_time.isoformat()))
        try:
            r_sub = get_submissions(Config.URL_SUB, field_list = Config.SUBMISSION_FIELD_LIST, 
                                        subreddit_list = Config.SUBREDDIT_LIST, 
                                        after = int(starting_time.timestamp()),
                                        size = 30,
                                        sort = 'created_utc')
            if r_sub.json()['data']:
                starting_time = datetime.datetime.fromtimestamp(r_sub.json()['data'][len(r_sub.json()['data'])-1]['created_utc']) 
                print("Fetched {} reddit submissions up until {}".format(len(r_sub.json()['data']), starting_time) )
                sub_list = r_sub.json()['data']
                sub_coll.insert_many(sub_list)
                for sub in sub_list:
                    try:
                        get_comments_list(sub)
                    except:
                        print("Error in fetching data for submission {}".format(sub['id']))
            else:
                print("Fetched ALL RESULTS up until {}".format(starting_time))
                break
        except:
            print("Error in fetching data from {}".format(starting_time.isoformat()))
except KeyboardInterrupt:
    print("Press Ctrl-C to terminate while statement")
    pass          
      
            
    


