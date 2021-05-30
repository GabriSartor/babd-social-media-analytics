from pymongo import MongoClient,UpdateOne

uri = "mongodb+srv://rdtsl.gakec.mongodb.net/myFirstDatabase?authSource=%24external&authMechanism=MONGODB-X509&retryWrites=true&w=majority"
client = MongoClient(uri,
                     tls=True,
                     tlsCertificateKeyFile='../config/X509-cert.pem')

db = client.reddit_soccer
sub_coll = db['reddit_submissions']
comm_coll = db['reddit_comments']

def get_thread_sample(limit = 1000, esl_related = None, include = ['title','selftext','_id'], exclude = None):
    pipeline = []
    match = dict()
    if esl_related is not None:
        match['is_esl_related'] = esl_related
    if exclude is not None:    
        for attribute in exclude:
            match[attribute] = { 
                "$exists": False, 
            }
    if len(match):
        pipeline.append({'$match': match })
        
    pipeline.append({'$sample': {'size': limit}})

    if include is not None:
        project = dict()
        for i in include:
            project[i] = 1
        pipeline.append({'$project': project})
        
    results = sub_coll.aggregate(pipeline)
    
    return results
    
def get_comments_sample(limit = 1000, esl_related = None, include = ['body','_id'], exclude = None):
    pipeline = []
    match = dict()
    if esl_related is not None:
        match['is_esl_related'] = esl_related
    if exclude is not None:    
        for attribute in exclude:
            match[attribute] = { 
                "$exists": False, 
            }
    if len(match):
        pipeline.append({'$match': match })
        
    pipeline.append({'$sample': {'size': limit}})

    if include is not None:
        project = dict()
        for i in include:
            project[i] = 1
        pipeline.append({'$project': project})
    results = comm_coll.aggregate(pipeline, allowDiskUse = True)
    
    return results
    
def bulk_update_comments(bulk):
    res = comm_coll.bulk_write([UpdateOne(b[0], b[1]) for b in bulk])
    return res