from database import get_thread_sample, get_comments_sample, bulk_update_comments
import flair
import pandas as pd

flair_sentiment = flair.models.TextClassifier.load('en-sentiment')

def from_cursor_to_df(cursor):
    return pd.DataFrame.from_dict(list(cursor))
    
def sentiment(x): 
    if len(x) == 0:
        return ['EMPTY (0)']
    s = flair.data.Sentence(x)
    flair_sentiment.predict(s)
    return s.labels

comments_field_list = ['body', 'is_esl_related', '_id']

df_comments = from_cursor_to_df(get_comments_sample(limit=200, esl_related = None, include = comments_field_list, exclude = ['flair_sentiment']))
total = len(df_comments)
while len(df_comments) > 0:
    try:
        print("Fetched {} comments. Starting sentiment analysis".format(len(df_comments)))
        df_comments['is_esl_related'].fillna(False, inplace=True)
        df_comments['Sentiment'] = df_comments['body'].apply(
            lambda x: sentiment(x)
        )
        df_comments['Sentiment_direction'] = df_comments['Sentiment'].apply(
            lambda x: str(x[0]).split()[0]
        )
        df_comments['Sentiment_accuracy'] = df_comments['Sentiment'].apply(
            lambda x: float(str(x[0]).split()[1].replace('(', '').replace(')', '') )
        )
        print("Sentiment computed. Going forward with update on MongoDB")
        bulk = []
        for index, row in df_comments.iterrows():
            bulk.append([{ "_id": (row['_id']) }, 
                         { "$set": 
                              { 'flair_sentiment' : row['Sentiment_direction'], 
                               'flair_sentiment_accuracy' : row['Sentiment_accuracy']
                              } 
                         }])
        res = bulk_update_comments(bulk)
        total += len(df_comments)
        print("Entry modified: {}".format(res.modified_count))
        print("Total progress: {}".format(total))
        df_comments = from_cursor_to_df(get_comments_sample(limit=200, esl_related = None, include = comments_field_list, exclude = ['flair_sentiment']))
    except:
        break