import pymysql

conn = pymysql.connect(host='localhost',
                            user='kyle',
                            password='192837',
                            database='REDDIT',
                            charset='utf8mb4',
                            cursorclass=pymysql.cursors.DictCursor)

cur = conn.cursor()

def get_rows_from_db(ticker, date):
    cur.execute(f"""
               SELECT ID, TICKER, CREATE_DATE, SENTIMENT_SCORE, NUM_COMMENTS, SCORE
               FROM REDDIT_DATA
               WHERE TICKER = "{ticker}" AND CREATE_DATE = "{date}"  """) 
    rows = cur.fetchall()
    return rows

def get_average_score(ticker, date):
    rows = get_rows_from_db(ticker, date)
    scores = [_['SENTIMENT_SCORE'] for _ in rows]
    return sum(scores) / len(scores)

def get_weighted_score(ticker, date):
    rows = get_rows_from_db(ticker, date)
    sentiment_scores = [_['SENTIMENT_SCORE'] for _ in rows]
    scores = [_['SCORE'] for _ in rows]
    num_comments = [_['NUM_COMMENTS'] for _ in rows]
    return sum(sentiment_scores) / (sum(scores) + len(scores)), sum(scores) + len(scores)

if __name__ == '__main__':
    rows = get_rows_from_db("AXP", "2009-01-14")
    print(rows)
    print(get_average_score("AXP", "2009-01-14"))
    print(get_weighted_score("AXP", "2009-01-14"))