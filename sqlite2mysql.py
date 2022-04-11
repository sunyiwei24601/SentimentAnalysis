import pymysql
import sqlite3

# Connect to the database
conn = pymysql.connect(host='localhost',
                             user='kyle',
                             password='192837',
                             database='REDDIT',
                             charset='utf8mb4',
                             cursorclass=pymysql.cursors.DictCursor)

conn2 = sqlite3.connect("reddit.db")
for i in range(1, 1700000, 1000):
    cur2 = conn2.cursor()
    cur = conn.cursor()
    start_id = i;
    end_id = i + 1000;
    cur2.execute(f'select * from reddit_data where id >= {start_id} and id < {end_id}')
    rows = cur2.fetchall()

    for row in rows:
        row = [i.encode().decode('utf8')  if isinstance(i, str) else i for i in row]
        sql = """INSERT INTO REDDIT_DATA (ID, TICKER, SCORE, AUTHOR, NUM_COMMENTS, CONTENT, SUBREDDIT, TITLE, CREATE_DATE, 
        CREATE_TIME, SENTIMENT_SCORE
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"""
        cur.execute(sql, row)
    conn.commit()
    print(f'finished {i + 1000} lines')
   