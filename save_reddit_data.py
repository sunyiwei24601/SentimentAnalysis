import sqlite3
import csv
from push_io_comments import stocks_dict

conn = sqlite3.connect("reddit.db")
c = conn.cursor()
c.execute("""
          CREATE TABLE IF NOT EXISTS REDDIT_DATA 
          (ID INTEGER PRIMARY KEY AUTO_INCREMENT NOT NULL ,
           TICKER         TEXT NOT NULL,
           SCORE          FLOAT NOT NULL,
           AUTHOR         TEXT  NOT NULL,
           NUM_COMMENTS   INT   NOT NULL,
           CONTENT        TEXT  NOT NULL,
           SUBREDDIT      TEXT  NOT NULL,
           TITLE          TEXT  NOT NULL,
           CREATE_DATE    DATE  NOT NULL,
           CREATE_TIME    INT   NOT NULL,
           SENTIMENT_SCORE FLOAT default 0.0
          );           
          """)
c.execute("CREATE INDEX IF NOT EXISTS ID_INDEX ON REDDIT_DATA(ID);")
c.execute("CREATE INDEX IF NOT EXISTS SEARCH_INDEX ON REDDIT_DATA(TICKER, CREATE_DATE);")


conn.commit()

def GenInsert(data):
    sql = f"""INSERT INTO REDDIT_DATA 
        ( TICKER, SCORE, AUTHOR, NUM_COMMENTS, CONTENT, SUBREDDIT, TITLE, CREATE_DATE, CREATE_TIME)
        VALUES
        (
        "{data.get('ticker')}", 
        {data.get('score', 0)},
        "{data.get('author', '').replace('"', "'")}",
        {data.get('num_comments', 0)},
        "{data.get('text', '').replace('"', "'")}",
        "{data.get('subreddit', '').replace('"', "'")}",
        "{data.get('title', '').replace('"', "'")}",
        "{data.get('date', 0)}",
        {data.get('created', 0)}
        );
        """
    
    return sql
    
for ticker in stocks_dict.values():
    filename = f'reddit_{ticker}.csv'
    with open(filename) as csvfile:
        c = conn.cursor()
        csvreader = csv.DictReader(csvfile, delimiter=",", quotechar='|')
        n=0
        for row in csvreader:
            data = dict(row)
            if data['ticker'] != ticker:
                continue
            n+=1
            if n % 1000 == 0:
                print(f"insert {n} {ticker}")

            insert_command = GenInsert(data)
            c.execute(insert_command)

        conn.commit()












conn.commit()






conn.close()
