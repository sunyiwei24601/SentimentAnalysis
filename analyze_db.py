from arguments import args
from analyzer import Analyzer
import pymysql


if __name__ == "__main__":

    print("Please wait while the analyzer is being initialized.")
    analyzer = Analyzer(will_train=False, args=args)
    
    conn = pymysql.connect(host='107.175.221.5',
                                user='kyle',
                                password='192837',
                                database='REDDIT',
                                charset='utf8mb4',
                                cursorclass=pymysql.cursors.DictCursor)    
    
    for i in range(911500, 2000000, 100):
        cur = conn.cursor()
        start_id = i;
        end_id = i + 100;
        cur.execute(f'select id, title, content from REDDIT_DATA where id >= {start_id} and id < {end_id}')
        rows = cur.fetchall()

        texts = []
        for n, row in enumerate(rows):
            id = row['id']
            title = row['title']
            content = row['content']
            sentence = title + " " + content
            texts.append(sentence[:200])
            sentiment, percentage = analyzer.classify_sentiment(sentence[:200])
            
            
            cur.execute(f'update REDDIT_DATA SET sentiment_score={percentage/100 * 2 - 1} where id={id}')

            
        # sentiment_score = analyzer.classify_sentiment_batch(texts)

        # # print(sentiment_score)
        # for n, id in enumerate(range(start_id, end_id)):
        #     cur.execute(f'update reddit_data SET sentiment_score={sentiment_score[n]} where id={id}')
        conn.commit()
        print(start_id, 'finished')
    
