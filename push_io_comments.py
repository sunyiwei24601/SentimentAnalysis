import json
import time
from pip import main
import requests
import csv
import os
import datetime as dt
from datetime import datetime
import pytz

def get_pushshift_data(**kwargs):
    """
    Gets data from the pushshift api.

    data_type can be 'comment' or 'submission'
    The rest of the args are interpreted as payload.

    Read more: https://github.com/pushshift/api
    """
    base_url = f"https://api.pushshift.io/reddit/submission/search/"
    payload = kwargs
    request = requests.get(base_url, params=payload)
    return request.json()

stocks_dict = {
    # "American Express": "AXP",
    # "Amgen": "AMGN",
    # "Apple": "AAPL",
    # "Boeing": "BA",
    # "Caterpillar": "CAT",
    # "Cisco": "CSCO",
    # "Chevron": "CVX",
    # "Goldman Sachs": "GS",
    # "Home Depot": "HS",
    # "Honeywell": "HON",
    # "IBM": "IBM",
    # "Intel": "INTC",
    # "Johnson & Johnson": "JNJ",
    # "Coca-Cola": "KO",
    # "JPMorgan": "JPM",
    # "McDonald": "MCD",
    # "3M": "MMM",
    # "Merck": "MRK",
    # "Microsoft": "MSFT",
    # "Nike": "NKE",
    # "Procter & Gamble": "PG",
    # "Travelers Companies": "TRV",
    # "UnitedHealth": "UNH",
    # "Salesforce": "CRM",
    # "Verizon": "VZ",
    "Visa": "V",
    "Walgreen": "WBA",
    "Walmart": "WMT",
    "Disney": "DIS",
    "Dow inc": "DOW",
}

timezone = pytz.timezone("America/New_York")

start_time = datetime(2015, 1, 1, 0, 0, tzinfo=timezone)
one_day = dt.timedelta(days=1)
end_time = datetime(2022, 1, 1, 0, 0, tzinfo=timezone)


def get_stock_data(query, stock):
    result = []
    today_time = start_time 
    tomorrow_time = start_time + one_day
    while(today_time < end_time):
        try:
            data = get_pushshift_data(q=query, 
                after=int(datetime.timestamp(today_time)), 
                before=int(datetime.timestamp(tomorrow_time)),
                sort_type="score",
                sort="desc"
                            )['data']
            for row in data:
                result.append({
                    "created":row['created_utc'],
                    "ticker": stock,
                    "score": row['score'],
                    "query": query,
                    "author": row['author'],
                    "link": row['full_link'],
                    "num_comments": row['num_comments'],
                    "text": row["selftext"].replace("\n", " ").replace("\r", ""),
                    "subreddit": row['subreddit'],
                    "title": row['title'],
                    "date": today_time.strftime("%Y-%m-%d")
                })
        except Exception as e:
            print("there is error: {}".format(e))
        today_time = tomorrow_time 
        tomorrow_time = today_time + one_day
        print(stock, today_time)
        time.sleep(1)
    return result


if __name__ == '__main__':
    
    results = []
    for query, stock in stocks_dict.items():
        results += get_stock_data(query, stock)
        with open('reddit_{}.csv'.format(stock), 'a') as csvfile:
            fieldnames = list(["created","ticker","score",'query','author','link','num_comments','text','subreddit','title','date'])

            writer = csv.DictWriter(csvfile, fieldnames=fieldnames, delimiter=",", quotechar='|')
            writer.writeheader()
            for row in results:
                writer.writerow(row)





