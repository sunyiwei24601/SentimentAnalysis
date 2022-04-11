from arguments import args
from analyzer import Analyzer
import sys

if __name__ == "__main__":

    print("Please wait while the analyzer is being initialized.")
    analyzer = Analyzer(will_train=False, args=args)
    filename = sys.argv[2]
    result = []
    with open(filename, newline='') as csvfile:
        csvreader = csv.DictReader(csvfile)
        for row in csvreader:
            result.append(dict(row))

    for row in result:
        sentiment, percentage = analyzer.classify_sentiment(row['text'])
        row['sentiment'] = sentiment
        row['percentage'] = percentage
        
    with open("result.csv", "w", newline='') as csvfile:
        fieldnames = list(result[0].keys())
        csvwriter = csv.DictWriter(csvfile,fieldnames=fieldnames)
        
        csvwriter.writeheader()
        for row in result:
            csvwriter.writerow(row)
