from weibo_scraper import get_weibo_profile, get_weibo_tweets_by_name
import csv
import datetime
import time
import os
import re


def convert_to_datetime(readpath, company):
    """
    This function should primarily be used for Weibo tweet data because of their formatting in mm-dd.
    This function will change the format to yyyy-mm-dd using the CURRENT YEAR as yyyy.
    It assumes the date is in the first column of the .csv file.
    """
    for file in os.listdir(readpath):
        if 'nlp.csv' in file:
            readfile = file

            print("Converting dates (Weibo) in {} to datetimes...".format(readfile))
            start = time.time()

            date_column = 0    # column number (zero-indexed) of the date in the .csv file; can change as needed

            header_found = False

            writefile = os.path.join(readpath, '{}_weibo_tweet_final.csv'.format(company))

            # Keep all data to write at the end
            data_output = []

            with open(os.path.join(readpath, readfile), 'r') as rf:
                reader = csv.reader(rf)

                for row in reader:
                    if not header_found:
                        data_output.append(row)
                        header_found = True
                        continue
                    date = row[date_column]

                    # Only change it to datetime object if it is datetime-like; those that are like '16小时前' or
                    #   '昨天 12:00' should not be converted (must do so manually)
                    if date.count('-') == 1:
                        current_year = datetime.datetime.now().year
                        date = datetime.datetime.strptime(str(current_year) + '-' + date, '%Y-%m-%d')
                        row[date_column] = date
                        data_output.append(row)

            # Write the data to the output
            with open(writefile, 'w') as wf:
                writer = csv.writer(wf)

                for row in data_output:
                    writer.writerow(row)

            end = time.time()
            print("Finished converting dates in {} seconds!\n".format(end-start))


def cleanhtml(raw_html):
    """
    Clean up the raw html output by removing tags

    :param raw_html: (str) raw html string to be cleaned
    :return: cleantext: (str) string containing text between html tags
    """
    # Regular expression to extract text in between HTML <> tags
    cleanr = re.compile('<.*?>')
    cleantext = re.sub(cleanr, '', raw_html)

    return cleantext


def get_profile_weibo(name):
    print("Getting {}'s Weibo profile...".format(name))
    start = time.time()

    # Get profile of the user specified by name
    profile = get_weibo_profile(name)

    # Extract the data corresponding to the fields specified in columns
    columns = ["Screen Name", "Profile URL", "Gender",
               "Followers Count", "Follow Count", "Description", "ID"]
    data = [profile.screen_name, profile.profile_url, profile.gender,
            profile.followers_count, profile.follow_count, profile.description, profile.id]

    end = time.time()
    print("Successfully retrieved {}'s Weibo profile in {} seconds!\n".format(name, end-start))

    return columns, data


def get_tweets_weibo(name, output_file, pages=10):
    # Column names in the resulting csv file
    columns = ['Created at', 'Tweet', 'is_paid', 'num_reposts', 'num_comments', 'num_likes']
    # Write these column names to the file first
    with open(output_file, 'a') as wf:
        writer = csv.writer(wf)
        writer.writerow(columns)

    # Parameters to keep track of progress
    num_processed = 0  # total number of tweets processed thus far
    start = time.time()  # time at which scraping started
    all_tweets = []  # contains every tweet scraped; we need to return this to retrieve total # of likes for the user

    print("Getting {}'s Weibo tweets...".format(name))

    # Get 'pages' number of pages of tweets of the user specified by name
    response = get_weibo_tweets_by_name(name, pages)

    # Extract the data corresponding to the fields specified in columns
    for tweet in response:
        # Only get the tweet if it isn't retweeted (if it's retweeted, it means someone else wrote the tweet)
        if 'retweeted_status' not in tweet['mblog'].keys():
            all_tweets.append([tweet['mblog']['created_at'], cleanhtml(tweet['mblog']['text']),
                          tweet['mblog']['is_paid'], tweet['mblog']['reposts_count'],
                          tweet['mblog']['comments_count'], tweet['mblog']['attitudes_count']])

            num_processed += 1

        # Once our batch size reaches 100, we write it to the output file
        if num_processed % 100 == 0:
            print("Writing items {} to {} to {}...".format(num_processed - 99, num_processed, output_file))
            with open(output_file, 'a') as wf:
                writer = csv.writer(wf)
                for item in all_tweets[-100:]:
                    writer.writerow(item)
            print("Done writing!")
            print("{} tweets Processed: {}".format(num_processed, datetime.datetime.now()))

    # Ensure that if there are any remaining tweets, we write it to the output file
    if num_processed % 100 > 0:
        print("Writing remaining {} items to {}...".format(num_processed % 100, output_file))
        with open(output_file, 'a') as wf:
            writer = csv.writer(wf)
            for tweet in all_tweets[-(num_processed % 100):]:
                writer.writerow(tweet)
        print("Done writing!")

    end = time.time()
    print("Successfully retrieved {} of {}'s Weibo tweets in {} seconds!\n".format(num_processed, name, end-start))

    return columns, all_tweets