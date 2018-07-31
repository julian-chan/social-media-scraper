import time
import csv
import datetime


def get_profile_twitter(name, api, output_file):
    print("Getting {}'s Twitter profile...".format(name))
    start = time.time()

    # Get the profile of the user corresponding to name
    user = api.get_user(screen_name=name)
    profile = list(user.__dict__.items())[1][1]

    columns = ['id_str', 'name', 'screen_name', 'location', 'description', 'url', 'followers_count', 'friends_count',
               'listed_count', 'created_at', 'favourites_count', 'verified', 'statuses_count',
               'profile_image_url_https',
               'profile_banner_url']

    data = [profile[key] for key in columns]

    # Write the data to the output
    with open(output_file, 'a') as wf:
        writer = csv.writer(wf)
        writer.writerow(columns)
        writer.writerow(data)

    end = time.time()
    print("Successfully retrieved {}'s Twitter profile in {} seconds!\n".format(name, end-start))


def get_tweets_twitter(name, api, output_file, num_tweets=100):
    # Column names in the resulting csv file
    columns = ['created_at', 'id_str', 'full_text', 'truncated', 'entities', 'source', 'in_reply_to_status_id',
               'in_reply_to_user_id', 'retweet_count', 'favorite_count']
    # Write these column names to the file first
    with open(output_file, 'a') as wf:
        writer = csv.writer(wf)
        writer.writerow(columns)

    # Parameters to keep track of progress
    num_processed = 0  # total number of tweets processed thus far
    start = time.time()  # time at which scraping started
    batch = []  # contains only 100 tweets at a time (for batch output)

    print("Getting {}'s Twitter tweets...".format(name))

    # Get the tweet timeline of the specified user, only considering at most 'count' tweets and expand all the
    #   tweets (because Twitter shortens tweets that are over 140 characters)
    for tweet in api.user_timeline(screen_name=name, count=num_tweets, tweet_mode='extended'):
        data = tweet._json
        # Only get the tweet if it isn't retweeted (if it's retweeted, it means someone else wrote the tweet)
        if 'retweeted_status' not in data.keys():
            # Extract the data corresponding to the fields listed in columns
            new_data = [data[key] for key in columns]
            batch.append(new_data)

            num_processed += 1

        # Once our batch size reaches 100, we write it to the output file and clear the batch
        if len(batch) == 100:
            print("Writing items {} to {} to {}...".format(num_processed - 99, num_processed, output_file))
            with open(output_file, 'a') as wf:
                writer = csv.writer(wf)
                for tweet_data in batch:
                    writer.writerow(tweet_data)
            batch = []
            print("Done writing!")
            print("{} tweets Processed: {}".format(num_processed, datetime.datetime.now()))

    # Ensure that if there are any remaining tweets, we write it to the output file
    if len(batch) > 0:
        print("Writing remaining {} items to {}...".format(len(batch), output_file))
        with open(output_file, 'a') as wf:
            writer = csv.writer(wf)
            for tweet in batch:
                writer.writerow(tweet)
        print("Done writing!")

    end = time.time()
    print("Successfully retrieved {} of {}'s Twitter tweets in {} seconds!\n".format(num_processed, name, end-start))