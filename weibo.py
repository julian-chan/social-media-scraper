import csv
import os
import ast
from utils.weibo_utils import get_profile_weibo, get_tweets_weibo


def write_profile_to_csv(filename, column_names, data):
    with open(filename, 'w') as f:
        writer = csv.writer(f)
        writer.writerow(column_names)
        writer.writerow(data)


if __name__ == "__main__":
    # Read in the credentials and parameters from the key_params.json file
    with open('key_params.json', 'r') as f:
        keys = ast.literal_eval(f.read())
        weibo_name = keys['parameters']['Weibo']['username']
        company = keys['parameters']['main']['company_folder']

    # If the Weibo folder is not in the company folder already, create it
    if 'Weibo' not in os.listdir('../{}'.format(company)):
        os.mkdir('../{}/Weibo'.format(company))

    # Output files for the profile and tweets
    tweet_output = "../{}/Weibo/{}_weibo_tweet.csv".format(company, weibo_name)
    profile_output = "../{}/Weibo/{}_weibo_profile.csv".format(company, weibo_name)

    # Get the tweets of the Weibo user specified by weibo_name, and get at most 'pages' # of pages (can be changed)
    #   and write it to tweet_output
    tweet_column, tweet_data = get_tweets_weibo(name=weibo_name, output_file=tweet_output, pages=50)

    # Get the profile of the Weibo user specified by weibo_name
    profile_column, profile_data = get_profile_weibo(name=weibo_name)
    # Sum up the number of reposts, comments, and likes across all of the user's tweets and add this data to the profile
    profile_column += ['Total Number of Reposts', 'Total Number of Comments', 'Total Number of Likes']
    profile_data += [sum([data[3] for data in tweet_data]), sum([data[4] for data in tweet_data]), \
                    sum([data[5] for data in tweet_data])]
    # Write the profile to profile_output
    write_profile_to_csv(profile_output, profile_column, profile_data)
