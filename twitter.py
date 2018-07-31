import tweepy
import os
import ast
from utils.twitter_utils import get_profile_twitter, get_tweets_twitter


if __name__ == "__main__":
    # Read in the credentials and parameters from the key_params.json file
    with open('key_params.json', 'r') as f:
        keys = ast.literal_eval(f.read())
        api_key = keys['keys']['Twitter']['api_key']
        api_secret = keys['keys']['Twitter']['api_secret']
        access_token = keys['keys']['Twitter']['access_token']
        access_token_secret = keys['keys']['Twitter']['access_token_secret']
        twitter_handle = keys['parameters']['Twitter']['handle']
        company = keys['parameters']['main']['company_folder']

    # Create the API object using Twitter's OAuth system
    auth = tweepy.OAuthHandler(api_key, api_secret)
    auth.set_access_token(access_token, access_token_secret)
    api = tweepy.API(auth)

    # If the Twitter folder is not in the company folder already, create it
    if 'Twitter' not in os.listdir('../{}'.format(company)):
        os.mkdir('../{}/Twitter'.format(company))

    # Output files for the profile and tweets
    profile_output = "../{}/Twitter/{}_twitter_profile.csv".format(company, twitter_handle)
    tweet_output = "../{}/Twitter/{}_twitter_tweet.csv".format(company, twitter_handle)

    # Get the Twitter profile of the specified twitter handle and write it to profile_output
    get_profile_twitter(name=twitter_handle, api=api, output_file=profile_output)
    # Get the tweets of the specified twitter handle (at most num_tweets) and write it to tweet_output
    get_tweets_twitter(name=twitter_handle, api=api, output_file=tweet_output, num_tweets=1000)
