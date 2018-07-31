import os
import ast
from utils.linkedin_utils import get_company_updates, get_historical_follower_data, \
    get_historical_status_update_statistics, get_company_follower_statistics


if __name__ == "__main__":
    # Read in the credentials and parameters from the key_params.json file
    with open('key_params.json', 'r') as f:
        keys = ast.literal_eval(f.read())
        consumer_key = keys['keys']['LinkedIn']['consumer_key']
        consumer_secret = keys['keys']['LinkedIn']['consumer_secret']
        code = keys['keys']['LinkedIn']['code']
        access_token = keys['keys']['LinkedIn']['access_token']
        company_id = keys['parameters']['LinkedIn']['company_id']
        company = keys['parameters']['main']['company_folder']

    # If the LinkedIn folder is not in the company folder already, create it
    if 'LinkedIn' not in os.listdir('../{}'.format(company)):
        os.mkdir('../{}/LinkedIn'.format(company))

    # Output files for the posts, comments, historical followers, historical status update statistics, and
    #   company follower statistics
    posts_output = '../{}/LinkedIn/linkedin_post.csv'.format(company)
    comments_output = '../{}/LinkedIn/linkedin_comment.csv'.format(company)
    hist_follower_output = '../{}/LinkedIn/linkedin_historical_followers.csv'.format(company)
    hist_status_update_output = '../{}/LinkedIn/linkedin_historical_status_update_statistics.csv'.format(company)
    company_follower_output = '../{}/LinkedIn/linkedin_company_follower_statistics.json'.format(company)

    # Get the posts and comments to those posts of the company and write them, respectively, to posts_output and
    #   comments_output
    get_company_updates(cid=company_id, access_token=access_token, posts_output=posts_output, comments_output=comments_output)
    # Get the historical follower data (paid, organic, total), broken down by interval (which can be changed), and
    #   starting from from_ts (which is a UNIX timestamp in ms, see https://www.epochconverter.com), and write the
    #   output to hist_follower_output
    get_historical_follower_data(cid=company_id, interval='day', access_token=access_token, output_file=hist_follower_output, from_ts='1514764800000')
    # Get the historical status update statistics (number of impressions), broken down by interval
    #   (which can be changed), and starting from from_ts (which is a UNIX timestamp in ms,
    #   see https://www.epochconverter.com), and write it to hist_status_update_output
    get_historical_status_update_statistics(cid=company_id, interval='day', access_token=access_token, output_file=hist_status_update_output, from_ts='1514764800000')
    # Get the company follower statistics and write it to company_follower_output
    get_company_follower_statistics(cid=company_id, access_token=access_token, output_file=company_follower_output)
