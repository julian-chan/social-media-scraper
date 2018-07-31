import os
import ast
from utils.facebook_utils import FacebookScraper


if __name__ == "__main__":
    # API TOKEN MUST BELONG TO USER WHO IS ADMIN/ANALYST/EDITOR OF THE PAGE
    # Read in the credentials and parameters from the key_params.json file
    with open('key_params.json', 'r') as f:
        keys = ast.literal_eval(f.read())
        api_token = keys['keys']['Facebook']['access_token']
        page_name = keys['parameters']['Facebook']['page_name']
        since_date = keys['parameters']['Facebook']['since']
        company = keys['parameters']['main']['company_folder']

    # If the Facebook folder is not in the company folder already, create it
    if 'Facebook' not in os.listdir('../{}'.format(company)):
        os.mkdir('../{}/Facebook'.format(company))

    # Output files for the profile, engagements, posts, and comments
    profile_output = "../{}/Facebook/{}_facebook_profile.csv".format(company, page_name)
    engagements_output = "../{}/Facebook/{}_facebook_engagements.csv".format(company, page_name)
    posts_output = "../{}/Facebook/{}_facebook_post.csv".format(company, page_name)
    comments_output = "../{}/Facebook/{}_facebook_comment.csv".format(company, page_name)

    # Create a Facebook Scraper object (see utils/facebook_utils.py for functions)
    # Specify how far to go back using the since_date parameter (YYYY-MM-DD)
    fbscraper = FacebookScraper(access_token=api_token, page_name=page_name, since_date=since_date)
    # Get the profile of the page_name specified in fbscraper and write to profile_output
    fbscraper.get_profile_facebook(output_file=profile_output)
    # Get the daily engagements of the page_name specified in fbscraper and write to engagements_output
    # Can also specify number of days to get data via the num_days parameter
    fbscraper.get_daily_engagements_facebook(output_file=engagements_output, num_days=100)

    # Get the posts of the page_name specified in fbscraper and write to posts_output
    post_columns, post_data = fbscraper.scrape_facebook_posts(output_file=posts_output)

    # Create a list of key-value pairs for each post for quick lookup when retrieving comments of each post
    posts = [dict(zip(post_columns, val)) for val in post_data]
    # Get the comments of the page_name specified in fbscraper and write to comments_output
    fbscraper.scrape_facebook_comments(posts=posts, output_file=comments_output)
