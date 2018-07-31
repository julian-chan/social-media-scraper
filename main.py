from utils.file_utils import join_post_files, join_comment_files
from utils.facebook_utils import split_fb_reactions
from utils.weibo_utils import convert_to_datetime
from utils.nlp_utils import NLP
import os
import time
import datetime
import ast


if __name__ == "__main__":

    # Get the folder name where the Facebook, Instagram, LinkedIn, Twitter, and Weibo folders are stored
    with open('key_params.json', 'r') as f:
        keys = ast.literal_eval(f.read())
        company = keys['parameters']['main']['company_folder']

    # If the designated folder doesn't exist, create it
    if company not in os.listdir('../'):
        os.mkdir('../{}'.format(company))

    # Record the start time of program execution
    start = time.time()

    print('*' * 75 + '\nPhase 1: Scraping...\n' + '*' * 75)
    # Scrape Facebook; runs the script on the command line
    os.system('python facebook.py')
    # Scrape Instagram; runs the script on the command line
    os.system('python instagram.py')
    print()
    # Scrape LinkedIn; runs the script on the command line
    os.system('python linkedin.py')
    # Scrape Twitter; runs the script on the command line
    os.system('python twitter.py')
    # Scrape Weibo; runs the script on the command line
    os.system('python weibo.py')


    print('*' * 75 + '\nPhase 2: Natural Language Processing...\n' + '*' * 75)

    # Instantiate NLP engine
    now = datetime.datetime.strftime(datetime.datetime.now(), '[%B %d, %Y %H:%M:%S]')
    print(now + " - Starting up NLP engine...\n")
    nlp = NLP()

    # Run NLP on Facebook posts
    nlp.process_nlp('../{}/Facebook'.format(company), company, 'Facebook post')

    # Run NLP on Facebook comments
    nlp.process_nlp('../{}/Facebook'.format(company), company, 'Facebook comment')

    # Run NLP on Weibo tweets
    nlp.process_nlp('../{}/Weibo'.format(company), company, 'Weibo tweet')

    # Run NLP on Instagram posts
    nlp.process_nlp('../{}/Instagram'.format(company), company, 'Instagram post')

    # Run NLP on Instagram comments
    nlp.process_nlp('../{}/Instagram'.format(company), company, 'Instagram comment')

    # Run NLP on Twitter tweets
    nlp.process_nlp('../{}/Twitter'.format(company), company, 'Twitter tweet')

    # Run NLP on LinkedIn posts
    nlp.process_nlp('../{}/LinkedIn'.format(company), company, 'LinkedIn post')

    # Run NLP on LinkedIn comments
    nlp.process_nlp('../{}/LinkedIn'.format(company), company, 'LinkedIn comment')

    # Delete NLP engine since we are not using anymore
    now = datetime.datetime.strftime(datetime.datetime.now(), '[%B %d, %Y %H:%M:%S]')
    print(now + " - Shutting down NLP engine...\n")
    del nlp
    NLP.count -= 1

    print('*' * 75 + '\nPhase 3: Post-Processing...\n' + '*' * 75)

    # Split Facebook reactions
    split_fb_reactions('../{}/Facebook'.format(company), company)

    # Convert Weibo dates to datetimes
    convert_to_datetime('../{}/Weibo'.format(company), company)

    # Combine all posts/tweets and comments into 2 big files
    join_post_files('../{}'.format(company), company)
    join_comment_files('../{}'.format(company), company)

    end = time.time()
    print("Entire process finished in {} seconds!".format(end-start))