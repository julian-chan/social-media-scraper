import os
import time
import ast
from utils.instagram_utils import get_profile_instagram


if __name__ == "__main__":
    # Read in the credentials and parameters from the key_params.json file
    with open('key_params.json', 'r') as f:
        keys = ast.literal_eval(f.read())
        username = keys['keys']['Instagram']['username']
        password = keys['keys']['Instagram']['password']
        instagram_username = keys['parameters']['Instagram']['username']
        company = keys['parameters']['main']['company_folder']

    # If the Instagram folder is not in the company folder already, create it
    if 'Instagram' not in os.listdir('../{}'.format(company)):
        os.mkdir('../{}/Instagram'.format(company))

    # Output file for the profile
    profile_output = '../{}/Instagram/{}_instagram_profile.csv'.format(company, instagram_username)
    get_profile_instagram(name=instagram_username, output_file=profile_output)

    print("Getting {}'s Instagram posts and comments...".format(instagram_username))
    start = time.time()

    # If a username and password are specified in the key_params.json file, use it to login; otherwise, don't use it
    # os.system('command') runs 'command' on the command line
    if username and password:
        os.system('cd ../{}; instaloader {} -l {} -p {} --no-captions --no-profile-pic --no-compress-json --dirname-pattern Instagram -V -C'.format(company, instagram_username, username, password))
    else:
        os.system('cd ../{}; instaloader {} --no-captions --no-profile-pic --no-compress-json --dirname-pattern Instagram -V -C'.format(company, instagram_username))

    end = time.time()
    print("Finished retrieving {}'s Instagram posts and comments in {} seconds!".format(instagram_username, end - start))