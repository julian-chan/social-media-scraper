import requests
import time
import json
import csv
import datetime


def get_company_updates(cid, access_token, posts_output, comments_output):
    # URL to send an HTTP GET request to
    link = 'https://api.linkedin.com/v1/companies/{}/updates?format=json'.format(cid)

    print("Getting LinkedIn Company Updates...")
    start = time.time()

    # Request the data using an HTTP request, passing in the necessary access_token parameter
    response = requests.get(link, params={'oauth2_access_token': access_token})

    end = time.time()

    if response.ok:
        output = json.loads(response.text)

        # Posts
        with open(posts_output, 'a') as wf:
            writer = csv.writer(wf)
            header = ['status_id', 'status_message', 'status_published', 'num_comments', 'num_likes']
            writer.writerow(header)

            # Extract the relevant data fields and write it to posts_output
            for row in output['values']:
                new_row = [row['updateContent']['companyStatusUpdate']['share']['id'],
                           row['updateContent']['companyStatusUpdate']['share']['comment'],
                           (datetime.datetime.utcfromtimestamp(row['timestamp']/1000) + datetime.timedelta(hours=+8)).strftime('%Y-%m-%d %H:%M:%S'),
                           row['updateComments']['_total'], row['likes']['_total']]
                writer.writerow(new_row)

        # Comments
        with open(comments_output, 'a') as wf:
            writer = csv.writer(wf)
            header = ['comment_id', 'comment_message', 'comment_published']
            writer.writerow(header)

            # Extract the relevant data fields and write it to comments_output
            for row in output['values']:
                for comment in row['updateComments']['values']:
                    new_row = [comment['id'], comment['comment'],
                               (datetime.datetime.utcfromtimestamp(comment['timestamp']/1000) + datetime.timedelta(hours=+8)).strftime('%Y-%m-%d %H:%M:%S')]
                    writer.writerow(new_row)
        print("Successfully retrieved LinkedIn Company Updates in {} seconds!\n".format(end-start))
    else:
        print("An error occurred! Error code {}: {}".format(response.status_code, response.reason))


def get_historical_follower_data(cid, interval, access_token, output_file, from_ts='1514764800000'):
    # URL to send an HTTP GET request to
    link = 'https://api.linkedin.com/v1/companies/{}/historical-follow-statistics?format=json'.format(cid)

    print("Getting Historical Follower Data...")
    start = time.time()

    # Request the data using an HTTP request, passing in the necessary access_token parameter,
    #   when to start (UNIX timestamp in ms), and the interval between data points
    response = requests.get(link, params={'oauth2_access_token': access_token, 'start-timestamp': from_ts, 'time-granularity': interval})

    end = time.time()

    if response.ok:
        output = json.loads(response.text)

        with open(output_file, 'a') as wf:
            writer = csv.writer(wf)
            header = ['date', 'num_organic_followers', 'num_paid_followers', 'num_total_followers']
            writer.writerow(header)

            # Extract the relevant data fields and write it to the output
            for row in output['values']:
                new_row = [(datetime.datetime.utcfromtimestamp(row['time']/1000) + datetime.timedelta(hours=+8)).strftime('%Y-%m-%d %H:%M:%S'),
                           row['organicFollowerCount'], row['paidFollowerCount'], row['totalFollowerCount']]
                writer.writerow(new_row)
        print("Successfully retrieved Historical Follower Data in {} seconds!\n".format(end-start))
    else:
        print("An error occurred! Error code {}: {}".format(response.status_code, response.reason))


def get_historical_status_update_statistics(cid, interval, access_token, output_file, from_ts='1514764800000'):
    # URL to send an HTTP GET request to
    link = 'https://api.linkedin.com/v1/companies/{}/historical-status-update-statistics?format=json'.format(cid)

    print("Getting Historical Status Update Statistics...")
    start = time.time()

    # Request the data using an HTTP request, passing in the necessary access_token parameter,
    #   when to start (UNIX timestamp in ms), and the interval between data points
    response = requests.get(link, params={'oauth2_access_token': access_token, 'start-timestamp': from_ts, 'time-granularity': interval})

    end = time.time()

    if response.ok:
        output = json.loads(response.text)

        with open(output_file, 'a') as wf:
            writer = csv.writer(wf)
            header = ['date', 'num_impressions']
            writer.writerow(header)

            # Extract the relevant data fields and write it to the output
            for row in output['values']:
                new_row = [(datetime.datetime.utcfromtimestamp(row['time']/1000) + datetime.timedelta(hours=+8)).strftime('%Y-%m-%d %H:%M:%S'),
                           row['impressionCount']]
                writer.writerow(new_row)
        print("Successfully retrieved Historical Status Update Statistics in {} seconds!\n".format(end-start))
    else:
        print("An error occurred! Error code {}: {}".format(response.status_code, response.reason))


def get_company_follower_statistics(cid, access_token, output_file):
    # URL to send an HTTP GET request to
    link = 'https://api.linkedin.com/v1/companies/{}/company-statistics?format=json'.format(cid)

    print("Getting Company Statistics...")
    start = time.time()

    # Request the data using an HTTP request, passing in the necessary access_token parameter,
    #   when to start (UNIX timestamp in ms), and the interval between data points
    response = requests.get(link, params={'oauth2_access_token': access_token})

    end = time.time()

    if response.ok:
        output = json.loads(response.text)

        # Extract the relevant data fields and write it to the output
        with open(output_file, 'a') as wf:
            json_string = json.dumps(output, indent=4)
            wf.write(json_string)
        print("Successfully retrieved Company Statistics in {} seconds!\n".format(end-start))
    else:
        print("An error occurred! Error code {}: {}".format(response.status_code, response.reason))