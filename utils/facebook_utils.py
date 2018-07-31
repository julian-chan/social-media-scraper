import os
import csv
import time
import datetime
import requests


# Splits reactions on each line to multiline reaction type and count (see docstring)
def split_fb_reactions(readpath, company):
    """
    This function will split up Facebook reactions into different rows.
    For example,

    / ----- / --------- / --------- / ------- /
    / ..... / num_likes / num_loves / num_sad /
    / ----- / --------- / --------- / ------- /
    / Row A /    10     /     8     /    4    /

                        |   |
                        |   |
                        |   |
                        .   .
                         . .
                          .

    / ----- / ------------- / ------------- /
    / ..... / reaction_type / num_reactions /
    / ----- / ------------- / ------------- /
    / Row A /     Like      /       10      /
    / Row A /     Love      /        8      /
    / Row A /     Sad       /        4      /
    """

    # The output will be placed in the same directory as the input
    writepath = readpath

    print("Splitting Facebook reactions in CSV files in {}...".format(readpath))
    start = time.time()

    # If the folder doesn't already exist, create it
    parent_dir, target_dir = writepath.rsplit('/', 1)
    if target_dir not in os.listdir(parent_dir):
        os.mkdir(writepath)

    # Go through every file in readpath and only split reactions for files that have already had NLP run on them (hence the _nlp.csv)
    # Name the new file the same except with _final.csv
    for readfile in os.listdir(readpath):
        if 'nlp.csv' in readfile:
            writefile = readfile[:-8] + '_final.csv'

            # Keep all data to write in batches
            data_output = []

            with open(os.path.join(readpath, readfile), 'r') as rf1:
                reader = csv.reader(rf1)
                header_found = False

                for row in reader:
                    # If the header has not been found yet, do some processing (see below comment) and mark it as found
                    # Rename the 7th column to num_total_reactions and add 4 columns as Channel, Shop, reaction_type, and reaction_count
                    if not header_found:
                        header = row
                        channel = 'Facebook'
                        header[6] = 'num_total_reactions'
                        if 'post' in readfile:
                            new_header = ['Channel', 'Shop'] + header[:9] + ['reaction_type', 'reaction_count'] + header[16:]
                        elif 'comment' in readfile:
                            new_header = ['Channel', 'Shop'] + header[:7] + ['reaction_type', 'reaction_count'] + header[14:]
                        data_output.append(new_header)
                        header_found = True
                    # The schema of posts and comments are different, so we need to take care of them separately
                    else:
                        if 'post' in readfile:
                            # Extract the data that is staying the same in the row
                            metadata = row[:6]
                            num_reactions = [row[6]]
                            num_comments = [row[7]]
                            num_shares = [row[8]]
                            nlp_stuff = row[16:]

                            # These columns are the reaction columns; this loop breaks them up into 7 different rows
                            for i in range(len(row[9:16])):
                                new_row = [channel, company] + metadata + num_reactions + num_comments + num_shares \
                                          + [header[9 + i][4:]] + [row[9 + i]] + nlp_stuff
                                data_output.append(new_row)

                        elif 'comment' in readfile:
                            # Extract the data that is staying the same in the row
                            metadata = row[:6]
                            num_reactions = [row[6]]
                            nlp_stuff = row[14:]

                            # These columns are the reaction columns; this loop breaks them up into 7 different rows
                            for i in range(len(row[7:14])):
                                new_row = [channel, company] + metadata + num_reactions + [header[7 + i][4:]] \
                                          + [row[7 + i]] + nlp_stuff
                                data_output.append(new_row)

            # Write all the data into the writefile
            with open(os.path.join(writepath, writefile), 'w') as wf1:
                writer = csv.writer(wf1)

                for row in data_output:
                    writer.writerow(row)

    end = time.time()
    print("Finished splitting Facebook reactions in {} seconds!\n".format(end-start))


# Facebook Scraper Object
# Contains functions for scraping posts, comments, profile, and engagements
class FacebookScraper(object):
    def __init__(self, access_token, page_name, since_date):
        # The root endpoint of the Facebook Graph API
        self.root = 'https://graph.facebook.com/v3.0/'
        # The node to get to page_name's posts
        self.node = '{}/posts/'.format(page_name)
        # Parameters such as how many items to limit to as well as the important ACCESS_TOKEN
        self.params = '?limit={}&access_token={}'.format(100, access_token)
        # Specify the furthest date to go back to when scraping for posts and comments
        self.since = "&since={}".format(since_date) if since_date is not '' else ''
        # Set the access token to be a private variable (not accessible from outside this object; hence the __
        self.__access_token = access_token
        # Set the page_name so this object has access to the variable
        self.page_name = page_name


    # Send request until success, wait for 5 seconds upon failure before next request
    def _request_until_success(self, url):
        success = False
        while success is False:
            # Send HTTP GET request to the specified url
            try:
                response = requests.get(url)
                if response.ok is True:
                    success = True
            # If the request fails, try again in 5 seconds
            except Exception as e:
                print(e)
                time.sleep(5)

                print("Error for URL {}: {}".format(url, datetime.datetime.now()))
                print("Retrying.")

        return response.json()


    # To deal with encoding Chinese/other arbitrary characters before writing to csv
    def _unicode_decode(self, text):
        try:
            return text.encode('utf-8').decode()
        except UnicodeDecodeError:
            return text.encode('utf-8')


    # Get the request url for retrieving posts; also specify the data fields that we want to retrieve
    def _get_post_feed_url(self, base_url):
        fields = "&fields=message,link,created_time,type,name,id,comments.limit(0).summary(true),shares,reactions" + \
                 ".limit(0).summary(true)"

        return base_url + fields


    # Get the request url for retrieving comments; also specify the data fields that we want to retrieve
    def _get_comment_feed_url(self, base_url):
        fields = "&fields=id,message,reactions.limit(0).summary(true)" + \
                 ",created_time,comments,from,attachment"

        return base_url + fields


    # Get the reactions for each post
    def _get_reactions_to_posts(self, base_url):
        reaction_types = ['like', 'love', 'wow', 'haha', 'sad', 'angry']
        reactions_dict = {}  # dict of {status_id: tuple<6>}

        for reaction_type in reaction_types:
            # Specify that we want the count of each reaction type
            fields = "&fields=reactions.type({}).limit(0).summary(total_count)".format(reaction_type.upper())
            url = base_url + fields
            # Request the data using an HTTP request
            data = self._request_until_success(url)['data']

            posts_seen = set()    # to remove duplicate posts
            for post in data:
                id = post['id']
                count = post['reactions']['summary']['total_count']
                posts_seen.add((id, count))

            for id, reaction_count in posts_seen:
                if id in reactions_dict:
                    reactions_dict[id] = reactions_dict[id] + (reaction_count,)
                else:
                    reactions_dict[id] = (reaction_count,)

        return reactions_dict


    # Get the reactions for each comment
    def _get_reactions_to_comments(self, base_url):
        reaction_types = ['like', 'love', 'wow', 'haha', 'sad', 'angry']
        reactions_dict = {}  # dict of {status_id: tuple<6>}

        for reaction_type in reaction_types:
            # Specify that we want the count of each reaction type
            fields = "&fields=reactions.type({}).limit(0).summary(total_count)".format(reaction_type.upper())
            url = base_url + fields
            # Request the data using an HTTP request
            data = self._request_until_success(url)['data']

            comments_seen = set()  # to remove duplicate comments
            for comment in data:
                id = comment['id']
                count = comment['reactions']['summary']['total_count']
                comments_seen.add((id, count))

            for id, reaction_count in comments_seen:
                if id in reactions_dict:
                    reactions_dict[id] = reactions_dict[id] + (reaction_count,)
                else:
                    reactions_dict[id] = (reaction_count,)

        return reactions_dict


    # Cleans the post data extracted from Facebook; encoding in UTF-8 and replacing non-existent values with 0
    def _process_post_reaction_data(self, post):
        # The post is a Python dictionary, so for top-level items, we can simply call the key.

        # Additionally, some items may not always exist, so must check for existence first

        post_id = post['id']
        post_type = post['type']

        # Ensures that if data is missing, we replace None with an empty string '' and that we properly handle character encoding
        post_message = '' if 'message' not in post else self._unicode_decode(post['message'])
        link_name = '' if 'name' not in post else self._unicode_decode(post['name'])
        post_link = '' if 'link' not in post else self._unicode_decode(post['link'])

        # Time needs special care since a) it's in UTC and
        # b) it's not easy to use in statistical programs.
        post_published = datetime.datetime.strptime(
            post['created_time'], '%Y-%m-%dT%H:%M:%S+0000')
        post_published = post_published + datetime.timedelta(hours=+8)  # Hong Kong time
        post_published = post_published.strftime('%Y-%m-%d %H:%M:%S')  # best time format for spreadsheet programs

        # Ensures that if data is missing, we don't access nonexistent fields; just replace it with 0
        num_reactions = 0 if 'reactions' not in post else post['reactions']['summary']['total_count']
        num_comments = 0 if 'comments' not in post else post['comments']['summary']['total_count']
        num_shares = 0 if 'shares' not in post else post['shares']['count']

        return (post_id, post_message, link_name, post_type, post_link,
                post_published, num_reactions, num_comments, num_shares)


    # Cleans the comment data extracted from Facebook; encoding in UTF-8 and replacing non-existent values with 0
    def _process_comment_reaction_data(self, comment, status_id, parent_id=''):
        # The comment is now a Python dictionary, so for top-level items, we can simply call the key.

        # Additionally, some items may not always exist, so must check for existence first

        comment_id = comment['id']

        # Ensures that if data is missing, we replace None with an empty string '' and that we properly handle character encoding
        comment_message = '' if 'message' not in comment or comment['message'] is '' else self._unicode_decode(comment['message'])
        if 'from' not in comment.keys():
            comment_author = None
        else:
            comment_author = self._unicode_decode(comment['from']['name'])

        # Ensures that if data is missing, we don't access nonexistent fields; just replace it with 0
        num_reactions = 0 if 'reactions' not in comment else comment['reactions']['summary']['total_count']

        if 'attachment' in comment:
            attachment_type = comment['attachment']['type']
            attachment_type = 'gif' if attachment_type == 'animated_image_share' else attachment_type
            attach_tag = "[[{}]]".format(attachment_type.upper())
            comment_message = attach_tag if comment_message is '' else comment_message + " " + attach_tag

        # Time needs special care since a) it's in UTC and
        # b) it's not easy to use in statistical programs.
        comment_published = datetime.datetime.strptime(comment['created_time'], '%Y-%m-%dT%H:%M:%S+0000')
        comment_published = comment_published + datetime.timedelta(hours=+8)  # Hong Kong time
        comment_published = comment_published.strftime('%Y-%m-%d %H:%M:%S')  # best time format for spreadsheet programs

        return (comment_id, status_id, parent_id, comment_message, comment_author,
                comment_published, num_reactions)


    # Main function to scrape posts and reactions
    def scrape_facebook_posts(self, output_file):
        # Column names in the resulting csv file
        columns = ["status_id", "status_message", "link_name", "status_type", "status_link", "status_published",
                   "num_reactions", "num_comments", "num_shares", "num_likes", "num_loves", "num_wows", "num_hahas",
                   "num_sads", "num_angrys", "num_special"]
        # Write these column names to the file first
        with open(output_file, 'a') as wf:
            writer = csv.writer(wf)
            writer.writerow(columns)

        # Parameters to keep track of progress
        has_next_page = True    # Facebook uses paging to handle long responses
        num_processed = 0       # total number of posts processed thus far
        start = time.time()     # time at which scraping started
        all_posts = []          # contains every post scraped; we need to return this to retrieve comments for each post

        print("Scraping {}'s Facebook page for posts...".format(self.page_name))

        after = ''
        # Keep looking for posts as long as there is another page of results
        while has_next_page:
            after = '' if after is '' else "&after={}".format(after)
            # Assemble the complete URL to send an HTTP GET request to
            base_url = self.root + self.node + self.params + after + self.since
            url = self._get_post_feed_url(base_url)

            # Request the data using an HTTP request
            posts = self._request_until_success(url)
            # Get the reactions to each of the posts
            reactions = self._get_reactions_to_posts(base_url)

            for post in posts['data']:

                # Ensure it is a post with the expected metadata
                if 'reactions' in post:
                    post_data = self._process_post_reaction_data(post)
                    reactions_data = reactions[post_data[0]]

                    # calculate thankful/pride through algebra
                    num_special = post_data[6] - sum(reactions_data)

                    # Add the post to our batch
                    all_posts.append(post_data + reactions_data + (num_special,))

                num_processed += 1

                # Once our batch size reaches 100, we write it to the output file
                if num_processed % 100 == 0:
                    print("Writing items {} to {} to {}...".format(num_processed - 99, num_processed, output_file))
                    with open(output_file, 'a') as wf:
                        writer = csv.writer(wf)
                        for row in all_posts[-100:]:
                            writer.writerow(row)
                    print("Done writing!")
                    print("{} Statuses Processed: {}".format(num_processed, datetime.datetime.now()))

            # If there is no next page, we're done.
            if 'paging' in posts:
                after = posts['paging']['cursors']['after']
            else:
                has_next_page = False

        # Ensure that if there are any remaining posts, we write it to the output file
        if num_processed % 100 > 0:
            print("Writing remaining {} items to {}...".format(num_processed % 100, output_file))
            with open(output_file, 'a') as wf:
                writer = csv.writer(wf)
                for row in all_posts[-(num_processed % 100):]:
                    writer.writerow(row)
            print("Done writing!")

        end = time.time()
        print("Successfully retrieved {} of {}'s statuses in {} seconds!\n".format(num_processed, self.page_name, end - start))

        return columns, all_posts


    # Main function to scrape comments and reactions
    def scrape_facebook_comments(self, posts, output_file):
        # Column names in the resulting csv file
        columns = ["comment_id", "status_id", "parent_id", "comment_message",
                       "comment_author", "comment_published", "num_reactions",
                       "num_likes", "num_loves", "num_wows", "num_hahas",
                       "num_sads", "num_angrys", "num_special"]
        # Write these column names to the file first
        with open(output_file, 'a') as wf:
            writer = csv.writer(wf)
            writer.writerow(columns)

        # Parameters to keep track of progress
        num_processed = 0       # total number of posts processed thus far
        start = time.time()     # time at which scraping started
        batch = []              # contains only 100 posts at a time (for batch output)

        print("Scraping {}'s Facebook page for comments...".format(self.page_name))

        after = ''
        # Go through each post, get its status id, and retrieve all comments corresponding to that status id
        for post in posts:
            has_next_page = True

            # Keep looking for comments as long as there is another page of results
            while has_next_page:

                # Assemble the complete URL to send an HTTP GET request to
                node = "/{}/comments".format(post['status_id'])
                after = '' if after is '' else "&after={}".format(after)
                base_url = self.root + node + self.params + after
                url = self._get_comment_feed_url(base_url)

                # Request the data using an HTTP request
                comments = self._request_until_success(url)
                # Get the reactions to each of the comments
                reactions = self._get_reactions_to_comments(base_url)

                # For each comment, also retrieve every comment to the comment
                for comment in comments['data']:
                    comment_data = self._process_comment_reaction_data(comment, post['status_id'])
                    reactions_data = reactions[comment_data[0]]

                    # calculate thankful/pride through algebra
                    num_special = comment_data[6] - sum(reactions_data)

                    # Add the comment to our batch
                    batch.append(comment_data + reactions_data + (num_special,))

                    num_processed += 1

                    if 'comments' in comment:
                        has_next_subpage = True
                        sub_after = ''

                        # Basically the same as before, except now we are looking for comments to comments instead of
                        #   comments to posts
                        while has_next_subpage:
                            sub_node = "/{}/comments".format(comment['id'])
                            sub_after = '' if sub_after is '' else "&after={}".format(
                                sub_after)
                            sub_base_url = self.root + sub_node + self.params + sub_after

                            sub_url = self._get_comment_feed_url(sub_base_url)
                            sub_comments = self._request_until_success(sub_url)
                            sub_reactions = self._get_reactions_to_comments(sub_base_url)

                            for sub_comment in sub_comments['data']:
                                sub_comment_data = self._process_comment_reaction_data(sub_comment, post['status_id'], comment['id'])
                                sub_reactions_data = sub_reactions[sub_comment_data[0]]

                                num_sub_special = sub_comment_data[6] - sum(sub_reactions_data)

                                # Add the comment to our batch
                                batch.append(sub_comment_data + sub_reactions_data + (num_sub_special,))

                                num_processed += 1

                                # Once our batch size reaches 100, we write it to the output file and clear the batch
                                if len(batch) == 100:
                                    print("Writing items {} to {} to {}...".format(num_processed - 99, num_processed,
                                                                                   output_file))
                                    with open(output_file, 'a') as wf:
                                        writer = csv.writer(wf)
                                        for row in batch:
                                            writer.writerow(row)
                                    batch = []
                                    print("Done writing!")
                                    print("{} Comments Processed: {}".format(num_processed, datetime.datetime.now()))

                            # Keep looking for comments to comments if there is another page of results
                            # If there is no next page, we're done.
                            if 'paging' in sub_comments:
                                if 'next' in sub_comments['paging']:
                                    sub_after = sub_comments['paging']['cursors']['after']
                                else:
                                    has_next_subpage = False
                            else:
                                has_next_subpage = False

                    # Once our batch size reaches 100, we write it to the output file and clear the batch
                    if len(batch) == 100:
                        print("Writing items {} to {} to {}...".format(num_processed - 99, num_processed, output_file))
                        with open(output_file, 'a') as wf:
                            writer = csv.writer(wf)
                            for row in batch:
                                writer.writerow(row)
                        batch = []
                        print("Done writing!")
                        print("{} Comments Processed: {}".format(num_processed, datetime.datetime.now()))

                # Keep looking for comments to posts if there is another page of results
                # If there is no next page, we're done.
                if 'paging' in comments:
                    if 'next' in comments['paging']:
                        after = comments['paging']['cursors']['after']
                    else:
                        has_next_page = False
                else:
                    has_next_page = False

        # Ensure that if there are any remaining comments, we write it to the output file
        if len(batch) > 0:
            print("Writing remaining {} items to {}...".format(len(batch), output_file))
            with open(output_file, 'a') as wf:
                writer = csv.writer(wf)
                for row in batch:
                    writer.writerow(row)
            print("Done writing!")

        end = time.time()
        print("Successfully retrieved {} of {}'s comments in {} seconds!\n".format(num_processed, self.page_name, end - start))


    # Gets profile data of the page
    def get_profile_facebook(self, output_file):
        print("Getting {}'s Facebook profile...".format(self.page_name))
        start = time.time()

        # Assemble the complete URL to send an HTTP GET request to
        link = self.root + self.page_name + '/'
        # Request the data using an HTTP request, passing in the necessary access_token parameter and specifying the
        #   data fields that we are interested in
        response = requests.get(link, params={'access_token': self.__access_token,
                                              'fields': 'id, about, engagement, fan_count, link, name, username'})
        # Convert the response to a JSON object
        response = response.json()

        columns = ['name', 'username', 'id', 'fan_count', 'link']
        output_data = [response[key] for key in columns]

        # Write the data to the output
        with open(output_file, 'a') as wf:
            writer = csv.writer(wf)
            writer.writerow(columns)
            writer.writerow(output_data)

        end = time.time()
        print("Successfully retrieved {}'s Facebook profile in {} seconds!\n".format(self.page_name, end - start))


    # Gets paid and organic impressions and engagements (at the day level)
    def get_daily_engagements_facebook(self, output_file, num_days=10):
        print("Getting {}'s Facebook engagements...".format(self.page_name))
        start = time.time()

        # Assemble the complete URLs to send an HTTP GET request to
        engagement = self.root + self.page_name + '/insights/page_content_activity_by_action_type_unique'
        total = self.root + self.page_name + '/insights/page_impressions'
        total_unique = self.root + self.page_name + '/insights/page_impressions_unique'
        organic = self.root + self.page_name + '/insights/page_impressions_organic'
        organic_unique = self.root + self.page_name + '/insights/page_impressions_organic_unique'
        paid = self.root + self.page_name + '/insights/page_impressions_paid'
        paid_unique = self.root + self.page_name + '/insights/page_impressions_paid_unique'

        lt_engagement = self.root + self.page_name + '/insights/post_activity_by_action_type_unique'
        lt_total = self.root + self.page_name + '/insights/post_impressions'
        lt_total_unique = self.root + self.page_name + '/insights/post_impressions_unique'
        lt_organic = self.root + self.page_name + '/insights/post_impressions_organic'
        lt_organic_unique = self.root + self.page_name + '/insights/post_impressions_organic_unique'
        lt_paid = self.root + self.page_name + '/insights/post_impressions_paid'
        lt_paid_unique = self.root + self.page_name + '/insights/post_impressions_paid_unique'

        # Column names in the resulting csv file
        columns = ["Date",
                   "Number of people talking about your Page's stories by Page Story type",
                   "Daily Total Impression by # of Times",
                   "Daily Total Impression by # of People",
                   "Organic Impression by # of Times (any content from or about your Page entered a person's screen through unpaid distribution)",
                   "Organic Impression by # of People (any content from or about your Page entered a person's screen through unpaid distribution)",
                   "Paid Impression by # of Times (any content from or about your Page entered a person's screen through paid distribution)",
                   "Paid Impression by # of People (any content from or about your Page entered a person's screen through paid distribution)",
                   "Lifetime Sum of Number of people talking about your Page's posts by Action type",
                   "Lifetime Total Impression by # of Times",
                   "Lifetime Total Impression by # of People",
                   "Lifetime Organic Impression by # of Times (any content from or about your Page entered a person's screen through unpaid distribution)",
                   "Lifetime Organic Impression by # of People (any content from or about your Page entered a person's screen through unpaid distribution)",
                   "Lifetime Paid Impression by # of Times (any content from or about your Page entered a person's screen through paid distribution)",
                   "Lifetime Paid Impression by # of People (any content from or about your Page entered a person's screen through paid distribution)"]
        # Write these column names to the file first
        with open(output_file, 'a') as wf:
            writer = csv.writer(wf)
            writer.writerow(columns)

        output_data = []

        # The loop does num_days // 2 + 1 because each response gives us 2 days worth of data (current and previous day).
        # This is to stay consistent with what the caller wants in num_days
        for _ in range(num_days // 2 + 1):
            # Request the data using an HTTP request, passing in the necessary access_token parameter
            e_resp = requests.get(engagement, params={'access_token': self.__access_token})
            t_resp = requests.get(total, params={'access_token': self.__access_token})
            tu_resp = requests.get(total_unique, params={'access_token': self.__access_token})
            o_resp = requests.get(organic, params={'access_token': self.__access_token})
            ou_resp = requests.get(organic_unique, params={'access_token': self.__access_token})
            p_resp = requests.get(paid, params={'access_token': self.__access_token})
            pu_resp = requests.get(paid_unique, params={'access_token': self.__access_token})

            lt_e_resp = requests.get(lt_engagement, params={'access_token': self.__access_token})
            lt_t_resp = requests.get(lt_total, params={'access_token': self.__access_token})
            lt_tu_resp = requests.get(lt_total_unique, params={'access_token': self.__access_token})
            lt_o_resp = requests.get(lt_organic, params={'access_token': self.__access_token})
            lt_ou_resp = requests.get(lt_organic_unique, params={'access_token': self.__access_token})
            lt_p_resp = requests.get(lt_paid, params={'access_token': self.__access_token})
            lt_pu_resp = requests.get(lt_paid_unique, params={'access_token': self.__access_token})

            # Convert the response to a JSON object
            e_resp = e_resp.json()
            t_resp = t_resp.json()
            tu_resp = tu_resp.json()
            o_resp = o_resp.json()
            ou_resp = ou_resp.json()
            p_resp = p_resp.json()
            pu_resp = pu_resp.json()

            lt_e_resp = lt_e_resp.json()
            lt_t_resp = lt_t_resp.json()
            lt_tu_resp = lt_tu_resp.json()
            lt_o_resp = lt_o_resp.json()
            lt_ou_resp = lt_ou_resp.json()
            lt_p_resp = lt_p_resp.json()
            lt_pu_resp = lt_pu_resp.json()

            # Access the data field in the response
            e_data = e_resp['data']
            t_data = t_resp['data']
            tu_data = tu_resp['data']
            o_data = o_resp['data']
            ou_data = ou_resp['data']
            p_data = p_resp['data']
            pu_data = pu_resp['data']

            lt_e_data = lt_e_resp['data']
            lt_t_data = lt_t_resp['data']
            lt_tu_data = lt_tu_resp['data']
            lt_o_data = lt_o_resp['data']
            lt_ou_data = lt_ou_resp['data']
            lt_p_data = lt_p_resp['data']
            lt_pu_data = lt_pu_resp['data']

            # Assemble the data corresponding to the column names; day 1 is the current day, day 2 is the previous day
            # Ensure that there is data to be accessed before trying to access nonexistent fields; otherwise, replace
            #   them with empty strings
            if len(lt_t_data) > 0:
                day1_data = (t_data[0]['values'][1]['end_time'], e_data[0]['values'][1]['value'],
                             t_data[0]['values'][1]['value'], tu_data[0]['values'][1]['value'],
                             o_data[0]['values'][1]['value'], ou_data[0]['values'][1]['value'],
                             p_data[0]['values'][1]['value'], pu_data[0]['values'][1]['value'],
                             lt_t_data[0]['values'][1]['end_time'], lt_e_data[0]['values'][1]['value'],
                             lt_t_data[0]['values'][1]['value'], lt_tu_data[0]['values'][1]['value'],
                             lt_o_data[0]['values'][1]['value'], lt_ou_data[0]['values'][1]['value'],
                             lt_p_data[0]['values'][1]['value'], lt_pu_data[0]['values'][1]['value'])

                day2_data = (t_data[0]['values'][0]['end_time'], e_data[0]['values'][0]['value'],
                             t_data[0]['values'][0]['value'], tu_data[0]['values'][0]['value'],
                             o_data[0]['values'][0]['value'], ou_data[0]['values'][0]['value'],
                             p_data[0]['values'][0]['value'], pu_data[0]['values'][0]['value'],
                             lt_t_data[0]['values'][0]['end_time'], lt_e_data[0]['values'][0]['value'],
                             lt_t_data[0]['values'][0]['value'], lt_tu_data[0]['values'][0]['value'],
                             lt_o_data[0]['values'][0]['value'], lt_ou_data[0]['values'][0]['value'],
                             lt_p_data[0]['values'][0]['value'], lt_pu_data[0]['values'][0]['value'])
            else:
                day1_data = (t_data[0]['values'][1]['end_time'], e_data[0]['values'][1]['value'],
                             t_data[0]['values'][1]['value'], tu_data[0]['values'][1]['value'],
                             o_data[0]['values'][1]['value'], ou_data[0]['values'][1]['value'],
                             p_data[0]['values'][1]['value'], pu_data[0]['values'][1]['value'],
                             '', '', '', '', '', '', '')

                day2_data = (t_data[0]['values'][0]['end_time'], e_data[0]['values'][0]['value'],
                             t_data[0]['values'][0]['value'], tu_data[0]['values'][0]['value'],
                             o_data[0]['values'][0]['value'], ou_data[0]['values'][0]['value'],
                             p_data[0]['values'][0]['value'], pu_data[0]['values'][0]['value'],
                             '', '', '', '', '', '', '')

            output_data.append(day1_data)
            output_data.append(day2_data)

            # Keep looking for engagements as long as there is another page of results
            engagement = e_resp['paging']['previous']
            total = t_resp['paging']['previous']
            total_unique = tu_resp['paging']['previous']
            organic = o_resp['paging']['previous']
            organic_unique = ou_resp['paging']['previous']
            paid = p_resp['paging']['previous']
            paid_unique = pu_resp['paging']['previous']

            lt_engagement = lt_e_resp['paging']['previous']
            lt_total = lt_t_resp['paging']['previous']
            lt_total_unique = lt_tu_resp['paging']['previous']
            lt_organic = lt_o_resp['paging']['previous']
            lt_organic_unique = lt_ou_resp['paging']['previous']
            lt_paid = lt_p_resp['paging']['previous']
            lt_paid_unique = lt_pu_resp['paging']['previous']

        # Write the data to the output
        with open(output_file, 'a') as wf:
            writer = csv.writer(wf)
            for row in output_data:
                writer.writerow(row)

        end = time.time()
        print("Successfully retrieved {}'s Facebook engagements in {} seconds!\n".format(self.page_name, end - start))
