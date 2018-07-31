import os
import csv
import time
import ast


def join_post_files(readpath, company):
    """
    This function joins the .csv files with 'posts' or 'tweets' in the filename together into a single .csv file.
    It will also append a column called "Channel" indicating which social media channel the data was extracted from.
    """
    # Keep all data to write at the end
    data_output = []

    writefile = os.path.join(readpath, 'posts.csv')

    print("Joining Posts/Tweets CSV files in {}...".format(readpath))
    start = time.time()

    header = ['Channel', 'Shop', 'status_id', 'status_message', 'link_name', 'status_type', 'status_link',
              'status_published', 'num_total_reactions', 'num_comments', 'num_shares', 'reaction_type',
              'reaction_count', 'sentiment_pos', 'sentiment_neg', 'keyword', 'keyword_weight']
    data_output.append(header)

    for social_media_source in os.listdir(readpath):
        social_media_source_path = os.path.join(readpath, social_media_source)
        if os.path.isdir(social_media_source_path):

            for readfile in os.listdir(social_media_source_path):
                if readfile.endswith('post_final.csv') or readfile.endswith('tweet_final.csv'):
                    header_found = False

                    # Facebook column names are the correct ones so no adjustment need to be made
                    if 'facebook' in readfile:
                        with open(os.path.join(social_media_source_path, readfile), 'r') as rf:
                            reader = csv.reader(rf)
                            for row in reader:
                                if not header_found:
                                    header_found = True
                                    continue
                                data_output.append(row)

                    # Weibo columns need to be adjusted to mean the same as Facebook columns; if no corresponding
                    #   column is found, leave it empty
                    elif 'weibo' in readfile:
                        with open(os.path.join(social_media_source_path, readfile), 'r') as rf:
                            reader = csv.reader(rf)
                            for row in reader:
                                if not header_found:
                                    header_found = True
                                    continue
                                new_row = [None] * len(header)
                                new_row[0] = 'Weibo'
                                new_row[1] = company
                                new_row[7] = row[0]
                                new_row[3] = row[1]
                                new_row[8] = row[5]
                                new_row[9] = row[4]
                                new_row[10] = row[3]
                                new_row[11] = 'likes'
                                new_row[12] = row[5]
                                new_row[13] = row[6]
                                new_row[14] = row[7]
                                new_row[15] = row[8]
                                new_row[16] = row[9]
                                data_output.append(new_row)

                    # Instagram columns need to be adjusted to mean the same as Facebook columns; if no corresponding
                    #   column is found, leave it empty
                    elif 'instagram' in readfile:
                        with open(os.path.join(social_media_source_path, readfile), 'r') as rf:
                            reader = csv.reader(rf)
                            for row in reader:
                                if not header_found:
                                    header_found = True
                                    continue
                                new_row = [None] * len(header)
                                new_row[0] = 'Instagram'
                                new_row[1] = company
                                new_row[2] = row[1]
                                new_row[7] = row[2]
                                new_row[2] = row[3]
                                new_row[6] = row[4]
                                new_row[3] = row[5]
                                new_row[8] = row[6]
                                new_row[11] = 'likes'
                                new_row[12] = row[6]
                                new_row[13] = row[7]
                                new_row[14] = row[8]
                                new_row[15] = row[9]
                                new_row[16] = row[10]
                                data_output.append(new_row)

                    # Twitter columns need to be adjusted to mean the same as Facebook columns; if no corresponding
                    #   column is found, leave it empty
                    elif 'twitter' in readfile:
                        with open(os.path.join(social_media_source_path, readfile), 'r') as rf:
                            reader = csv.reader(rf)
                            for row in reader:
                                if not header_found:
                                    header_found = True
                                    continue
                                new_row = [None] * len(header)
                                new_row[0] = 'Twitter'
                                new_row[1] = company
                                new_row[7] = row[0]
                                new_row[2] = row[1]
                                new_row[3] = row[2]
                                metadata = ast.literal_eval(row[4])
                                new_row[6] = metadata['urls'][0]['expanded_url'] if len(metadata['urls']) > 0 else ''
                                new_row[10] = row[8]
                                new_row[8] = row[9]
                                new_row[11] = 'likes'
                                new_row[12] = row[9]
                                new_row[13] = row[10]
                                new_row[14] = row[11]
                                new_row[15] = row[12]
                                new_row[16] = row[13]
                                data_output.append(new_row)

                    # LinkedIn columns need to be adjusted to mean the same as Facebook columns; if no corresponding
                    #   column is found, leave it empty
                    elif 'linkedin' in readfile:
                        with open(os.path.join(social_media_source_path, readfile), 'r') as rf:
                            reader = csv.reader(rf)

                            for row in reader:
                                if not header_found:
                                    header_found = True
                                    continue
                                new_row = [None] * len(header)
                                new_row[0] = 'LinkedIn'
                                new_row[1] = company
                                new_row[2] = row[0]
                                new_row[3] = row[1]
                                new_row[7] = row[2]
                                new_row[8] = row[4]
                                new_row[9] = row[3]
                                new_row[11] = 'likes'
                                new_row[12] = row[4]
                                new_row[13] = row[5]
                                new_row[14] = row[6]
                                new_row[15] = row[7]
                                new_row[16] = row[8]
                                data_output.append(new_row)


    # Write the data to the output
    with open(writefile, 'w') as wf:
        writer = csv.writer(wf)

        for row in data_output:
            writer.writerow(row)

    end = time.time()
    print("Finished joining files in {} seconds!\n".format(end-start))


def join_comment_files(readpath, company):
    """
        This function joins the .csv files with 'posts' in the filename together into a single .csv file.
        It will also append a column called "Channel" indicating which social media channel the data was extracted from.
        """
    # Keep all data to write at the end
    data_output = []

    writefile = os.path.join(readpath, 'comments.csv')

    print("Joining Comments CSV files in {}...".format(readpath))
    start = time.time()

    header = ['Channel', 'Shop', 'comment_id', 'status_id', 'parent_id', 'comment_message', 'comment_author',
              'comment_published', 'num_total_reactions', 'reaction_type', 'reaction_count', 'sentiment_pos',
              'sentiment_neg', 'keyword', 'keyword_weight']
    data_output.append(header)

    for social_media_source in os.listdir(readpath):
        social_media_source_path = os.path.join(readpath, social_media_source)
        if os.path.isdir(social_media_source_path):

            for readfile in os.listdir(social_media_source_path):
                if readfile.endswith('comment_final.csv'):
                    header_found = False

                    # Facebook column names are the correct ones so no adjustment need to be made
                    if 'facebook' in readfile:
                        with open(os.path.join(social_media_source_path, readfile), 'r') as rf:
                            reader = csv.reader(rf)
                            for row in reader:
                                if not header_found:
                                    header_found = True
                                    continue
                                data_output.append(row)

                    # Weibo has no comment data
                    elif 'weibo' in readfile:
                        continue

                    # Instagram columns need to be adjusted to mean the same as Facebook columns; if no corresponding
                    #   column is found, leave it empty
                    elif 'instagram' in readfile:
                        with open(os.path.join(social_media_source_path, readfile), 'r') as rf:
                            reader = csv.reader(rf)
                            for row in reader:
                                if not header_found:
                                    header_found = True
                                    continue
                                new_row = [None] * len(header)
                                new_row[0] = 'Instagram'
                                new_row[1] = company
                                new_row[2] = row[1]
                                new_row[3] = row[2]
                                new_row[7] = row[3]
                                new_row[5] = row[4]
                                new_row[11] = row[5]
                                new_row[12] = row[6]
                                new_row[13] = row[7]
                                new_row[14] = row[8]
                                data_output.append(new_row)

                    # LinkedIn columns need to be adjusted to mean the same as Facebook columns; if no corresponding
                    #   column is found, leave it empty
                    elif 'linkedin' in readfile:
                        with open(os.path.join(social_media_source_path, readfile), 'r') as rf:
                            reader = csv.reader(rf)
                            for row in reader:
                                if not header_found:
                                    header_found = True
                                    continue
                                new_row = [None] * len(header)
                                new_row[0] = 'LinkedIn'
                                new_row[1] = company
                                new_row[2] = row[0]
                                new_row[3] = row[1]
                                new_row[7] = row[2]
                                new_row[11] = row[3]
                                new_row[12] = row[4]
                                new_row[13] = row[5]
                                new_row[14] = row[6]
                                data_output.append(new_row)

    # Write the data to the output
    with open(writefile, 'w') as wf:
        writer = csv.writer(wf)

        for row in data_output:
            writer.writerow(row)

    end = time.time()
    print("Finished joining files in {} seconds!\n".format(end - start))