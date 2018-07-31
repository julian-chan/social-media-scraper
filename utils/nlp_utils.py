from bosonnlp import BosonNLP, HTTPError
import datetime
import time
import os
import json
import csv

# Wrapper Object on BosonNLP Python SDK -- DON'T WORRY ABOUT THIS CLASS
class _BosonNLPWrapper(object):
    """
    NLP object using the BosonNLP API Python SDK.
    """

    news_categories = ['physical education', 'education', 'finance', 'society', 'entertainment', 'military',
                       'domestic', 'science and technology', 'the internet', 'real estate', 'international',
                       'women', 'car', 'game']

    def __init__(self, api_token=None):
        try:
            assert api_token is not None, "Please provide an API token"
        except AssertionError as e:
            raise

        self.token = api_token
        self.nlp = BosonNLP(self.token)


    def get_sentiment(self, text):
        """
        Performs sentiment analysis on a text passage (works for Chinese text).
        See: http://docs.bosonnlp.com/sentiment.html

        Parameters
        ----------
        text (string): text passage to be analyzed for sentiment


        Returns
        -------
        dictionary with 'positive' and 'negative' as keys with their respective weights as values

        >>> nlp = BosonNLPWrapper('')
        >>> nlp.get_sentiment('不要打擾我')
        {'positive': 0.3704911989140307, 'negative': 0.6295088010859693}
        >>> nlp.get_sentiment('我很高興跟你見面')
        {'positive': 0.856280735624867, 'negative': 0.14371926437513308}
        """
        pos, neg = self.nlp.sentiment(text)[0]

        return {'positive': pos, 'negative': neg}


    def classify_news(self, text):
        """
        Classifies news text into 14 different categories.
        See: http://docs.bosonnlp.com/classify.html

        Parameters
        ----------
        text (string): text passage to classify into news categories defined in news_categories

        Returns
        -------
        one of the 14 categories in news_categories that the text was classified into
        """
        numbering = range(len(_BosonNLPWrapper.news_categories))
        cats_dict = dict(zip(numbering, _BosonNLPWrapper.news_categories))

        clsfy_num = self.nlp.classify(text)[0]

        return cats_dict[clsfy_num]


    def extract_keywords(self, text, top_k=3):
        """
        Extracts the top k keywords and the weight of each word in the text.
        See: http://docs.bosonnlp.com/keywords.html

        Parameters
        ----------
        text (string): text passage from which to extract keywords
        top_k (integer): number of keywords to return

        Returns
        -------
        list of key-value pairs {word: weight}


        >>> nlp = BosonNLPWrapper('')
        >>> nlp.extract_keywords('我最愛老虎堂，奶茶香醇，波霸彈Q 好香的黑糖味')
        [{'波霸彈': 0.5980681967308248}, {'黑糖': 0.4699792421671365}, {'香醇': 0.4497614275300947}]
        """
        result = self.nlp.extract_keywords(text, top_k)  # outputs in sorted order of weight

        return [{result[i][1]: result[i][0]} for i in range(len(result))]


    def segment_words_and_tag(self, text):
        """
        Splits up text into segments of "words" and tags them with their respective part of speech.
        See: http://docs.bosonnlp.com/tag.html

        Parameters
        ----------
        text (string): text passage to segment into separate "words" and tags them with parts of speech

        Returns
        -------
        list of key-value pairs {word: part-of-speech-tag}
        """
        result = self.nlp.tag(text)[0]
        words = result['word']
        tags = result['tag']

        return [{words[i]: tags[i]} for i in range(len(words))]


    def get_summary(self, content, title='', pct_limit=0.2):
        """
        Extracts a new digest (summary) of the content.
        See: http://docs.bosonnlp.com/summary.html

        Parameters
        ----------
        text (string): text passage to summarize
        title (string): title of the passage (optional, may provide more accurate results)
        pct_limit (float): max length of the summary in terms of percentage of the original word count

        Returns
        -------
        string containing the summary of the passage
        """
        summary = self.nlp.summary(title, content, pct_limit)

        return summary


class NLP(object):
    """
    WARNING: Only instantiate this object once because multiple instances of this object will interfere with the
    internal tracking of API calls and handling of API limits.
    """
    count = 0

    def __init__(self):
        if NLP.count == 1:
            print("WARNING: Only instantiate this object once because multiple instances of this object will interfere with the internal tracking of API calls and handling of API limits.")
            return

        # Each API token can make 500 calls to the Sentiment Analysis API and Keyword Extraction API per day
        # For more information, see https://bosonnlp.com (can register for unlimited # of keys, can use any email)
        # Ex. random39@gmail.com (no email verification needed)
        self.tokens = ['eHVevhuQ.26503.5G4ulqIRsFFD', 'j4H_vex6.26470.x7bzYNdy5LGq', 'q8GRPikz.26486.drEpC5rH-4wC',
                       'rIH6NtSG.26487.inAY4OHWfflR', '_YN4K0kr.26508.kwGqCp8eR_lS', 'rBx1WA_5.26509.UsErJk_NBrJE',
                       'OmfefO6S.26510.XS6hmSkAniIl', 'LiNoocj5.26544.xz6heqD9HvHG', 'OAC68btw.26532.ZLjyAGek5E0N',
                       'vVSCqmBr.26533.pZ-Gv2wFvBg0', 'KnbRe6CD.26534.3D0jPLFYLCTv', 'EyKyRNXU.26535.pyvPqbEoB7uW',
                       'b7C8HCWA.26536.9tsKKP6aOAqp', 'sXWxkz2K.26543.Y6FjnNsHcuI7', 'MIbU2qM6.26537.8f78PtslMOcE',
                       'BtoV2XF_.26538.VN6hCAkANhoJ', 'w-8eHKON.26539.MIU5lk19hFfr', 'NFr4D0x7.26540.r7iD9Dnfd0CP',
                       'RuJlkgoZ.26493.A5AQp0hEHzs8', 'RAfPKECs.26494.NQYg6KHSDkOT', 'iYTQuusv.26495.tKuO6lI7NcMq',
                       'od7UCoAY.26496.kn8_nSKAkHFX', 'T2YaLNQ0.26497.QunI7kcseUt3', 'Sruc0TbK.26498._JzTpgXzWGv8',
                       'WI_aPtl0.26499.Jepfz1bJrXcs', 'X-G6t59F.26500.Z9MKWGiq1oiO', 'HiaVIJzA.26501.YIXOmsfG-QCL',
                       'akCejCBA.26624.uX7v8nApSs5t', 'fSisSiNy.26625.XP4uR2bqyj5z', 'bw2XKHBq.26626.2NIrxUwSeo2G']

        self.tokens_remaining = len(self.tokens)

        self.nlp = _BosonNLPWrapper(self.tokens[0])

        NLP.count += 1


    def __get_next_token(self):
        # Remove the rate limited token from the top of the stack
        old_token = self.tokens.pop(0)

        # Add it back to the bottom of the stack
        self.tokens.append(old_token)

        # Get the new token
        new_token = self.tokens[0]

        return new_token


    def __run_nlp(self, readpath, company, type, text_position):

        # For updating progress
        num_processed = 0

        # The output will be placed in the same directory as the input
        writepath = readpath

        # If the folder doesn't already exist, create it
        parent_dir, target_dir = writepath.rsplit('/', 1)
        if target_dir not in os.listdir(parent_dir):
            os.mkdir(writepath)

        print("Running Sentiment Analysis and Keyword Extraction on {}'s {}s...".format(company, type))
        start = time.time()

        for readfile in os.listdir(readpath):
            # Only run NLP on csv files with 'post', 'comment', or 'tweet' in their filenames
            if 'csv' in readfile and ('post' in readfile or 'comment' in readfile or 'tweet' in readfile):
                filetype = type.lower().replace(' ', '_')
                if filetype in readfile:
                    # LinkedIn and Twitter don't require further processing, so they can be _final after running NLP
                    if 'linkedin' in filetype or 'twitter' in filetype:
                        writefile = '{}_{}_final.csv'.format(company, filetype)
                    # Facebook needs to get reactions split
                    # Instagram is NOT dealt with in this function
                    # Weibo needs to have its datetimes converted
                    else:
                        writefile = '{}_{}_nlp.csv'.format(company, filetype)

                    # Contains only 100 posts at a time (for batch output)
                    batch = []

                    output_file = os.path.join(writepath, writefile)

                    with open(os.path.join(readpath, readfile), 'r') as rf:
                        reader = csv.reader(rf)
                        data = list(reader)
                        if len(data) <= 0:
                            continue
                        header = data[0]
                        # Add columns for NLP analysis in the schema
                        new_header = header + ['sentiment_pos', 'sentiment_neg', 'keyword', 'keyword_weight']
                        data = data[1:]

                    # Write the new header to the file first
                    with open(output_file, 'a') as wf:
                        writer = csv.writer(wf)
                        writer.writerow(new_header)

                    # Get only the text for every row
                    texts = [elem[text_position] for elem in data]

                    for i in range(len(texts)):
                        # Try to run sentiment analysis and keyword extraction
                        # If it fails due to an HTTPError, try another api token and if all are exhausted, then quit
                        while True:
                            try:
                                sentiment = self.nlp.get_sentiment(texts[i])
                                keywords = self.nlp.extract_keywords(texts[i])
                                break
                            except HTTPError:
                                if self.tokens_remaining <= 0:
                                    print("All API tokens exhausted.")
                                    exit()
                                self.tokens_remaining -= 1
                                new_token = self.__get_next_token()
                                print("Using new API token {}....".format(new_token))
                                self.nlp = _BosonNLPWrapper(new_token)

                        # Get the original data in the row
                        row = data[i]
                        # Get the positive and negative sentiment scores
                        pos, neg = sentiment['positive'], sentiment['negative']

                        # Add in a new row of data for each keyword detected
                        for word_weight in keywords:
                            word = list(word_weight)[0]
                            weight = word_weight[word]
                            new_row = row + [pos, neg, word, weight]
                            batch.append(new_row)
                            num_processed += 1

                            # Once our batch size reaches 100, we write it to the output file, and clear the batch
                            if len(batch) == 100:
                                print("Writing items {} to {} to {}...".format(num_processed - 99, num_processed,
                                                                               output_file))
                                with open(output_file, 'a') as wf:
                                    writer = csv.writer(wf)
                                    for row in batch:
                                        writer.writerow(row)
                                batch = []
                                print("Done writing!")
                                print("Processed {} {}s...".format(num_processed, type))

                        # If there are no keywords, just add in 1 row with an empty string and a significance score of 0
                        if len(keywords) == 0:
                            new_row = row + [pos, neg, '', 0]
                            batch.append(new_row)
                            num_processed += 1

                        # Once our batch size reaches 100, we write it to the output file, and clear the batch
                        if len(batch) == 100:
                            print("Writing items {} to {} to {}...".format(num_processed - 99, num_processed,
                                                                           output_file))
                            with open(output_file, 'a') as wf:
                                writer = csv.writer(wf)
                                for row in batch:
                                    writer.writerow(row)
                            batch = []
                            print("Done writing!")
                            print("Processed {} {}s...".format(num_processed, type))

                    # Ensure that if there are any remaining rows, we write it to the output file
                    if len(batch) > 0:
                        print("Writing remaining {} items to {}...".format(len(batch), output_file))
                        with open(output_file, 'a') as wf:
                            writer = csv.writer(wf)
                            for row in batch:
                                writer.writerow(row)
                        print("Done writing!")

        end = time.time()
        print("Finished running Sentiment Analysis and Keyword Extraction on {} {}s in {} seconds!\n"
              .format(num_processed, type, end - start))


    def __ig_posts_nlp(self, readpath, company):

        # For updating progress
        num_processed = 0

        # Contains only 100 posts at a time (for batch output)
        batch = []

        # The output will be placed in the same directory as the input
        writepath = readpath

        # If the folder doesn't already exist, create it
        parent_dir, target_dir = writepath.rsplit('/', 1)
        if target_dir not in os.listdir(parent_dir):
            os.mkdir(writepath)

        writefile = os.path.join(writepath, '{}_instagram_post_final.csv'.format(company))

        print("Running Sentiment Analysis and Keyword Extraction on {}'s Instagram posts...".format(company))
        start = time.time()

        header = ['Channel', 'Shop', 'status_published', 'status_id', 'status_link',
                  'caption', 'num_likes', 'sentiment_pos', 'sentiment_neg', 'keyword',
                  'keyword_weight']
        batch.append(header)

        for file in os.listdir(readpath):
            # Only run NLP on JSON files that are posts
            if 'UTC.json' in file and 'comments' not in file:
                with open(os.path.join(readpath, file), 'r') as rf:
                    # Extract relevant data from the JSON file
                    metadata = json.loads(rf.read())
                    channel = 'Instagram'
                    shop = metadata['node']['owner']['full_name']
                    status_published = file[:10]
                    status_id = metadata['node']['id']
                    status_link = metadata['node']['display_url']
                    caption = metadata['node']['edge_media_to_caption']['edges'][0]['node']['text']
                    num_likes = metadata['node']['edge_media_preview_like']['count']

                    # Try to run sentiment analysis and keyword extraction
                    # If it fails due to an HTTPError, try another api token and if all are exhausted, then quit
                    while True:
                        try:
                            sentiment = self.nlp.get_sentiment(caption)
                            keywords = self.nlp.extract_keywords(caption)
                            break
                        except HTTPError:
                            if self.tokens_remaining <= 0:
                                print("All API tokens exhausted.")
                                exit()
                            self.tokens_remaining -= 1
                            new_token = self.__get_next_token()
                            print("Using new API token {}...".format(new_token))
                            self.nlp = _BosonNLPWrapper(new_token)

                    # Get the positive and negative sentiment scores
                    pos, neg = sentiment['positive'], sentiment['negative']

                    # Add in a new row of data for each keyword detected
                    for word_weight in keywords:
                        word = list(word_weight)[0]
                        weight = word_weight[word]
                        new_row = [channel, shop, status_published, status_id, status_link,
                                   caption, num_likes, pos, neg, word, weight]
                        batch.append(new_row)

                    num_processed += 1

                    # Once our batch size reaches 100, we write it to the output file, and clear the batch
                    if len(batch) == 100:
                        print("Writing items {} to {} to {}...".format(num_processed - 99, num_processed, writefile))
                        with open(writefile, 'a') as wf:
                            writer = csv.writer(wf)
                            for row in batch:
                                writer.writerow(row)
                        batch = []
                        print("Done writing!")
                        print("Processed {} Instagram posts...".format(num_processed))

        # Ensure that if there are any remaining rows, we write it to the output file
        if len(batch) > 0:
            print("Writing remaining {} items to {}...".format(len(batch), writefile))
            with open(writefile, 'a') as wf:
                writer = csv.writer(wf)
                for row in batch:
                    writer.writerow(row)
            print("Done writing!")

        end = time.time()
        print("Finished running Sentiment Analysis and Keyword Extraction on {} Instagram posts in {} seconds!\n"
              .format(num_processed, end-start))


    def __ig_comments_nlp(self, readpath, company):

        # For updating progress
        num_processed = 0

        # Contains only 100 posts at a time (for batch output)
        batch = []

        # The output will be placed in the same directory as the input
        writepath = readpath

        # If the folder doesn't already exist, create it
        parent_dir, target_dir = writepath.rsplit('/', 1)
        if target_dir not in os.listdir(parent_dir):
            os.mkdir(writepath)

        writefile = os.path.join(writepath, '{}_instagram_comment_final.csv'.format(company))

        print("Running Sentiment Analysis and Keyword Extraction on {}'s Instagram comments...".format(company))
        start = time.time()

        header = ['Channel', 'comment_id', 'status_id', 'comment_published', 'comment',
                  'sentiment_pos', 'sentiment_neg', 'keyword', 'keyword_weight']
        batch.append(header)

        for file in os.listdir(readpath):
            # Only run NLP on JSON files that are comments
            if 'UTC.json' in file and 'comments' not in file:
                with open(os.path.join(readpath, file), 'r') as rf:
                    # Extract relevant data from the JSON file
                    metadata = json.loads(rf.read())
                    channel = 'Instagram'
                    status_id = metadata['node']['id']

                    date_published = []
                    comments = []
                    comment_ids = []
                    comments_file = file[:-5] + '_comments.json'
                    if comments_file in os.listdir(readpath):
                        with open(os.path.join(readpath, comments_file), 'r') as cf:
                            data = json.loads(cf.read())
                            for comment in data:
                                date = datetime.datetime.fromtimestamp(int(comment['created_at'])).strftime(
                                    '%Y-%m-%d %H:%M:%S')
                                date_published.append(date)
                                comments.append(comment['text'])
                                comment_ids.append(comment['id'])

                    for i in range(len(comments)):
                        # Try to run sentiment analysis and keyword extraction
                        # If it fails due to an HTTPError, try another api token and if all are exhausted, then quit
                        while True:
                            try:
                                sentiment = self.nlp.get_sentiment(comments[i])
                                keywords = self.nlp.extract_keywords(comments[i])
                                break
                            except HTTPError:
                                if self.tokens_remaining <= 0:
                                    print("All API tokens exhausted.")
                                    exit()
                                self.tokens_remaining -= 1
                                new_token = self.__get_next_token()
                                now = datetime.datetime.strftime(datetime.datetime.now(), '[%B %d, %Y %H:%M:%S]')
                                print("Using new API token {}...".format(new_token))
                                self.nlp = _BosonNLPWrapper(new_token)

                        # Get the positive and negative sentiment scores
                        pos, neg = sentiment['positive'], sentiment['negative']

                        # Add in a new row of data for each keyword detected
                        for word_weight in keywords:
                            word = list(word_weight)[0]
                            weight = word_weight[word]
                            new_row = [channel, comment_ids[i], status_id, date_published[i], comments[i],
                                       pos, neg, word, weight]
                            batch.append(new_row)

                        num_processed += 1

                        # Once our batch size reaches 100, we write it to the output file, and clear the batch
                        if len(batch) == 100:
                            print("Writing items {} to {} to {}...".format(num_processed - 99, num_processed, writefile))
                            with open(writefile, 'a') as wf:
                                writer = csv.writer(wf)
                                for row in batch:
                                    writer.writerow(row)
                            batch = []
                            print("Done writing!")
                            print("Processed {} Instagram comments...".format(num_processed))

        # Ensure that if there are any remaining rows, we write it to the output file
        if len(batch) > 0:
            print("Writing remaining {} items to {}...".format(len(batch), writefile))
            with open(writefile, 'a') as wf:
                writer = csv.writer(wf)
                for row in batch:
                    writer.writerow(row)
            print("Done writing!")

        end = time.time()
        print("Finished running Sentiment Analysis and Keyword Extraction on {} Instagram comments in {} seconds!\n"
              .format(num_processed, end-start))


    def process_nlp(self, readpath, company, type):
        filetype = type.lower()

        # Determine in which column the text is in depending on the fle type and call __run_nlp appropriately
        # Instagram is called separately because it has no columns and must perform file-joining in the process
        if filetype == 'facebook post':
            return self.__run_nlp(readpath, company, filetype, 1)
        elif filetype == 'facebook comment':
            return self.__run_nlp(readpath, company, filetype, 3)
        elif filetype == 'weibo tweet':
            return self.__run_nlp(readpath, company, filetype, 1)
        elif filetype == 'instagram post':
            return self.__ig_posts_nlp(readpath, company)
        elif filetype == 'instagram comment':
            return self.__ig_comments_nlp(readpath, company)
        elif filetype == 'twitter tweet':
            return self.__run_nlp(readpath, company, filetype, 2)
        elif filetype == 'linkedin post':
            return self.__run_nlp(readpath, company, filetype, 1)
        elif filetype == 'linkedin comment':
            return self.__run_nlp(readpath, company, filetype, 1)
