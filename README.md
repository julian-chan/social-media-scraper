# social-media-scraper
Working as of August 1, 2018.

## Overview of this tool
* Get Facebook posts, comments, and engagements of a page that you are an admin, editor, or analyst of
* Get Instagram posts, likes, and comments of a public user OR of a private user that you follow (login required)
* Get LinkedIn profile, posts, likes, comments, engagements, and impressions of a company page you are an admin of
* Get Twitter profile and tweets of any Twitter user
* Get Weibo profile, tweets, and engagements of any Weibo user
* Run sentiment analysis and keyword extraction on bodies of text retrieved from social media
* Consolidate posts/tweets and comments into 2 big csv files

This tool is compatible with Python 3.6+.

## Python Dependencies
* [tweepy](http://tweepy.readthedocs.io/en/v3.5.0/)
* [instaloader](https://instaloader.github.io)
* [bosonnlp](http://bosonnlp-py.readthedocs.io)
* [weibo-scraper](https://github.com/Xarrow/weibo-scraper)

To install a missing `package`, run 

```pip install package```

## How to Use
Before running this script, it is important that you get the necessary access tokens and permissions from the various social media channels. See API_KeyGeneration.txt for instructions on how to acquire these tokens.

Once you get these tokens, make sure to paste them into key_params.json.

In addition, input the names of the Facebook and LinkedIn pages and Instagram, Twitter, and Weibo users into key_params.json.

Once you have acquired the necessary access tokens and parameters in key_params.json, run

```python run.py```

A status log will be output {date}_{time}_log.txt


