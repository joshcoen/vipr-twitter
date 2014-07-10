from flask import Flask, render_template
from pprint import pprint, pformat
import logging
import requests
from threading import Thread
from StringIO import StringIO
import tinys3
import uuid
from TwitterAPI import TwitterAPI
from datetime import timedelta
from redis import Redis
from local_config import *

import random
import os
import time



logging.basicConfig(level=logging.DEBUG)
app = Flask(__name__)
img_session = requests.Session()
url_prefix = "https://s3.amazonaws.com/com.exaforge.vipr-image-store/"
r_tweet_map = Redis(host='pub-redis-12630.us-east-1-1.1.ec2.garantiadata.com',port=12630,db=0,password='P@ssword1!')
pool = tinys3.Pool(akia,secret,tls=True, default_bucket='com.exaforge.vipr-image-store', )



def watch_tweet_stream():
    api = TwitterAPI(
        twitter1,
        twitter2,
        twitter3,
        twitter4
    )
    r = api.request('statuses/filter', {'track':'#vmworld,#emc,#sanfrancisco,#vmware'})
    for item in r.get_iterator():
        if 'entities' in item and 'media' in item['entities']:
            for hashtag in item['entities']['hashtags']:
                for media_object in item['entities']['media']:
                    if media_object['type'] == 'photo':
                        if hashtag['text'].lower() == "emc":  
                            logging.info("Dispatching thread to capture for: " + media_object['media_url'] + " with hashtag: " + hashtag['text'].lower())
                            t = Thread(target=capture_photo_to_s3,args=(media_object['media_url'], hashtag['text'].lower()))
                            t.start()
                        if hashtag['text'].lower() == "vmworld":  
                            logging.info("Dispatching thread to capture for: " + media_object['media_url'] + " with hashtag: " + hashtag['text'].lower())
                            t = Thread(target=capture_photo_to_s3,args=(media_object['media_url'], hashtag['text'].lower()))
                            t.start()
                        if hashtag['text'].lower() == "sanfrancisco":  
                            logging.info("Dispatching thread to capture for: " + media_object['media_url'] + " with hashtag: " + hashtag['text'].lower())
                            t = Thread(target=capture_photo_to_s3,args=(media_object['media_url'], hashtag['text'].lower()))
                            t.start()
                        if hashtag['text'].lower() == "vmware":  
                            logging.info("Dispatching thread to capture for: " + media_object['media_url'] + " with hashtag: " + hashtag['text'].lower())
                            t = Thread(target=capture_photo_to_s3,args=(media_object['media_url'], hashtag['text'].lower()))
                            t.start()                            

def capture_photo_to_s3(url=None, hashtag=None):
    """

    :param url: string
    :return: :rtype: string
    """
    if url is None: return False

    default_headers ={'x-amz-storage-class': 'REDUCED_REDUNDANCY'}
    expiry_time = timedelta(days=1)
    image_response = img_session.get(url)
    if image_response.status_code == 200:
        logging.info("Grabbed image from %s" % url)
        key = str(uuid.uuid4())
        logging.info("Uploading %s as %s" % (key,image_response.headers.get('content-type')))
        r = pool.upload(
            key=key,
            local_file=StringIO(image_response.content),
            content_type=image_response.headers.get('content-type'),
            headers=default_headers,
            expires=expiry_time
        )
        r_tweet_map.set(key,url_prefix+key,ex=86400)
        if hashtag == "emc":
            r_tweet_map.sadd("emc",url_prefix+key)
        if hashtag == "sanfrancisco":
            r_tweet_map.sadd("sanfrancisco",url_prefix+key)
        if hashtag == "vmworld":
            r_tweet_map.sadd("vmworld",url_prefix+key)
        if hashtag == "vmware":
            r_tweet_map.sadd("vmworld",url_prefix+key)
    else:
        logging.warning("Failed to grab %s" % url)
        return None

t = Thread(target=watch_tweet_stream, args=())
t.start()

@app.route('/')
def dashboard():
    sf_to_return = {}
    emc_to_return = {}
    sf_keys = r_tweet_map.smembers('sanfrancisco')
    emc_keys = r_tweet_map.smembers('emc')
    #all_keys = r_tweet_map.keys()
    for key in sf_keys:
        sf_to_return[key] = r_tweet_map.srandmember('sanfrancisco', number=None)
    

    for key in emc_keys:
        emc_to_return[key] = r_tweet_map.srandmember('emc', number=None)


    return render_template('default.html',sf_urls=random.sample(sf_to_return.items(),min(10,len(sf_keys))),emc_urls=random.sample(emc_to_return.items(),min(10,len(emc_keys))))


if __name__ == '__main__':
    port = os.getenv('VCAP_APP_PORT', '5000')
    logging.info("Running on port " + port)
    app.run(debug=False,port=int(port),host='0.0.0.0')
