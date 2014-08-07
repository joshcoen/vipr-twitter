from flask import Flask, render_template
from pprint import pprint, pformat
import logging
import requests
from threading import Thread
import boto
from boto.s3.key import Key
from boto.s3.lifecycle import Lifecycle, Expiration
import uuid
from TwitterAPI import TwitterAPI
from datetime import timedelta
from local_config import *
import random
import time
import os
from apscheduler.schedulers.background import BackgroundScheduler


scheduler = BackgroundScheduler()


from beaker.cache import CacheManager
from beaker.util import parse_cache_config_options

cache_opts = {
    'cache.type': 'memory',
}

cache = CacheManager(**parse_cache_config_options(cache_opts))

desired_hashtag = '#mtvhottest'
bucket_name = 'user34-s3'
min_items = 50
max_age = 300

logging.basicConfig(level=logging.INFO, format='%(asctime)s %(filename)s: '
                                '%(levelname)s: '
                                '%(funcName)s(): '
                                '%(lineno)d:\t'
                                '%(message)s')

app = Flask(__name__)
img_session = requests.Session()

s3conn = boto.connect_s3(akia,secret,host='object.vipronline.com')
bucket = s3conn.get_bucket(bucket_name)




def watch_tweet_stream(hashtag):
    api = TwitterAPI(
        twitter1,
        twitter2,
        twitter3,
        twitter4
    )
    r = api.request('statuses/filter', {'track':hashtag})
    for item in r.get_iterator():
        if 'entities' in item and 'media' in item['entities']:
            for tag in item['entities']['hashtags']:
                for media_object in item['entities']['media']:
                    if media_object['type'] == 'photo':
                            logging.info("Dispatching thread to capture for: " + media_object['media_url'] + " with hashtag: " + hashtag.lower())
                            #capture_photo_to_object(media_object['media_url'], hashtag.lower())
                            t = Thread(target=capture_photo_to_object,args=(media_object['media_url'], hashtag.lower()))
                            t.start()


def delete_old_keys(hashtag):
    list_of_keys = list(bucket.list(hashtag))
    current_time = time.time()
    to_delete = []
    for key in list_of_keys:
        hashtag,timestamp,guid = key.name.split('/')
        timestamp = int(timestamp)
        if len(list_of_keys) > min_items and current_time - max_age < timestamp:
            to_delete.append(key)
            list_of_keys.remove(key)

    logging.info("Target keys to delete:" + str(len(to_delete)) + pformat(to_delete))
    results = bucket.delete_keys(to_delete)
    logging.info("Deleted")
    logging.info(results.deleted)
    logging.info("Errors")
    logging.info(results.errors)



def capture_photo_to_object(url=None, hashtag=None):
    """

    :param url: string
    :return: :rtype: string
    """
    if url is None: return False

    expiry_time = timedelta(days=1)
    image_response = img_session.get(url)
    if image_response.status_code == 200:
        logging.debug("Grabbed image from %s" % url)
        guid = str(uuid.uuid4())
        k = Key(bucket)
        timestamp = str(int(time.time()))
        k.key = "/".join([hashtag,timestamp,guid])
        logging.debug("Uploading %s as %s" % (k.key,image_response.headers.get('content-type')))
        k.set_contents_from_string(image_response.content)
        return k.key
    else:
        logging.warning("Failed to grab %s" % url)
        return None

@cache.cache('get_keys_for_hashtag', expire=300)
def get_keys_for_hashtag(hashtag,return_limit=25,):
    candidates = list(bucket.list(hashtag))
    max_return = min(len(candidates),return_limit)
    return random.sample(candidates,max_return)

def get_image_url_from_key(key):
    return key.generate_url(1200)

watch = Thread(target=watch_tweet_stream, args=(desired_hashtag,))
watch.daemon = True
watch.start()


@app.route('/')
def dashboard():
    urls = [get_image_url_from_key(key) for key in get_keys_for_hashtag(desired_hashtag)]
    pprint(urls)
    return render_template('default.html',urls=urls)





if __name__ == '__main__':

    #delete_old_keys()
    # delete = Thread(target=delete_old_keys)
    # delete.start()
    #
    scheduler.add_job(delete_old_keys,'interval',args=(desired_hashtag,),minutes=1)
    scheduler.start()
    port = os.getenv('VCAP_APP_PORT', '5000')
    app.run(debug=False,port=int(port),host='0.0.0.0',threaded=True)

    #watch_tweet_stream("picture")
    # urls = [get_image_url_from_key(key) for key in get_keys_for_hashtag(desired_hashtag)]
    # pprint(urls)
