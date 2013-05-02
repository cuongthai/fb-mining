import datetime

import logging
import re
import urllib
import webapp2
from models import UserPost
import os

from mapreduce.lib import files
from google.appengine.api import taskqueue
from google.appengine.api import users
from google.appengine.ext import db
from mapreduce import base_handler
from mapreduce import mapreduce_pipeline
from mapreduce import operation as op
from mapreduce import shuffler
from google.appengine.ext.webapp import template
from google.appengine.ext import blobstore
from google.appengine.ext.webapp import blobstore_handlers
class FileMetadata(db.Model):
    __SEP = ".."
    __NEXT = "./"
    
    owner = db.StringProperty()
    uploadedOn = db.DateTimeProperty()
    source = db.StringProperty()
    blobkey = db.StringProperty()
    wordcount_link = db.StringProperty()

    @staticmethod
    def getFirstKeyForUser(username):

        return db.Key.from_path("FileMetadata", username + FileMetadata.__SEP)

    @staticmethod
    def getLastKeyForUser(username):

        return db.Key.from_path("FileMetadata", username + FileMetadata.__NEXT)

    @staticmethod
    def getKeyName(username, date):


        sep = FileMetadata.__SEP
        return str(username + sep + str(date) + sep)

class WordCountPipeline(base_handler.PipelineBase):
    """A pipeline to run Word count demo.

    Args:
    blobkey: blobkey to process as string. Should be a zip archive with
    text files inside.
    """

    def run(self, filekey):
        logging.debug("filename is %s" % filekey)
        output = yield mapreduce_pipeline.MapreducePipeline(
            "word_count",
            "mapreduce_handler.word_count_map",
            "mapreduce_handler.word_count_reduce",
            "mapreduce.input_readers.DatastoreInputReader",
            "mapreduce.output_writers.BlobstoreOutputWriter",
            mapper_params={
                "entity_kind": 'models.UserPost',
                "batch_size": 100,
            },
            reducer_params={
                "mime_type": "text/plain",
            },
            shards=32)
        yield StoreOutput("WordCount", filekey, output)

def word_count_map(data):
    """Word count map function."""
    #(entry, text_fn) = data
    #entities = text_fn()

    #for entity in entities:
    for liker in data.likers:
        yield ('%s_%s'%(data.user_id,liker),'')


def word_count_reduce(key, values):
    """Word count reduce function."""
    yield "%s: %d\n" % (key, len(values))

class MapReduceHandler(webapp2.RequestHandler):

    def get(self):
        source = "from datastore"

        username = 'cuong'
        date = datetime.datetime.now()
        key = FileMetadata.getKeyName(username, date)
        
        m = FileMetadata(key_name = key)
        m.owner = username
        m.uploadedOn = date
        m.source = source
        filekey = m.put()
        logging.debug(filekey)
        pipeline = WordCountPipeline(str(filekey))
        pipeline.start()
        logging.info("pipeline url %s"%pipeline.base_path + "/status?root=" + pipeline.pipeline_id)
        self.redirect(pipeline.base_path + "/status?root=" + pipeline.pipeline_id)

class StoreOutput(base_handler.PipelineBase):
    """A pipeline to store the result of the MapReduce job in the database.

    Args:
    mr_type: the type of mapreduce job run (e.g., WordCount, Index)
    encoded_key: the DB key corresponding to the metadata of this job
    output: the blobstore location where the output of the job is stored
    """

    def run(self, mr_type, encoded_key, output):
        logging.debug("output is %s" % str(output))
        key = db.Key(encoded=encoded_key)
        m = FileMetadata.get(key)
      
        if mr_type == "WordCount":
            m.wordcount_link = output[0]


        m.put()
class UploadHandler(blobstore_handlers.BlobstoreUploadHandler):
    """Handler to upload data to blobstore."""

    def post(self):
        source = "from datastore"

        username = 'cuong'
        date = datetime.datetime.now()
        key = FileMetadata.getKeyName(username, date)
        
        m = FileMetadata(key_name = key)
        m.owner = username
        m.uploadedOn = date
        m.source = source
        m.put()

        self.redirect("/admin/mapreduce/")


class DownloadHandler(blobstore_handlers.BlobstoreDownloadHandler):
    """Handler to download blob by blobkey."""

    def get(self, key):
        key = str(urllib.unquote(key)).strip()
        logging.debug("key is %s" % key)
        blob_info = blobstore.BlobInfo.get(key)
        self.send_blob(blob_info)
