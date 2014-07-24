import mock
import json
import unittest
import cPickle as pickle
import collections

from thefeeder.worker import Consumer
from thefeeder.worker import Worker
from thefeeder.worker import queue
from thefeeder.worker import queue_key
from thefeeder.worker import authenticated_filter
from thefeeder.worker import public_filter

class WorkersTestCase(unittest.TestCase):


    def setUp(self):
        with open('tests/original.json') as data_file:    
            dictionary = json.load(data_file, object_pairs_hook=collections.OrderedDict)
            # the authenticated data would be unadulterated original data
            self.expected_authenticated_data = dictionary
            pickled = pickle.dumps(dictionary)
            self.test_message = ('titles_queue', pickled)

        with open('tests/public.json') as data_file:
            self.expected_public_data = json.load(data_file, object_pairs_hook=collections.OrderedDict) 

        self.public_feed = 'http://search-api/load/public_titles'
        self.authenticated_feed= 'http://search-api/load/authenticated_titles'

        self.headers = {"Content-Type": "application/json"}


    def test_extract_public_data_from_message(self):
        public_data = public_filter(self.test_message)
        assert public_data == self.expected_public_data

    def test_extract_data_that_needs_authentication_from_message (self):
        private_data = authenticated_filter(self.test_message)
        assert private_data == self.expected_authenticated_data

    @mock.patch("requests.put")
    def test_worker_should_put_public_data_to_public_destination(self, mock_put):

        worker = Worker(self.public_feed, public_filter)
        worker.do_work(self.test_message)

        mock_put.assert_called_with(self.public_feed, data=json.dumps(self.expected_public_data), headers=self.headers)


    @mock.patch("requests.put")
    def test_worker_should_put_authenticated_data_to_authenicated_destination(self, mock_put):

        worker = Worker(self.authenticated_feed, authenticated_filter)
        worker.do_work(self.test_message)

        mock_put.assert_called_with(self.authenticated_feed, data=json.dumps(self.expected_authenticated_data), headers=self.headers)


    @mock.patch("redis.Redis.blpop")
    def test_consumer_should_pull_data_from_queue(self, mock_blpop):

        consumer = Consumer(queue, queue_key, [])
        consumer.get_next_message()

        mock_blpop.assert_called_with(queue_key)
