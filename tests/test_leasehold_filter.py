import json
import unittest
import cPickle as pickle
import collections
from thefeeder.worker import public_filter

class LeaseholdFilterTestCase(unittest.TestCase):


    def setUp(self):
        with open('tests/original_with_leasehold.json') as data_file:
            dictionary = json.load(data_file, object_pairs_hook=collections.OrderedDict)
            # the authenticated data would be unadulterated original data
            self.expected_authenticated_data = dictionary
            pickled = pickle.dumps(dictionary)
            self.test_message = ('titles_queue', pickled)

        with open('tests/public_with_leasehold.json') as data_file:
            self.expected_public_data = json.load(data_file, object_pairs_hook=collections.OrderedDict)

        self.public_feed = 'http://search-api/load/public_titles'
        self.authenticated_feed= 'http://search-api/load/authenticated_titles'

        self.headers = {"Content-Type": "application/json"}

    def test_extract_public_data_from_message(self):
        public_data = public_filter(self.test_message)
        self.assertEquals(public_data, self.expected_public_data)

