import mock
import json
import unittest
from mock import call

from worker import ConsumerThread, Worker, queue, queue_key, extract_public, extract_private


class WorkersTestCase(unittest.TestCase):

    def setUp(self):
        self.test_message = ('titles_queue',
                '''{'proprietors': [
                        {'first_name': 'Gustavo', 'last_name': 'Fring'}, {'first_name': '', 'last_name': ''}],
                'title_number': 'TN1234567',
                'property': {'address':
                                {'house_number': '1', 'town': 'Albuquerque', 'postcode': '98765', 'road': 'Somewhere'},
                'class_of_title': 'qualified',
                'tenure': 'freehold'},
                'payment': {'titles': ['1234'],
                'price_paid': '987654321'}}''')


        self.expected_public_data = {'title_number': 'TN1234567',
                                        'house_number': '1',
                                        'town': 'Albuquerque',
                                        'postcode': '98765',
                                        'road': 'Somewhere',
                                         'price_paid': '987654321'}

        self.expected_private_data = {'proprietors': [
                                         {'first_name': 'Gustavo', 'last_name': 'Fring'},
                                         {'first_name': '', 'last_name': ''}],
                                            'title_number': 'TN1234567',
                                            'property':
                                               {'address':
                                                {'house_number': '1', 'town': 'Albuquerque', 'postcode': '98765', 'road':'Somewhere'},
                                            'class_of_title': 'qualified',
                                            'tenure': 'freehold'},
                                            'payment': {'titles': ['1234'],
                                            'price_paid': '987654321'}}

        self.public_feeds = ['http://public-titles-api/title', 'http://search-api/load']
        self.private_feeds = ['http://private-titles-api/title']


    def test_extract_public_data_from_message(self):
        public_data = extract_public(self.test_message)
        assert public_data == self.expected_public_data

    def test_extract_private_data_from_message (self):
        private_data = extract_private(self.test_message)
        assert private_data == self.expected_private_data

    @mock.patch("requests.put")
    def test_worker_should_put_public_data_to_destinations(self, mock_put):

        title_url = '%s/%s' % (self.public_feeds[0],  self.expected_public_data['title_number'])
        search_url = '%s/%s' % (self.public_feeds[1],  self.expected_public_data['title_number'])

        headers = {"Content-Type": "application/json"}

        worker = Worker(self.public_feeds, extract_public)
        worker.do_work(self.test_message)

        calls = [call(title_url, data=json.dumps(self.expected_public_data), headers=headers),
                 call(search_url, data=json.dumps(self.expected_public_data), headers=headers)]

        mock_put.assert_has_calls(calls, any_order=True)

    @mock.patch("requests.put")
    def test_worker_should_put_private_data_to_destinations(self, mock_put):

        private_title_url = '%s/%s' % (self.private_feeds[0],  self.expected_private_data['title_number'])

        headers = {"Content-Type": "application/json"}

        worker = Worker(self.private_feeds, extract_private)
        worker.do_work(self.test_message)

        mock_put.assert_called_with(private_title_url, data=json.dumps(self.expected_private_data), headers=headers)

    @mock.patch("redis.Redis.blpop")
    def test_consumer_should_pull_data_from_queue(self, mock_blpop):
        consumer = ConsumerThread(queue, queue_key, [])
        consumer.get_next_message()

        mock_blpop.assert_called_with(queue_key)
