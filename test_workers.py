import mock
import json
import unittest
from mock import call

from worker import ConsumerThread, Worker, queue, queue_key, authenticated_filter, public_filter


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

        self.expected_authenticated_data = {'proprietors': [
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

        consumer = ConsumerThread(queue, queue_key, [])
        consumer.get_next_message()

        mock_blpop.assert_called_with(queue_key)
