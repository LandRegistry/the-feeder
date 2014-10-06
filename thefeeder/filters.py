import cPickle as pickle

from thefeeder import logger, public_message_validator
import base64
import json
import collections


def pickle_loads_data(message):
    depickled = pickle.loads(message[1])
    return json.loads(base64.b64decode(depickled[u'object'][u'data']), object_pairs_hook=collections.OrderedDict)

def authenticated_filter(message):
    # presumably return the data "as-is"
    return pickle_loads_data(message)


def public_filter(message):
    """ Add only the data that is available to the public """
    try:
        depickled = pickle_loads_data(message)
        return public_message_validator.to_canonical_form(depickled)
    except Exception as e:
        logger.error("There was an exception when filtering the data. Error: %s" % e)


def geo_filter(message):
    result = {}
    depickled = pickle_loads_data(message)
    result['title_number'] = depickled['title_number']
    result['extent'] = depickled['extent']

    return result
