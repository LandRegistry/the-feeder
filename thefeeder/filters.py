
from thefeeder import logger, public_message_validator
import base64
import json
import collections


def get_data(data, chain_name=u'type'):
    """
    Extract data from message.
    For head, chain_name == type
    For history, chain_name == history
    """
    message = data[u'message_envelope'][u'message']
    if message[u'chain_name'] == chain_name:
        return json.loads(base64.b64decode(message[u'message'][u'object'][u'data']), object_pairs_hook=collections.OrderedDict)
    else:
        return None


def authenticated_filter(data):
    # presumably return the data "as-is"
    return get_data(data)


def public_filter(data):
    """ Add only the data that is available to the public """
    try:
        data = get_data(data)
        return public_message_validator.to_canonical_form(data)
    except Exception as e:
        logger.error("There was an exception when filtering the data. Error: %s" % e)


def geo_filter(data):
    result = {}
    data = get_data(data)
    result['title_number'] = data['title_number']
    result['extent'] = data['extent']

    return result


def history_filter(data):
    return get_data(data, chain_name='history')
