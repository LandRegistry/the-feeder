import cPickle as pickle

from thefeeder import logger, public_message_validator


def authenticated_filter(message):
    # presumably return the data "as-is"
    return pickle.loads(message[1])


def public_filter(message):
    """ Add only the data that is available to the public """
    try:
        depickled = pickle.loads(message[1])
        return public_message_validator.to_canonical_form(depickled)
    except Exception as e:
        logger.error("There was an exception when filtering the data. Error: %s" % e)


def geo_filter(message):
    result = {}
    depickled = pickle.loads(message[1])
    result['title_number'] = depickled['title_number']
    result['extent'] = depickled['extent']

    return result