import cPickle as pickle


def authenticated_filter(message):
    # presumably return the data "as-is"
    return pickle.loads(message[1])


def public_filter(message):
    """Removing sensitive data"""

    depickled = pickle.loads(message[1])
    leases = depickled.get('leases')
    if leases:
        for lease in leases:
            lessee_name = lease['lessee_name']
            proprietors = depickled.get('proprietors')
            for p in proprietors:
                if lessee_name == p.get('full_name'):
                    lease.pop('lessee_name')

    depickled.pop('proprietors', None)
    depickled.pop('charges', None)
    return depickled


def geo_filter(message):
    result = {}
    depickled = pickle.loads(message[1])
    result['title_number'] = depickled['title_number']
    result['extent'] = depickled['extent']

    return result