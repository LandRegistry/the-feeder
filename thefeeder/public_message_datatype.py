import collections
import logging

from datatypes.core import DictionaryValidator
from datatypes.validators import geo_json_validator, entry_validator
from voluptuous import All


schema = {
    "title_number": All(str),
    "extent": geo_json_validator.geo_json_schema,
    "property_description": entry_validator.entry_schema,
    "price_paid": entry_validator.entry_schema,
}

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger()
logger.addHandler(logging.StreamHandler())

class PublicMessageDatatype(DictionaryValidator):
    def to_canonical_form(self, data):
        # self.validate(data) TODO: this is not working

        filtered = {}

        for expected_key in schema.iterkeys():
            found = data.get(expected_key)

            if found:
                filtered[expected_key] = found

        return filtered

    def define_error_dictionary(self):
        return {
            "title_number": "title_number is a required field",
            "property_description": "property_description is a required field",
            "price_paid": "price_paid is a required field",
            "extent": "Extent must be well formed",
        }

    def define_schema(self):
        return schema
