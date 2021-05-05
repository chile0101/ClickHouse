from collections import namedtuple
from functools import reduce


def merge(*records):
    """
    :param records: (varargs list of namedtuple) The patient details.
    :returns: (namedtuple) named Patient, containing details from all records, in entry order.
    """

    fields = []

    for record in records:
        fields.extend(record._fields)

    # fields = reduce(lambda x, y: x.extend(y._fields), [], records)

    Patient = namedtuple('Patient', fields)
    values = reduce(lambda x, y: x + y, records)

    return Patient(*values)


PersonalDetails = namedtuple('PersonalDetails', ['date_of_birth'])
personal_details = PersonalDetails(date_of_birth='06-04-1972')

Complexion = namedtuple('Complexion', ['eye_color', 'hair_color'])
complexion = Complexion(eye_color='Blue', hair_color='Black')

print(merge(personal_details, complexion))
