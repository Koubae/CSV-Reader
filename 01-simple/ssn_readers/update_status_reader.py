import csv
from itertools import islice
from collections import namedtuple
from datetime import datetime


update_status = r'ssn_readers\csv_dir/update_status.csv'


def csv_extractor(file_name): # General CSV extractor
    with open(file_name, 'r') as f:
        rows = csv.reader(f, delimiter=',', quotechar='"')
        yield from rows


def update_status_header():
    """
    Generates PersonalInfo Header
    :return: a namedtuple with CSV Fields
    """
    personal_csv_rows = csv_extractor(update_status)
    personal_csv_head = [row for row in islice(personal_csv_rows, 1)]
    PersonalInfo = namedtuple('PersonalInfo', *personal_csv_head)
    yield PersonalInfo


def update_status_extractor():
    """
    Extracts all rows information, dropping the csv Header
    :return: yields csv's row array
    """
    personal_csv_rows = csv_extractor(update_status)
    rows = islice(personal_csv_rows, 1, None)
    yield from rows


def clean_date(date):
    """

    :param date: strig format %Y-%m-%d %H:%M:%S, needs to remove the 'T' from the middle, expect data string like;
    2017-06-10T11:20:41Z
    :return: string like 2017-06-10 11:20:41Z
    """
    return ' '.join(date.split('T'))


def data_parser(row):
    """
    Converts Row Items into Python DataTypes, needs clean_date to split and remove the T from the date string
    :param row: List
    :return: List with converted Data
    """
    for idx, data in enumerate(row):
        try:
            value = datetime.strptime(clean_date(data), '%Y-%m-%d %H:%M:%S%z').date()
            row[idx] = value
        except ValueError:
            try:
                element = data.replace('-', '_')
                row[idx] = int(element)
            except ValueError as err:
                pass
    yield row


def up_gen_row():
    """
    Creates Named Tuple from each Row in the CSV file.
    Calls Personal_info_header for Header, Personal_inftoextractor for each row, converts row with data_parser
    :return: yields PersonalInfo => Namedtuple for each row
    """
    header = next(update_status_header())
    row = update_status_extractor()
    converted_row = data_parser(next(row))
    while True:
        try:
            new_row_instance = header(*next(converted_row))
            yield new_row_instance
            converted_row = data_parser(next(row))
        except StopIteration as err:
            print(StopIteration)
            break

def up_output_values():
    """
    Function that Yields All the CSV Fields and converts data into Python data
    :return: Yields a list of each CSV's row with converted Data.
    """
    row = update_status_extractor()
    converted_row = data_parser(next(row))
    while True:
        try:
            yield next(converted_row)
            converted_row = data_parser(next(row))
        except StopIteration as err:
            print(err)
            break