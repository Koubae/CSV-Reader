import csv
from itertools import islice
from collections import namedtuple
from datetime import datetime


personal_info = r'ssn_readers\csv_dir\personal_info.csv'


def csv_extractor(file_name): # General CSV extractor
    with open(file_name, 'r') as f:
        rows = csv.reader(f, delimiter=',', quotechar='"')
        yield from rows


def personal_info_header():
    """
    Generates PersonalInfo Header
    :return: a namedtuple with CSV Fields
    """
    personal_csv_rows = csv_extractor(personal_info)
    personal_csv_head = [row for row in islice(personal_csv_rows, 1)]
    PersonalInfo = namedtuple('PersonalInfo', *personal_csv_head)
    yield PersonalInfo


def personal_info_extractor():
    """
    Extracts all rows information, dropping the csv Header
    :return: yields csv's row array
    """
    personal_csv_rows = csv_extractor(personal_info)
    rows = islice(personal_csv_rows, 1, None)
    yield from rows


def data_parser(row):
    """
    Converts Row Items into Python DataTypes
    :param row: List
    :return: List with converted Data
    """
    for idx, data in enumerate(row):
        try:
            value = datetime.strptime(data, '%d%m%Y').date()
            row[idx] = value
        except ValueError:
            try:
                element = data.replace('-', '_')
                row[idx] = int(element)
            except ValueError as err:
                pass
    yield row


def per_gen_row():
    """
    Creates Named Tuple from each Row in the CSV file.
    Calls Personal_info_header for Header, Personal_inftoextractor for each row, converts row with data_parser
    :return: yields PersonalInfo => Namedtuple for each row
    """
    header = next(personal_info_header())
    row = personal_info_extractor()
    converted_row = data_parser(next(row))
    while True:
        try:
            new_row_instance = header(*next(converted_row))
            yield new_row_instance
            converted_row = data_parser(next(row))
        except StopIteration as err:
            print(err)
            break


def info_output_values():
    """
    Function that Yields All the CSV Fields and converts data into Python data
    :return: Yields a list of each CSV's row with converted Data.
    """
    row = personal_info_extractor()
    converted_row = data_parser(next(row))
    while True:
        try:
            yield next(converted_row)
            converted_row = data_parser(next(row))
        except StopIteration as err:
            print(err)
            break
