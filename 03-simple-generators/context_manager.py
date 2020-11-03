import csv
from collections import namedtuple
from contextlib import contextmanager
from itertools import islice


def get_dialect(f_name):
    with open(f_name) as f:
        return csv.Sniffer().sniff(f.read(1000))


@contextmanager
def parsed_data(f_name):
    f = open(f_name, 'r')
    try:
        reader = csv.reader(f, get_dialect(f_name))
        headers = map(lambda x: x.lower(), next(reader))
        nt = namedtuple('Data', headers)
        yield parsed_data_iter(reader, nt)
    finally:
        f.close()


with parsed_data('personal_info.csv') as data:
        for row in islice(data, 5):
            #print(row)
            x = row


@contextmanager
def parsed_data(f_name):
    def get_dialect(f_name):
        with open(f_name) as f:
            return csv.Sniffer().sniff(f.read(1000))
    
    def parsed_data_iter(data_iter, nt):
        for row in data_iter:
            yield nt(*row) 
        
    f = open(f_name, 'r')
    try:
        reader = csv.reader(f, get_dialect(f_name))
        headers = map(lambda x: x.lower(), next(reader))
        nt = namedtuple('Data', headers)
        yield parsed_data_iter(reader, nt)
    finally:
        f.close()

f_names = 'cars.csv', 'personal_info.csv'
for f_name in f_names:
    with parsed_data(f_name) as data:
        for row in islice(data, 5):
            #print(row)
    print('-------------------')


@contextmanager
def parsed_data(f_name):
    f = open(f_name, 'r')
    try:
        dialect = csv.Sniffer().sniff(f.read(1000))
        f.seek(0)
        reader = csv.reader(f, dialect)
        headers = map(lambda x: x.lower(), next(reader))
        nt = namedtuple('Data', headers)
        yield (nt(*row) for row in reader)
    finally:
        f.close()


f_names = 'cars.csv', 'personal_info.csv'
for f_name in f_names:
    with parsed_data(f_name) as data:
        for row in islice(data, 5):
            print(row)
    print('-------------------')