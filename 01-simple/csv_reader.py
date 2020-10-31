import ssn_readers
from collections import namedtuple
from itertools import islice, chain, starmap, zip_longest, takewhile
from datetime import datetime
#
# __all__ = [personal_info, employment, update_status, vehicles,
#            per_gen_row, vehi_gen_row, up_gen_row,employment_gen_row]
# personal_info_header, vehicles_info_header, update_status_header, employment_header

person = ssn_readers.per_gen_row()
vehicle = ssn_readers.vehi_gen_row()
update = ssn_readers.up_gen_row()
employment = ssn_readers.employment_gen_row()

# These Valueables are Iterator, and can be called once only, then get exausted
h_person = ssn_readers.personal_info_header()
h_vehicle = ssn_readers.vehicles_info_header()
h_update = ssn_readers.update_status_header()
h_employment = ssn_readers.employment_header()

current_record_array = list()
state_record_array = list()


def combined_headers():
    """
    Calculates all the Header from CSV files
    :return: List of CSV's headers
    """

    get_heads = [field._fields for field in chain(h_person, h_employment, h_vehicle, h_update)]
    compress_heads = chain.from_iterable(get_heads)
    return list(dict.fromkeys(compress_heads, None).keys())


def combined_headers_forloop():
    """
    Equivalen to combined_headers but using a for loop
    :return: List of CSV's headers
    """

    compress_heads = list()
    get_heads = [field._fields for field in chain(h_person, h_employment, h_vehicle, h_update)]
    for values in get_heads:
        for value in values:
            if value not in compress_heads:
                compress_heads.append(value)
    return compress_heads


def employee_header(headers):
    """
    Generates  EmployeeHeader Header
    :return: a namedtuple with CSV Fields
    """
    Employee = namedtuple('Employee', headers)
    yield Employee


def employee_extractor():
    """
   Extracts Employee info from All CSV's files, dropping the csv Header
   :return: yields csv's row array
   """

    personal_stats = ssn_readers.info_output_values()
    employment_stats = ssn_readers.employment_output_values()
    vehicle_stats = ssn_readers.vehi_output_values()
    update_stats = ssn_readers.up_output_values()
    while True:
        try:
            compressed_data = chain.from_iterable([next(personal_stats), next(employment_stats), next(vehicle_stats), next(update_stats)])
            yield list(dict.fromkeys(compressed_data, None))
        except StopIteration as err:
            print(err)
            break


def generate_employee():
    """
    Creates Named Tuple from each Row in the CSV file.
    :return: yields Employee => Namedtuple for each row
    """
    header = next(employee_header(combined_headers()))
    row = employee_extractor()
    while True:
        try:
            new_employee = header(*next(row))
            yield new_employee
        except StopIteration as err:
            print(err)
            break



def state_record(employee_record):
    """
    Function that extract Not Updated date before the 3/1/2017, last_updated Employee namedtuple's attribute
    param employee_record: Employee Namedtuple instance
    :return: Lazy iterator Yields data Instances of Employee namedtuple in a state record < 3/1/2017
    """
    with open('state_record.txt', 'w+') as f:
        for line in employee_record:
            f.write(str(line) + '\n')


def current_record(employee_record):
    """
    Function that extract Updated data equal or later of 3/1/2017, last_updated Employee namedtuple's attribute
    :param employee_record: Employee Namedtuple instance
    :return: Lazy iterator Yields data Instances of Employee namedtuple in a state record >= 3/1/2017
    """
    print('here')
    with open('current_record.txt', 'w+') as f:
        for line in employee_record:
            f.write(str(line) + '\n')

# TODO create lazy iterator that extracts data from current_record_array & state_record_array
def split_data_dates(range_=None):
    """
    Lazy Iterator that splits updated row and insert them in current_data or state_date, also injects
    the data into the current_record_array & state_record_array = list()
    :param range_: int
    :return: Yield and writes updated data.
    """
    sentinel_date = datetime.strptime('3/1/2017', '%d/%m/%Y').date()
    employee = generate_employee()
    global current_record_array
    global state_record_array
    if not range_:
        for data in employee:
            if data.last_updated < sentinel_date:
                state_record_array.append(data)
            else:
                current_record_array.append(data)
    else:
        for index in range(range_):
            try:
                row = next(employee)
                if row.last_updated < sentinel_date:
                    state_record_array.append(row)
                else:
                    current_record_array.append(row)
            except StopIteration as err:
                print(err)
                pass
    yield current_record(current_record_array)
    yield state_record(state_record_array)


def max_gender_per_carmake():
    male_make = dict()
    female_make = dict()
    all_employee = generate_employee()
    for i in all_employee:
        vehicle_make = i.vehicle_make
        if i.gender.lower() == 'male':
            if male_make.get(vehicle_make):
                male_make[vehicle_make] += 1
            else:
                male_make.__setitem__(vehicle_make, 1)
        elif i.gender.lower() == 'female':
            if female_make.get(vehicle_make):
                female_make[vehicle_make] += 1
            else:
                female_make.__setitem__(vehicle_make, 1)


    max_make = max(male_make.values())
    max_female = max(female_make.values())
    result = dict()
    for k, i in male_make.items():
        if i == max_make:
            result.__setitem__('Male', {k:i})
    for k, i in female_make.items():
        if i == max_female:
            if result.get('Female'):
                prev = result.get('Female')
                result['Female'] = ({k: i}, prev)
            else:
                result.__setitem__('Female', {k:i})
    return result


