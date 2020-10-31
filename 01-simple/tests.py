from csv_reader import *


def test_csv_read():

    # 4 Lazy iterators, they extracts data from CSV file
    assert list(person)
    assert list(vehicle)
    assert list(update)
    assert list(employment)

    # Lazy Iterators, getting Header from 4 CSV, got exhausted
    # assert next(h_person)._fields
    # assert next(h_vehicle)._fields
    # assert next(h_update)._fields
    # assert next(h_employment)._fields

    # headers = combined_headers()
    # print(headers)
    # ['ssn', 'first_name', 'last_name', 'gender', 'language', 'employer',
    # 'department', 'employee_id', 'vehicle_make', 'vehicle_model',
    # 'model_year', 'last_updated', 'created']

    # employees_all = employee_extractor()
    # assert list(employees_all)

    employee = generate_employee()
    print(list(employee))


if __name__ == '__main__':
    test_csv_read()