from .personal_info_reader import per_gen_row,  personal_info_header, info_output_values
from .vehicle_reader import vehi_gen_row, vehicles_info_header, vehi_output_values
from .update_status_reader import up_gen_row, update_status_header, up_output_values
from .employment_reader import employment_gen_row, employment_header, employment_output_values

csv_files = {
    'personal_info': 'csv_dir/personal_info.csv',
    'employment': 'csv_dir/employment.csv',
    'update_status': 'csv_dir/update_status.csv',
    'vehicles': 'csv_dir/vehicles.csv'
}

personal_info = csv_files['personal_info']
employment = csv_files['employment']
update_status = csv_files['update_status']
vehicles = csv_files['vehicles']

__all__ = [personal_info, employment, update_status, vehicles,
           per_gen_row, vehi_gen_row, up_gen_row,employment_gen_row,
           personal_info_header, vehicles_info_header, update_status_header, employment_header,
           info_output_values, vehi_output_values, up_output_values, employment_output_values]



