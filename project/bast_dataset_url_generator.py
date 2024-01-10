import pandas as pd
import pathlib
import os
from sqlalchemy import create_engine
from trainline_traffic_counter_mapping import generate_counter_trainline_mapping

def get_data_dir_path():
    # Get path to /data directory
    projects_path = pathlib.Path(__file__).parent.resolve()
    data_path = (projects_path / '..' / 'data').resolve()
    return data_path

def get_project_dir_path():
    projects_path = pathlib.Path(__file__).parent.resolve()
    return projects_path


def generate_bast_dataset_url_file():
    # Go through all possibly used datasets, check which are available, 
    # and save the dataset url if available
    # for counter in counter_nr_list:
    #     for year in range(start_year, end_year+1):
    #         dataset_url = 'https://www.bast.de/videos/' + str(year) + '/zst' + str(counter) + '.zip'
    #         try:
    #             pd.read_csv(dataset_url, sep=None, engine='python')   # Setting sep=None lets pd depict delimiter automatically
    #         except:
    #             continue
            
    #         bast_dataset_url_list.append(dataset_url)

    used_traffic_counter_datasets = {}

    data_dir_path = get_data_dir_path()
    for root, dir, files in os.walk(data_dir_path):
        for filename in files:
            if len(filename) == len('traffic_counter_0000.sqlite') and filename[0:16] == 'traffic_counter_':
                # This is seen as a traffic counter sqlite db file
                # 1. Extract counter nr
                counter_nr = int(filename[16:20])
                # 2. Get all table names
                engine = create_engine('sqlite:///' + str(data_dir_path / filename))
                table_names = engine.table_names()
                # print('for filename', filename, 'with counter nr', counter_nr, 'tables:', table_names)
                used_traffic_counter_datasets.update({counter_nr: table_names})
    
    # Generate list of dataset URLs from counter NRs and table names (=years)
    bast_dataset_url_list = []
    for counter_nr, year_str_list in used_traffic_counter_datasets.items():
        for year_str in year_str_list:
            dataset_url = 'https://www.bast.de/videos/' + year_str + '/zst' + str(counter_nr) + '.zip'
            bast_dataset_url_list.append(dataset_url)
    
    # Overwrite BASt dataset urls file
    bast_url_filename = (get_project_dir_path() / 'bast_all_dataset_urls.md').resolve()
    with open(bast_url_filename, "w") as f:
        f.writelines(['# List of used BASt dataset URLs\n\n', 
                     'This is a auto-generated file, called from the main-pipeline.',
                     'The list is produced, by inspecting the SQLite databases in the /data directory.',
                     'This list contains all URLs to the BASt traffic counter datasets found in the /data directory:\n\n'])
        
        for bast_dataset_url in bast_dataset_url_list:
            f.writelines(['* ' + bast_dataset_url + '\n'])
            
        f.flush()
            
            
            
            
# generate_bast_dataset_url_file([1178, 1183, 1186, 1187], 2010, 2023)
generate_bast_dataset_url_file()