import pathlib
import os
from main_pipeline import MainPipeline, ExecutionRequest
from auto_traffic_counter_pipeline import AutoHourlyTrafficCounterPipeline
from train_punctuality_pipeline import TrainPunctualityPipeline
import pandas as pd
from pandas.testing import assert_frame_equal
from numpy import NaN
from sqlalchemy import create_engine
from datetime import datetime, date


def get_data_dir_path():
    # Get path to /data directory
    projects_path = pathlib.Path(__file__).parent.resolve()
    data_path = (projects_path / '..' / 'data').resolve()
    return data_path


# Check that output files and tables are created
def test_pipeline_output():
    # Get the database files that shall be created by the pipeline
    pipeline = MainPipeline()
    execution_reqs = pipeline.get_requested_executions()
    
    data_path = get_data_dir_path()        
    # Check if the /data directory contains output files
    #   -> current safe approach: tell user to delete them
    exisiting_output_files = []
    for root, dir, files in os.walk(data_path):
        for e_req in execution_reqs:
            if e_req.output_db in files:
                exisiting_output_files.append(e_req.output_db)
    
    # Remove duplicates
    exisiting_output_files = list(set(exisiting_output_files))
    
    if len(exisiting_output_files) > 0:
        print('Some output files of the pipeline already exist. Test will not be efficient.')
        print('For best testing please delete:', exisiting_output_files, '\n')
        # Testing will still be done, but less useful
    
    # Run the pipeline via the main pipelines main function
    pipeline.run() # TODO: UNCOMMENT
    
    print('\n------------------------------------')
    print('Testing existance of output files...')
    
    missing_output_files = []
    for root, dir, files in os.walk(data_path):
        for e_req in execution_reqs:
            if e_req.output_db not in files:
                missing_output_files.append(files)
    
    if len(missing_output_files) > 0:
        print('Unable to find following output files:', missing_output_files)
        
    assert len(missing_output_files) == 0
    print('All exepected output files were found! v/')

    # Get all tables that shall have been created per db
    db_tables_mapping = {}
    for e_req in execution_reqs:
        # Check for valid name
        if len(e_req.output_table) > 0:
            if e_req.output_db in db_tables_mapping:
                db_tables_mapping[e_req.output_db].append(e_req.output_table)
            else:
                db_tables_mapping[e_req.output_db] = [e_req.output_table]


    # Go through all db and all their tables, and compare wanted tables per db
    # to actually existing tables per db
    for db_name in db_tables_mapping:
        engine = create_engine('sqlite:///' + str(data_path / db_name))
        exisiting_tables = engine.table_names()
        for req_table in db_tables_mapping[db_name]:
            if req_table not in exisiting_tables:
                print('Table', req_table, 'not found in DB', db_name)
            
            assert req_table in db_tables_mapping[db_name]


                


###################################################
# Tests for the Auto Traffic Counter Pipeline

class TrafficPipelineTests():
    @classmethod
    def test_remove_duplicates(cls):
        pipeline = AutoHourlyTrafficCounterPipeline('', None, '')
        data = pd.DataFrame([['a', 'b', 'c'], [1, 2, 3], ['a', 'b', 'c']])
        pipeline.dataset_df = data
        assert data.shape == (3, 3)
        
        # Remove dups
        pipeline._remove_duplicate_rows()
        unique_data = pipeline.dataset_df
        assert data.shape == (2, 3)
        

    # More research needs to be done how int-columns are affected if no value is present. NaN is a float. Whole column float?         
    # @classmethod
    # def test_curate_hours(cls):
    #     pipeline = AutoHourlyTrafficCounterPipeline('', None, '')
    #     data = pd.DataFrame([['a', 1, 'b'], ['a', NaN, 'b'], ['a', 3, 'b']])
    #     data.columns = ['a', 'Stunde', 'b']
    #     pipeline.dataset_df = data
        
    #     pipeline._curate_hours()
    #     expected_df = pd.DataFrame([['a', 1, 'b'], ['a', 2, 'b'], ['a', 3, 'b']])
    #     expected_df.columns = ['a', 'Stunde', 'b']
        
    #     print(data)
    #     print(pipeline.dataset_df)
    #     print(expected_df)
        
    #     assert_frame_equal(pipeline.dataset_df, expected_df)
    @classmethod
    def test_merge_two_columns(cls):
        pipeline = AutoHourlyTrafficCounterPipeline('', None, '')
        data = pd.DataFrame([['a', 1, 'b'], ['a', 2, 'b'], ['a', 3, 'b']])
        data.columns = ['col1', 'Stunde', 'col2']
        
        pipeline.dataset_df = data
        pipeline._merge_two_columns('col1', 'col2', 'new_col')
        
        assert pipeline.dataset_df.shape == (3, 2)
        
    
    @classmethod
    def test_remove_columns(cls):
        pipeline = AutoHourlyTrafficCounterPipeline('', None, '')
        data = pd.DataFrame([['a', 1, 'b'], ['a', 2, 'b'], ['a', 3, 'b']])
        data.columns = ['col1', 'col2', 'col3']
        pipeline.dataset_df = data
        
        pipeline._remove_columns(['col1', 'col3'])
        assert data.shape == (3, 1)
        
    @classmethod
    def test_merge_hourly_to_daily(cls):
        pipeline = AutoHourlyTrafficCounterPipeline('', None, '')
        df = pd.DataFrame([
            [2125, 1173, 'Schleswig-Holstein', 'A7', 4, 'w', 5, 10, 20, 40, '-', '-', '-', '-', 1, 0, datetime(2020, 6, 10, 0)],
            [2125, 1173, 'Schleswig-Holstein', 'A7', 4, 's', 5, 10, 20, 40, '-', '-', '-', '-', 1, 0, datetime(2020, 6, 10, 1)],
            [2125, 1173, 'Schleswig-Holstein', 'A7', 4, 's', 5, 10, 20, 40, '-', 'u', '-', '-', 1, 0, datetime(2020, 6, 10, 2)],
            [2125, 1173, 'Schleswig-Holstein', 'A7', 4, 's', 5, 10, 20, 40, '-', 'u', '-', '-', 1, 0, datetime(2020, 6, 10, 3)],
            [2125, 1173, 'Schleswig-Holstein', 'A7', 4, 's', 5, 10, 20, 40, '-', 'u', '-', '-', 1, 0, datetime(2020, 6, 10, 4)],
            [2125, 1173, 'Schleswig-Holstein', 'A7', 4, 's', 5, 10, 20, 40, 'u', 'u', '-', '-', 1, 0, datetime(2020, 6, 10, 5)],
            [2125, 1173, 'Schleswig-Holstein', 'A7', 4, 's', 5, 10, 20, 40, 'u', 'u', '-', '-', 1, 0, datetime(2020, 6, 10, 6)],
            [2125, 1173, 'Schleswig-Holstein', 'A7', 4, 's', 5, 10, 20, 40, 'u', 'a', '-', '-', 1, 0, datetime(2020, 6, 10, 7)],
            [2125, 1173, 'Schleswig-Holstein', 'A7', 4, 'w', 5, 10, 20, 40, 'u', 'a', '-', '-', 1, 0, datetime(2020, 6, 10, 8)],
            [2125, 1173, 'Schleswig-Holstein', 'A7', 4, 'w', 5, 10, 20, 40, 'u', 'a', '-', '-', 1, 0, datetime(2020, 6, 10, 9)],
            [2125, 1173, 'Schleswig-Holstein', 'A7', 4, 'w', 5, 10, 20, 40, 'u', 'a', '-', '-', 1, 0, datetime(2020, 6, 10, 10)],
            [2125, 1173, 'Schleswig-Holstein', 'A7', 4, 'w', 5, 10, 20, 40, 'u', 'a', '-', '-', 1, 0, datetime(2020, 6, 10, 11)],
            [2125, 1173, 'Schleswig-Holstein', 'A7', 4, 'w', 5, 10, 20, 40, 'u', 'a', '-', '-', 1, 0, datetime(2020, 6, 10, 12)],
            [2125, 1173, 'Schleswig-Holstein', 'A7', 4, 'w', 5, 10, 20, 40, 'u', 'a', '-', '-', 1, 0, datetime(2020, 6, 10, 13)],
            [2125, 1173, 'Schleswig-Holstein', 'A7', 4, 'w', 5, 10, 20, 40, 'u', 'a', '-', '-', 1, 0, datetime(2020, 6, 10, 14)],
            [2125, 1173, 'Schleswig-Holstein', 'A7', 4, 'w', 5, 10, 20, 40, 'u', 'a', '-', '-', 1, 0, datetime(2020, 6, 10, 15)],
            [2125, 1173, 'Schleswig-Holstein', 'A7', 4, 'w', 5, 10, 20, 40, 'u', 'a', '-', '-', 1, 0, datetime(2020, 6, 10, 16)],
            [2125, 1173, 'Schleswig-Holstein', 'A7', 4, 'w', 5, 10, 20, 40, 'u', 'a', '-', '-', 1, 0, datetime(2020, 6, 10, 17)],
            [2125, 1173, 'Schleswig-Holstein', 'A7', 4, 'w', 5, 10, 20, 40, 'u', 'a', '-', '-', 1, 0, datetime(2020, 6, 10, 18)],
            [2125, 1173, 'Schleswig-Holstein', 'A7', 4, 'w', 5, 10, 20, 40, 'u', 'a', '-', '-', 1, 0, datetime(2020, 6, 10, 19)],
            [2125, 1173, 'Schleswig-Holstein', 'A7', 4, 'w', 5, 10, 20, 40, 'u', 'a', '-', '-', 1, 0, datetime(2020, 6, 10, 20)],
            [2125, 1173, 'Schleswig-Holstein', 'A7', 4, 'w', 5, 10, 20, 40, 'u', 'a', '-', '-', 1, 0, datetime(2020, 6, 10, 21)],
            [2125, 1173, 'Schleswig-Holstein', 'A7', 4, 'w', 5, 10, 20, 40, 'u', '-', '-', '-', 1, 0, datetime(2020, 6, 10, 22)],
            [2125, 1173, 'Schleswig-Holstein', 'A7', 4, 'w', 5, 10, 20, 40, '-', '-', '-', '-', 1, 0, datetime(2020, 6, 10, 23)],
            [2125, 1173, 'Schleswig-Holstein', 'A7', 4, 'w', 5, 10, 20, 40, '-', '-', '-', '-', 1, 0, datetime(2020, 6, 11, 0)],
            ])
        column_labels = ['tk_nr', 'counter_id', 'federal_state', 'street', 'weekday', 'day_type', 'car_dir1_cnt', 
                      'car_dir2_cnt', 'bus_dir1_cnt', 'bus_dir2_cnt', 'car_dir1_validity', 'car_dir2_validity', 'bus_dir1_validity', 
                      'bus_dir2_validity', 'curations', 'is_errornous', 'timestamp']
        
        df.columns = column_labels
        correct_merge_df = pd.DataFrame([[2125, 1173, 'Schleswig-Holstein', 'A7', 4, 'w', 5*24, 10*24, 20*24, 40*24, 'u', 'a', '-', '-', 24, 0, date(2020, 6, 10)]])
        correct_merge_df.columns = column_labels
        
        pipeline.dataset_df = df
        pipeline._merge_hourly_to_daily_measurements()

        assert_frame_equal(pipeline.dataset_df.reset_index(drop=True), correct_merge_df.reset_index(drop=True))



###################################################
# Tests for the Train Punctuality Pipeline

class TrainPuctualityPipelineTests():
    @classmethod
    def test_remove_duplicates(cls):
        pipeline = TrainPunctualityPipeline('', None, '')
        data = pd.DataFrame([['a', 'b', 'c'], ['1', '2', '3'] , ['a', 'b', 'c']])
        pipeline.dataset_df = data
        assert data.shape == (3, 3)
        
        # Remove dups
        pipeline._remove_duplicate_rows()
        unique_data = pipeline.dataset_df
        assert data.shape == (2, 3)




def main():
    # test_pipeline_output()
    
    # Traffic Counter Pipeline
    TrafficPipelineTests.test_remove_duplicates()
    # TrafficPipelineTests.test_curate_hours()
    TrafficPipelineTests.test_merge_two_columns()
    TrafficPipelineTests.test_remove_columns()
    TrafficPipelineTests.test_merge_hourly_to_daily()
    
    # Train Puncutality Pipeline
    TrainPuctualityPipelineTests.test_remove_duplicates()


if __name__ == '__main__':
    main()