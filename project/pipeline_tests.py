import pathlib
import os
from main_pipeline import MainPipeline, ExecutionRequest
from auto_traffic_counter_pipeline import AutoHourlyTrafficCounterPipeline
from train_punctuality_pipeline import TrainPunctualityPipeline
import pandas as pd
from pandas.testing import assert_frame_equal
from numpy import NaN


def get_data_dir_path():
    # Get path to /data directory
    projects_path = pathlib.Path(__file__).parent.resolve()
    # print(projects_path)
    data_path = (projects_path / '..' / 'data').resolve()
    # print(data_path)
    return data_path


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
    pipeline.run()
    
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
    test_pipeline_output()
    
    # Traffic Counter Pipeline
    TrafficPipelineTests.test_remove_duplicates()
    # TrafficPipelineTests.test_curate_hours()
    TrafficPipelineTests.test_merge_two_columns()
    TrafficPipelineTests.test_remove_columns()
    
    # Train Puncutality Pipeline
    TrainPuctualityPipelineTests.test_remove_duplicates()


if __name__ == '__main__':
    main()