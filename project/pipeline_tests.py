import pathlib
import os
from main_pipeline import MainPipeline, ExecutionRequest
# from main_pipeline import main as main_pipeline_main


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
                






def main():
    test_pipeline_output()


if __name__ == '__main__':
    main()