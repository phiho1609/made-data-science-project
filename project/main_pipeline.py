import pathlib
from sqlalchemy import create_engine

from train_punctuality_pipeline import TrainPunctualityPipeline
from auto_traffic_counter_pipeline import AutoHourlyTrafficCounterPipeline

from trainline_traffic_counter_mapping import generate_counter_trainline_mapping
from bast_dataset_url_generator import generate_bast_dataset_url_file

''' Instances of ExecutionRequest describe a request to execute a pipeline with a dataset URL 
as input, and the expected / wished for output'''
class ExecutionRequest():
    
    def __init__(self, dataset_source: str, output_db: str, output_table: str, pipeline):
        self.dataset_source = dataset_source
        self.output_db = output_db
        self.output_table = output_table
        self.pipeline = pipeline


class MainPipeline():
    
    def __init__(self):
        self.requested_executions = []
        self.database_mappings = {}
        pass
    
    def get_requested_executions(self):
        # Build a list of pipeline-requests, which will be executed
        self.requested_executions = [
            ExecutionRequest('https://opendata.schleswig-holstein.de/dataset/84256bd9-562c-4ea0-b0c6-908cd1e9e593/resource/c1407750-f05f-4715-8688-c0ff01b49131/download/puenktlichkeit.csv',
                             'train_punctuality.sqlite', '', TrainPunctualityPipeline),
        ]
        
        # Get all needed counter from trainline-counter mapping
        counter_trainline_mapping = generate_counter_trainline_mapping()
        all_counters = [counter_key for counter_key in counter_trainline_mapping.keys()]
        years = range(2010, 2022)   # Train punctuality has useful values from 
        for counter in all_counters:
            for year in years:
                ex_req = ExecutionRequest('https://www.bast.de/videos/' + str(year) + '/zst' + str(counter) + '.zip', 
                                          'traffic_counter_' + str(counter) + '.sqlite', str(year), AutoHourlyTrafficCounterPipeline)
                self.requested_executions.append(ex_req)
        
        return self.requested_executions
    
    
    def _create_databases(self):
        # Get path to /data directory
        projects_path = pathlib.Path(__file__).parent.resolve()
        data_path = (projects_path / '..' / 'data').resolve()
        
        # Add engines for all requested databases
        for execution_request in self.requested_executions:
            if execution_request.output_db not in self.database_mappings:
                # Database engine not created yet -> create it
                engine = create_engine('sqlite:///' + str(data_path / execution_request.output_db))
                # Store engine in map, with name of db as key
                self.database_mappings[execution_request.output_db] = engine
        
    
    def _start_dataset_pipelines(self):
        
        for e_req in self.requested_executions:
            pipeline = e_req.pipeline(e_req.dataset_source, self.database_mappings[e_req.output_db], e_req.output_table)
            print('\nStarting pipeline', e_req.pipeline.__name__, '\nDataset Source:"', e_req.dataset_source, '"\nOutput: DB: "', e_req.output_db, '", Table: "', e_req.output_table, '"')
            print('------------------------------------------------\n')
            pipeline.run()
        
        
    def run(self):
        self.get_requested_executions()
        self._create_databases()
        self._start_dataset_pipelines()
        
        # Generate a suitable file containg a list of ALL BASt traffic counter datasets used
        generate_bast_dataset_url_file()
        
        
        
        
def main():
    pipeline = MainPipeline()
    pipeline.run()
    
    

if __name__ == '__main__':
    main()