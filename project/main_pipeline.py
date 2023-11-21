import pathlib
from sqlalchemy import create_engine

from train_punctuality_pipeline import TrainPunctualityPipeline
from auto_traffic_counter_pipeline import AutoHourlyTrafficCounterPipeline


class MainPipeline():
    
    def __init__(self):
        # self.train_punc_engine = None
        # self.traffic_counter_engine = None
        pass
    
    def _create_databases(self):
        # Get path to /data directory
        projects_path = pathlib.Path(__file__).parent.resolve()
        print(projects_path)
        data_path = (projects_path / '..' / 'data').resolve()
        print(data_path)
        print((data_path / 'db.sqlite'))
        
        train_punc_db_name      = 'train_punctuality.sqlite'
        traffic_counter_1173_db_name = 'traffic_counter_1173.sqlite'
        traffic_counter_1157_db_name = 'traffic_counter_1157.sqlite'
        self.train_punc_engine           = create_engine('sqlite:///' + str(data_path / train_punc_db_name))
        self.traffic_counter_1173_engine = create_engine('sqlite:///' + str(data_path / traffic_counter_1173_db_name))
        self.traffic_counter_1157_engine = create_engine('sqlite:///' + str(data_path / traffic_counter_1157_db_name))
        
    
    def _start_dataset_pipelines(self):
        train_punctuality_pipeline = TrainPunctualityPipeline('https://opendata.schleswig-holstein.de/dataset/84256bd9-562c-4ea0-b0c6-908cd1e9e593/resource/c1407750-f05f-4715-8688-c0ff01b49131/download/puenktlichkeit.csv', self.train_punc_engine)
        print('\nStarting Train Punctuality Pipeline...')
        train_punctuality_pipeline.run()
        
        traffic_counter_1173_2010_pipeline = AutoHourlyTrafficCounterPipeline('https://www.bast.de/videos/2010/zst1173.zip', 
                                                                              self.traffic_counter_1173_engine, '2010')
        print('\nStarting Traffic Counter 1173 2010 Pipeline...')
        traffic_counter_1173_2010_pipeline.run()
        traffic_counter_1173_2011_pipeline = AutoHourlyTrafficCounterPipeline('https://www.bast.de/videos/2011/zst1173.zip', 
                                                                              self.traffic_counter_1173_engine, '2011')
        print('\nStarting Traffic Counter 1173 2011 Pipeline...')
        traffic_counter_1173_2011_pipeline.run()
        traffic_counter_1157_2010_pipeline = AutoHourlyTrafficCounterPipeline('https://www.bast.de/videos/2010/zst1157.zip', 
                                                                              self.traffic_counter_1157_engine, '2010')
        print('\nStarting Traffic Counter 1157 2010 Pipeline...')
        traffic_counter_1157_2010_pipeline.run()
        traffic_counter_1157_2011_pipeline = AutoHourlyTrafficCounterPipeline('https://www.bast.de/videos/2011/zst1157.zip', 
                                                                              self.traffic_counter_1157_engine, '2011')
        print('\nStarting Traffic Counter 1157 2011 Pipeline...')
        traffic_counter_1157_2011_pipeline.run()
        
        
    def run(self):
        self._create_databases()
        self._start_dataset_pipelines()
        
        
        
        
def main():
    pipeline = MainPipeline()
    pipeline.run()
    
    

if __name__ == '__main__':
    main()