
import pandas as pd
from sqlalchemy import create_engine
import urllib.request
from zipfile import ZipFile

# zip_filename, headers = urllib.request.urlretrieve('https://gtfs.rhoenenergie-bus.de/GTFS.zip')

# print(zip_filename)

# df = None

# with ZipFile(zip_filename) as zip_file:
#     with zip_file.open('stops.txt') as stops_file:
        
#         df = pd.read_csv(stops_file)
        

# print(df.head(10))


class GtfsPipeline():
    
    zip_url = ''
    zip_filename = ''
    dataframe = None
    
    def __init__(self, zip_url: str):
        self.zip_url = zip_url
        
    
    def download_zip(self):
        self.zip_filename, headers = urllib.request.urlretrieve(self.zip_url)

    def extract_stops(self):
        with ZipFile(self.zip_filename) as zip_file:
            with zip_file.open('stops.txt') as stops_file:
                
                self.dataframe = pd.read_csv(stops_file)
    
    def drop_irrelevant_cols(self):
        self.dataframe = self.dataframe[['stop_id', 'stop_name', 'stop_lat', 'stop_lon', 'zone_id']]
    
    def drop_irrelevant_zones(self):
        allowed_zone_ids = [2001]
        self.dataframe = self.dataframe[self.dataframe['zone_id'].isin(allowed_zone_ids)]
        
    def drop_invalid_coords(self):
        MIN_ANGLE = -90
        MAX_ANGLE =  90
        self.dataframe = self.dataframe[(self.dataframe['stop_lat'] >= MIN_ANGLE) & (self.dataframe['stop_lat'] <= MAX_ANGLE) 
                                      & (self.dataframe['stop_lon'] >= MIN_ANGLE) & (self.dataframe['stop_lon'] <= MAX_ANGLE)]
    
    def drop_invalid_rows(self):
        self.dataframe = self.dataframe.dropna(axis='columns')
    
    def save_to_sqlite(self, db_name: str, table_name: str):
        engine = create_engine('sqlite:///' + db_name + '.sqlite')
        self.dataframe.to_sql(table_name, engine, if_exists='replace', index=False)
    
    def run(self):
        self.download_zip()
        self.extract_stops()
        self.drop_irrelevant_cols()
        self.drop_irrelevant_zones()
        self.drop_invalid_coords()
        self.drop_invalid_rows()
        print(self.dataframe.head())
        self.save_to_sqlite('gtfs', 'stops')
        
        
if __name__ == '__main__':
    URL = 'https://gtfs.rhoenenergie-bus.de/GTFS.zip'
    pipeline = GtfsPipeline(URL)
    pipeline.run()