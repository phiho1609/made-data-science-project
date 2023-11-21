import pandas as pd
import numpy as np
from sqlalchemy import create_engine
from datetime import date, time, datetime, timedelta, MINYEAR
import calendar


class TrainPunctualityPipeline():
    
    # Init the pipeline
    def __init__(self, dataset_url: str, db_engine):
        self.dataset_url = dataset_url
        self.db_engine = db_engine
    
    
    # Pull the data from the net
    def _pull_dataset(self):
        self.dataset_df = pd.read_csv(self.dataset_url, sep=None)   # Setting sep=None lets pd depict delimiter automatically
        # print(self.dataset_df.head(5))
        
    
    # Put data from pandas dataframe into a new sqlite table
    def _convert_df_to_dbtable(self):
        self.dataset_df.to_sql('All', self.db_engine, if_exists='replace', index=False)
    
    def _convert_dfs_to_dbtables(self, named_dfs):
        # Convert multiple (via params given) dfs into tables
        for name, df in named_dfs:
            df.to_sql(name, self.db_engine, if_exists='replace', index=False)
    
    def _remove_duplicate_rows(self):
        # Remove duplicated rows and print result
        row_cnt_before = len(self.dataset_df)
        self.dataset_df.drop_duplicates(inplace=True)   # inplace to modify exisiting df
        row_cnt_after = len(self.dataset_df)
        
        if row_cnt_before != row_cnt_after:
            print('Removed', row_cnt_before - row_cnt_after, 'duplicate rows!')
        else:
            print('No duplicate rows detected!')
            
        # Reindex, to not have missing indices from dropped rows
        self.dataset_df.reset_index(drop=True, inplace=True)    # drop=True: do not insert missing indices
        
    
    def _curate_errornous_rows(self):
        # Set pandas to see '' and inf as NA values as well
        orig_inf_as_na = pd.options.mode.use_inf_as_na
        pd.options.mode.use_inf_as_na = True
        
        self.dataset_df['is_errornous'] = [0] * len(self.dataset_df)
        self.dataset_df['curations']    = [0] * len(self.dataset_df)
        
        # Quick eye-inspection showed stable information
        # Additionally: Abstractness of data, will make it hard to curate errors
        
        na_df = self.dataset_df.isna()
        for i in range(len(na_df)):
            na_row = na_df.iloc[i]
            if True in na_row:
                print('Row', i, 'contains an NA value!')
        
        # Set inf_as_na back to original value
        pd.options.mode.use_inf_as_na = orig_inf_as_na
    
    def _split_df_trainroute_based(self):
        
        named_df_list = []
        if 'linie' in self.dataset_df.columns:
            group_by_columns = ['linie']
            named_df_list = [(name, df.reset_index(drop=True)) for name, df in self.dataset_df.groupby(group_by_columns)]
        else:
            group_by_columns = ['train', 'start_station', 'connecting_stops', 'end_station']
            df_list = [df.reset_index(drop=True) for _, df in self.dataset_df.groupby(group_by_columns)]
            # Build names for the dfs via train and stops
            for df in df_list:
                name = df.loc[0, 'train'] + ': ' + df.loc[0, 'start_station'] + ' - ' + df.loc[0, 'end_station']
                named_df_list.append((name, df))
        
        # group_by_columns = ['train', 'start_station', 'connecting_stops', 'end_station']
        # df_list = [df.reset_index(drop=True) for _, df in self.dataset_df.groupby(group_by_columns)]
        
        # for df in df_list:
        #     print(df.head(5))
        
        # TODO: sort rows according to date
        
        # test = self.dataset_df.groupby('linie')
        # print(test.groups)
        
        # sum_rows = 0
        # print('Split df into', len(df_named_list), 'groups')
        # for name, df in df_named_list:
        #     sum_rows += len(df)
        #     print('NAme:', name)
        #     print(df.head(5))
        # print('Combined rows:', sum_rows)
        
        return named_df_list
    
    
    def _replace_year_month_columns_with_date(self):
        # Set pandas to see '' and inf as NA values as well
        orig_inf_as_na = pd.options.mode.use_inf_as_na
        pd.options.mode.use_inf_as_na = True
        
        df = self.dataset_df
        
        # Create new timestamp columns
        df['timeperiod_start'] = [date(year=MINYEAR, month=1, day=1)] * len(df)
        # df['timeperiod_start'] = [datetime(year=MINYEAR, month=1, day=1)] * len(df)
        df['timeperiod_end']   = [date(year=MINYEAR, month=1, day=1)] * len(df)
        # df['timeperiod_end']   = [datetime(year=MINYEAR, month=1, day=1)] * len(df)
        
        
        # Combine year and month into timestamp
        for i in range(len(df)):
            year = df.loc[i, 'jahr'] if not pd.isna(df.loc[i, 'jahr']) else None
            month = df.loc[i, 'monat'] if not pd.isna(df.loc[i, 'monat']) else None
            
            start_timestamp = None
            end_timestamp   = None
            if year != None and month != None:
                _, days_in_month = calendar.monthrange(year, month)
                start_timestamp = date(year, month, 1)
                end_timestamp   = date(year, month, days_in_month)
            
            df.loc[i, 'timeperiod_start'] = start_timestamp
            df.loc[i, 'timeperiod_end'  ] = end_timestamp
            
        
        # Remove old columns
        df.drop(['jahr', 'monat'], axis='columns', inplace=True)
        
        # Set pandas to see '' and inf as NA values as well
        orig_inf_as_na = pd.options.mode.use_inf_as_na
        pd.options.mode.use_inf_as_na = True
    
    
    def _split_trainline_column(self):
        # Set pandas to see '' and inf as NA values as well
        orig_inf_as_na = pd.options.mode.use_inf_as_na
        pd.options.mode.use_inf_as_na = True
    
        df = self.dataset_df
        
        # Create new columns
        df['train']         = [''] * len(df)
        df['start_station'] = [''] * len(df)
        df['connecting_stops'] = [''] * len(df)   # Create empty list for each row as init value
        df['end_station']   = [''] * len(df) 
        
        # Split current train line info into the new columns
        for i in range(len(df)):
            train_line = df.loc[i, 'linie'] if not pd.isna(df.loc[i, 'linie']) else None
            if train_line == None:
                continue
            
            # Split on second whitespace (seems to be correct format), to get train and stations info split
            fst_ws_idx = train_line.find(' ')
            snd_ws_idx = train_line.find(' ', fst_ws_idx+1)
            # Safe train info
            train = train_line[0:snd_ws_idx].strip()
            df.loc[i, 'train'] = train
            
            # Split and safe station names
            stations_list = train_line[snd_ws_idx:].split(' - ')
            if len(stations_list) < 2:
                print('Train line at row', i, 'could not be transformed to stops. Problematic value:', train_line)
                df.loc[i, 'is_errornous'] = 1
                continue
            
            # First and last value are start and end stop
            start_station = stations_list[0].strip()
            end_station   = stations_list[len(stations_list)-1].strip()
            df.loc[i, 'start_station'] = start_station
            df.loc[i, 'end_station']   = end_station
            
            # Values in between (if any) are the stops in between
            connecting_stops_str = ''
            for stop in stations_list[1:len(stations_list)-1]:
                connecting_stops_str += stop + ','
            
            # Remove last comma from the string
            if len(connecting_stops_str) > 0:
                connecting_stops_str = connecting_stops_str[0:-1]
                df.loc[i, 'connecting_stops'] = connecting_stops_str
                # pass
        
        # Drop old columns
        df.drop(['linie'], axis='columns', inplace=True)
        

        self.dataset_df = df
        
        # Set pandas to see '' and inf as NA values as well
        orig_inf_as_na = pd.options.mode.use_inf_as_na
        pd.options.mode.use_inf_as_na = True
    
    
    
    def _transform_data(self):
        # Convert date to timestamp
        self._replace_year_month_columns_with_date()
        # Split 'linie' into the train nr, the start station, connecting stops and the stop
        self._split_trainline_column()
        # TODO: possibly rename columns
    
    def run(self):
        self._pull_dataset()
        
        self._remove_duplicate_rows()
        self._curate_errornous_rows()
        
        self._transform_data()
        
        named_df_list = self._split_df_trainroute_based()
        self._convert_dfs_to_dbtables(named_df_list)    # Used if df is split into multiple dfs
        
        
        # self._convert_df_to_dbtable()                 # Used to convert the self.df into a sql table
        
        

def test_pipeline():
    db_str = 'sqlite:///train_punctuality.sqlite'
    engine = create_engine(db_str, echo=False)
    print('DB creation successfull!')
    
    pipeline = TrainPunctualityPipeline('https://opendata.schleswig-holstein.de/dataset/84256bd9-562c-4ea0-b0c6-908cd1e9e593/resource/c1407750-f05f-4715-8688-c0ff01b49131/download/puenktlichkeit.csv', engine)
    pipeline.run()
    
    
if __name__ == '__main__':
    test_pipeline()