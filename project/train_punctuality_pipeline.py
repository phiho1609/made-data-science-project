import pandas as pd
import numpy as np
from sqlalchemy import create_engine


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
        self.dataset_df.to_sql(self.output_table_name, self.db_engine, if_exists='replace', index=False)
    
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
        
    
    def _curate_errornous_rows(self):
        # Set pandas to see '' and inf as NA values as well
        orig_inf_as_na = pd.options.mode.use_inf_as_na
        pd.options.mode.use_inf_as_na = True
        
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
        named_df_list = [(name, df) for name, df in self.dataset_df.groupby('linie')]
        
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
    
    def run(self):
        self._pull_dataset()
        
        # # Checkout duplicates
        # duplicates = self.dataset_df.duplicated()
        # for i in range(len(duplicates)):
        #     if duplicates.values[i] == True:
        #         print('Row', i, 'is a dupliacte')
        
        self._remove_duplicate_rows()
        self._curate_errornous_rows()
        # TODO: possibly rename columns
        # TODO: possibly convert date to timestamp
        # TODO: possibly split 'linie' into the train, the start and the stop
        
        named_df_list = self._split_df_trainroute_based()
        
        self._convert_dfs_to_dbtables(named_df_list)
        
        
        # self._convert_df_to_dbtable()
        
        

def test_pipeline():
    db_str = 'sqlite:///train_punctuality.sqlite'
    engine = create_engine(db_str, echo=False)
    print('DB creation successfull!')
    
    pipeline = TrainPunctualityPipeline('https://opendata.schleswig-holstein.de/dataset/84256bd9-562c-4ea0-b0c6-908cd1e9e593/resource/c1407750-f05f-4715-8688-c0ff01b49131/download/puenktlichkeit.csv', engine)
    pipeline.run()
    
    
if __name__ == '__main__':
    test_pipeline()