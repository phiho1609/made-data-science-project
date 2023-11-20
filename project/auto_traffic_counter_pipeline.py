import pandas as pd
import numpy as np
from sqlalchemy import create_engine
import csv
from datetime import date

# Pipeline representation specialized for the 'Autmoatic Traffic Counter' datasets from BASt
class AutoHourlyTrafficCounterPipeline():
    
    # Init the pipeline
    def __init__(self, dataset_url: str, db_engine, output_table_name: str):
        self.dataset_url = dataset_url
        self.db_engine = db_engine
        self.output_table_name = output_table_name
        
    # Pull the data from the net
    def _pull_dataset(self):
        self.dataset_df = pd.read_csv(self.dataset_url, sep=None)   # Setting sep=None lets pd depict delimiter automatically
        # print(self.dataset_df.head(5))
        
    # Put data from pandas dataframe into a new sqlite table
    def _convert_df_to_dbtable(self):
        self.dataset_df.to_sql(self.output_table_name, self.db_engine, if_exists='replace', index=False)
        
    
    def _remove_duplicate_rows(self):
        # Remove duplicated rows and print result
        # row_cnt_before = self.dataset_df.shape[1]
        row_cnt_before = len(self.dataset_df)
        self.dataset_df.drop_duplicates(inplace=True)   # inplace to modify exisiting df
        row_cnt_after = len(self.dataset_df)
        # print('Row count:', row_cnt_before)
        
        if row_cnt_before != row_cnt_after:
            print('Removed', row_cnt_before - row_cnt_after, 'duplicate rows!')
        else:
            print('No duplicate rows detected!')
    
    def _get_prev_and_next_col_entry(self, df, idx: int, col_name: str):
        prev_idx = idx-1 if (idx > 0) else 1
        next_idx = idx+1 if idx < len(df)-1 else len(df)-2
        
        if pd.isna(df.loc[prev_idx, col_name]):
            return (None, None)
        if pd.isna(df.loc[next_idx, col_name]):
            return (None, None)
        
        return (df.loc[prev_idx, col_name], df.loc[next_idx, col_name])
    
    
    def _curate_hours(self):
        # Set pandas to see '' and inf as NA values as well
        orig_inf_as_na = pd.options.mode.use_inf_as_na
        pd.options.mode.use_inf_as_na = True
        
        df = self.dataset_df
        for i in range(len(df)):
            # Check if hour available
            if not pd.isna(df.loc[i, 'Stunde']):
                # Hour available -> all good
                continue
            
            # Hour is not present, try to fix it
            ####################################
            prev_hour, next_hour = self._get_prev_and_next_col_entry(df, i, 'Stunde')
            if prev_hour == None or next_hour == None:
                # Entry cannot be fixed
                df.loc[i, 'is_errornous'] = 1
                continue
            
            
            # Determine correct hour
            curated_hour = None
            if prev_hour == 23 and next_hour == 1:
                # Special cases of hour being last hour of prev day
                curated_hour = 24
            elif prev_hour == 24 and next_hour == 2:
                # Special cases of hour being first hour on new day
                curated_hour = 1
            elif next_hour == prev_hour+2:
                # Hour is midday
                curated_hour = prev_hour+1
            
            if curated_hour == None:
                # Error, hour not easily determinable
                df.loc[i, 'is_errornous'] = 1
                continue
            
            df.loc[i, 'Stunde'] = curated_hour
        
        
        self.dataset_df = df
        
        # Set inf_as_na back to original value
        pd.options.mode.use_inf_as_na = orig_inf_as_na
        
        
        
    
    def _get_bast_date_diff(self, date1_str: str, date2_str: str):
        # Format is yymmdd
        date1 = date(int('20'+date1_str[0:2]), int(date1_str[2:4]), int(date1_str[4:6]))
        date2 = date(int('20'+date2_str[0:2]), int(date2_str[2:4]), int(date2_str[4:6]))
        
        date_diff = (date2 - date1).days
        # print('Datediff between', date1, 'and', date2, 'is', date_diff, 'days')
        return date_diff
            
    def _curate_dates(self):
        # Set pandas to see '' and inf as NA values as well
        orig_inf_as_na = pd.options.mode.use_inf_as_na
        pd.options.mode.use_inf_as_na = True
        
        df = self.dataset_df
        
        for i in range(len(df)):
            # Correct date
            if not pd.isna(df.loc[i, 'Datum']):
                # Date is present -> all good
                continue
            
            # Date is not present, try to fix it
            ####################################
            prev_idx = i-1 if (i > 0) else 1
            next_idx = i+1 if i < len(df)-1 else len(df)-2
            
            
            if pd.isna(df.loc[prev_idx, 'Datum']):
                # Date cannot be fixed, since prev date is also broken
                df.loc[i, 'is_errornous'] = 1
                continue
            if pd.isna(df.loc[next_idx, 'Datum']):
                # Date cannot be fixed, since next date is also broken
                df.loc[i, 'is_errornous'] = 1
                continue
            
            prev_bast_date = df.loc[prev_idx, 'Datum']    
            next_bast_date = df.loc[next_idx, 'Datum']
            
            date_diff = self._get_bast_date_diff(prev_bast_date, next_bast_date)
            if date_diff > 1:
                # Not fixable, hourly data points expected, so either same or next day
                df.loc[i, 'is_errornous'] = 1
                continue
            
            # Use hours of prev and next entry to determine if same or next day
            if pd.isna(df.loc[prev_idx, 'Stunde']) or pd.isna(df.loc[next_idx, 'Stunde']):
                # Hours are broken, not possible to fix dates
                df.loc[i, 'is_errornous'] = 1
                continue
            
            prev_hour = df.loc[prev_idx, 'Stunde']
            hour      = df.loc[i,        'Stunde']
            next_hour = df.loc[next_idx, 'Stunde']
            if hour-1 == prev_hour:
                # Current row has same date as previous row
                df.loc[i, 'Datum'] = df.loc[prev_idx, 'Datum']
                df.loc[i, 'curations'] += 1
            elif hour+1 == next_hour:
                # Current row has same date as next row
                df.loc[i, 'Datum'] = df.loc[next_idx, 'Datum']
                df.loc[i, 'curations'] += 1
            else:
                df.loc[i, 'is_errornous'] = 1
                
            continue
        
        self.dataset_df = df    # Likely unnecessary, but making sure the member df is updated

        # Set inf_as_na back to original value
        pd.options.mode.use_inf_as_na = orig_inf_as_na
        
        
    
    def _curate_errornous_rows(self):
        df = self.dataset_df
        
        # Create indicators in df:
        #   - row was curated
        #   - row has to be removed
        
        df['curations'] = [0] * len(df)
        df['is_errornous'] = [0] * len(df)
        
        # df = df.assign(curations=pd.Series(np.array([0] * len(df))))
        # df = df.assign(is_errornous=pd.Series(np.array([0] * len(df))))
        
        # print(df.head(10))
        
        # Set pandas to see '' and inf as NA values as well
        orig_inf_as_na = pd.options.mode.use_inf_as_na
        pd.options.mode.use_inf_as_na = True
        
        
        # First, try to fix date
        
        self.dataset_df = df
        self._curate_hours()
        self._curate_dates()
        
        df = self.dataset_df
        
        # Set inf_as_na back to original value
        pd.options.mode.use_inf_as_na = orig_inf_as_na
        
        
    
    def run(self):
        self._pull_dataset()
        self._remove_duplicate_rows()
        self._curate_errornous_rows()
        
        # self.dataset_df['test_col'] = [0] * len(self.dataset_df)
        
        # self._remove_errornous_rows()
        self._convert_df_to_dbtable()
        
        
        
        
        
def test_pipeline():
    db_str = 'sqlite:///testdb.sqlite'
    engine = create_engine(db_str, echo=False)
    print('DB creation sucessfull!')
    pipeline = AutoHourlyTrafficCounterPipeline('https://www.bast.de/videos/2011/zst1173.zip', engine, 'Moorkaten_2011')
    pipeline.run()
    

if __name__ == '__main__':
    test_pipeline()

