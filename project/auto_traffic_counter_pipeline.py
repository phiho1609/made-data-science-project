import pandas as pd
import numpy as np
from sqlalchemy import create_engine
import csv
from datetime import date, datetime, MINYEAR
from german_states_nrs import german_states_nrs
from pipeline import Pipeline


# Pipeline representation specialized for the 'Autmoatic Traffic Counter' datasets from BASt
class AutoHourlyTrafficCounterPipeline(Pipeline):
    
    # Pull the data from the net
    def _pull_dataset(self):
        self.dataset_df = pd.read_csv(self.dataset_url, sep=None, engine='python')   # Setting sep=None lets pd depict delimiter automatically

    # Put data from pandas dataframe into a new sqlite table
    def _convert_df_to_dbtable(self):
        self.dataset_df.to_sql(self.output_table_name, self.db_engine, if_exists='replace', index=False)
        
    
    def _remove_duplicate_rows(self):
        # Remove duplicated rows and print result
        row_cnt_before = len(self.dataset_df)
        self.dataset_df.drop_duplicates(inplace=True)   # inplace to modify exisiting df
        row_cnt_after = len(self.dataset_df)
        
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
            
            df.loc[i, 'Stunde'] = int(curated_hour)
        
        
        self.dataset_df = df
        
        # Set inf_as_na back to original value
        pd.options.mode.use_inf_as_na = orig_inf_as_na
        
        
        
    
    def _get_bast_date_day_diff(self, date1_str: str, date2_str: str):
        # Note BASt-date format is 'yymmdd'
        date1 = date(int('20'+date1_str[0:2]), int(date1_str[2:4]), int(date1_str[4:6]))
        date2 = date(int('20'+date2_str[0:2]), int(date2_str[2:4]), int(date2_str[4:6]))
        
        date_diff = (date2 - date1).days
        return date_diff
    
    def _get_bast_datetime_hour_diff(self, date1_str: str, hour1_str: str, date2_str: str, hour2_str: str):
        # Cast given time in BASt's format into datetime (Note that datetime hours are from 0-23, but BASt from 1 to 24)
        datetime1 = datetime(int('20'+date1_str[0:2]), int(date1_str[2:4]), int(date1_str[4:6]), int(hour1_str)-1)
        datetime2 = datetime(int('20'+date2_str[0:2]), int(date2_str[2:4]), int(date2_str[4:6]), int(hour2_str)-1)
        
        datetime_diff = datetime2 - datetime1
        hour_diff = (datetime_diff.days * 24) + (datetime_diff.seconds / 3600)
        return hour_diff
        
            
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
            prev_bast_date, next_bast_date = self._get_prev_and_next_col_entry(df, i, 'Datum')
            if prev_bast_date == None or next_bast_date == None:
                df.loc[i, 'is_errornous'] = 1
                continue
            
            
            date_diff = self._get_bast_date_day_diff(prev_bast_date, next_bast_date)
            if date_diff > 1:
                # Not fixable, hourly data points expected, so either same or next day
                df.loc[i, 'is_errornous'] = 1
                continue
            
            # Use hours of prev and next entry to determine if same or next day
            prev_hour, next_hour = self._get_prev_and_next_col_entry(df, i, 'Stunde')
            if prev_hour == None or next_hour == None:
                # Hours are broken, not possible to fix dates
                df.loc[i, 'is_errornous'] = 1
                continue
            
            
            hour = df.loc[i, 'Stunde']
            if hour-1 == prev_hour:
                # Current row has same date as previous row
                df.loc[i, 'Datum'] = prev_bast_date
                df.loc[i, 'curations'] += 1
            elif hour+1 == next_hour:
                # Current row has same date as next row
                df.loc[i, 'Datum'] = next_bast_date
                df.loc[i, 'curations'] += 1
            else:
                df.loc[i, 'is_errornous'] = 1
                
            continue
        
        self.dataset_df = df    # Likely unnecessary, but making sure the member df is updated

        # Set inf_as_na back to original value
        pd.options.mode.use_inf_as_na = orig_inf_as_na
        
    
    def _curate_relevant_traffic(self, relevant_traffic_ids: set):
        # Check that passed traffic ids are actual df columns
        if len(relevant_traffic_ids) == 0:
            raise RuntimeWarning('Function called with empty set of relevant traffic ids!')
        for traffic_id in relevant_traffic_ids:
            if traffic_id not in self.dataset_df.columns:
                raise RuntimeError('Traffic id "', traffic_id, '" is not a column in the dataframe')
        
        # Set pandas to see '' and inf as NA values as well
        orig_inf_as_na = pd.options.mode.use_inf_as_na
        pd.options.mode.use_inf_as_na = True
        df = self.dataset_df
        
        for i in range(len(df)):
            # Check if recreation via prev and next entry is possible
            # (possible == prev entry is hour before, next entry hour after)
            prev_date, next_date = self._get_prev_and_next_col_entry(df, i, 'Datum')
            prev_hour, next_hour = self._get_prev_and_next_col_entry(df, i, 'Stunde')
            if prev_date == None or next_date == None or prev_hour == None or next_hour == None:
                # Data recreation not possible, skip
                df.loc[i, 'is_errornous'] = 1
                continue
            
            # Check that prev and next entry are -1 and +1 hour respectively
            hour_diff = self._get_bast_datetime_hour_diff(prev_date, prev_hour, next_date, next_hour)
            if hour_diff != 2:
                print('Missing Traffic counts are unresolvable for row:', i)
                df.loc[i, 'is_errornous'] = 1
                continue
            
            # After here it can be expected that the prev entry is -1 hour and the next +1 hour
            
            for traffic_id in relevant_traffic_ids:
                # Check if relevant traffic count is missing
                if not pd.isna(df.loc[i, traffic_id]):
                    # Value is present -> all good
                    continue
                
                # Missing traffic count found -> resolve via average
                # Check if both prev and next entry have traffic count
                prev_idx = max(i-1, 0)
                next_idx = min(i+1, len(df)-1)
                if pd.isna(df.loc[prev_idx, traffic_id]) or pd.isna(df.loc[next_idx, traffic_id]):
                    # At least one entry is also NA --> no curation possible
                    df.loc[i, 'is_errornous'] = 1
                    continue
                
                # Both prev and next row have a usable entry
                avg_value = (df.loc[prev_idx, traffic_id] + df.loc[next_idx, traffic_id]) / 2
                df.loc[i, traffic_id] = avg_value
                df.loc[i, 'curations'] += 1
                print('Successfully curated: row', i, 'column:', traffic_id)
                
        self.dataset_df = df
        # Set inf_as_na back to original value
        pd.options.mode.use_inf_as_na = orig_inf_as_na
    
    
    
    def _curate_errornous_rows(self):
        df = self.dataset_df
        
        # Create indicators in df:
        #   - row was curated
        #   - row has to be removed
        df['curations'] = [0] * len(df)
        df['is_errornous'] = [0] * len(df)
        
        # Change dtypes if deemed necessary:
        #   'Datum': int64 -> object (string)
        df = df.astype({'Datum': str})
        
        # Set pandas to see '' and inf as NA values as well
        orig_inf_as_na = pd.options.mode.use_inf_as_na
        pd.options.mode.use_inf_as_na = True
        
        self.dataset_df = df
        self._curate_hours()
        self._curate_dates()
        # Relevant traffic groups as substitute for commuting by train are determined to be:
        #   - PKW
        #   - Motobike
        #   - Bus
        relevant_traffic_identifiers = {'Pkw_R1', 'Pkw_R2', 'Mot_R1', 'Mot_R2', 'Bus_R1', 'Bus_R2'}
        self._curate_relevant_traffic(relevant_traffic_identifiers)
        
        df = self.dataset_df
        
        # Set inf_as_na back to original value
        pd.options.mode.use_inf_as_na = orig_inf_as_na
        
        
    def _merge_two_columns(self, col_name1: str, col_name2: str, new_col_name: str):
        df = self.dataset_df
        # Check availability of specified columns
        if col_name1 not in df or col_name2 not in df:
            raise RuntimeError('Merge input column "', col_name1, '" or "', col_name2, '" do not exist!')
        if new_col_name in df:
            raise RuntimeError('New column name "', new_col_name, '" already exisits in dataframe! Cannot be reused.')
        
        orig_inf_as_na = pd.options.mode.use_inf_as_na
        pd.options.mode.use_inf_as_na = True
        
        # Create new column
        df[new_col_name] = [''] * len(df)
        # Fill new column with combination of old columns
        for i in range(len(df)):
            column1_value = df.loc[i, col_name1] if not pd.isna(df.loc[i, col_name1]) else ''
            column2_value = df.loc[i, col_name2] if not pd.isna(df.loc[i, col_name2]) else ''
        
            df.loc[i, new_col_name] = column1_value + str(column2_value)
        
        # Drop old columns
        df.drop([col_name1, col_name2], axis='columns', inplace=True)
        
        pd.options.mode.use_inf_as_na = orig_inf_as_na
        self.dataset_df = df
    
    
    def _fill_statenr_column_with_names(self):
        df = self.dataset_df
        
        # Change column type to string
        df = df.astype({'Land': str})
        # Exchange State Nr with state name
        for i in range(len(df)):
            state_nr = df.loc[i, 'Land']
            if int(state_nr) in german_states_nrs:
                df.loc[i, 'Land'] = german_states_nrs.get(int(state_nr))
        
        self.dataset_df = df
    
    
    def _replace_bast_time_columns_with_datetime(self):
        # Set pandas to see '' and inf as NA values as well
        orig_inf_as_na = pd.options.mode.use_inf_as_na
        pd.options.mode.use_inf_as_na = True
        
        df = self.dataset_df
        
        # Create new timestamp column
        df['timestamp'] = [datetime(year=MINYEAR, month=1, day=1)] * len(df)
        
        # Combine date and hour columns into the timestamp column
        for i in range(len(df)):
            bast_date = df.loc[i, 'Datum' ] if not pd.isna(df.loc[i, 'Datum' ]) else None
            hour      = df.loc[i, 'Stunde'] if not pd.isna(df.loc[i, 'Stunde']) else None
            
            timestamp = None
            if bast_date != None and hour != None:
                timestamp = datetime(int('20'+bast_date[0:2]), int(bast_date[2:4]), int(bast_date[4:6]), int(hour)-1)
            
            df.loc[i, 'timestamp'] = timestamp
            
        # Remove old columns
        df.drop(['Datum', 'Stunde'], axis='columns', inplace=True)
        
        self.dataset_df = df
        # Set inf_as_na back to original value
        pd.options.mode.use_inf_as_na = orig_inf_as_na
    
    
    def _remove_columns(self, columns: list):
        df = self.dataset_df
        # Remove non existing columns from list
        cleaned_columns = []
        for col_name in columns:
            if col_name in df.columns:
                cleaned_columns.append(col_name)
            else:
                print('Warning: Non-existing column name "', col_name, '" was passed to be deleted!')
                
            
        # Drop columns from (cleaned) list
        df.drop(cleaned_columns, axis='columns', inplace=True)
        
        self.dataset_df = df
        
    
    def _transform_data(self):
        # Replace state Nrs with actual names
        self._fill_statenr_column_with_names()
        # Merge Street-type and number together
        self._merge_two_columns('Strklas', 'Strnum', 'street')
        # Create actual datetime from date and hour
        self._replace_bast_time_columns_with_datetime()
        # Remove unnecessary traffic counters
        self._remove_columns(['KFZ_R1', 'K_KFZ_R1', 'KFZ_R2', 'K_KFZ_R2', 'Lkw_R1', 'K_Lkw_R1', 'Lkw_R2', 'K_Lkw_R2', 
                              'PLZ_R1', 'K_PLZ_R1', 'Lfw_R1', 'K_Lfw_R1', 'PmA_R1', 'K_PmA_R1', 'LoA_R1', 'K_LoA_R1', 'Lzg_R1', 
                              'K_Lzg_R1', 'Sat_R1', 'K_Sat_R1', 'PLZ_R2', 'K_PLZ_R2', 'Lfw_R2', 'K_Lfw_R2', 'PmA_R2', 
                              'K_PmA_R2', 'LoA_R2', 'K_LoA_R2', 'Lzg_R2', 'K_Lzg_R2', 'Sat_R2', 'K_Sat_R2', 'Son_R1', 
                              'K_Son_R1', 'Son_R2', 'K_Son_R2'])
        
    
    
    def run(self):
        self._pull_dataset()
        self._remove_duplicate_rows()
        self._curate_errornous_rows()
        
        self._transform_data()
        
        self.dataset_df = Pipeline.rename_columns({'TKNR': 'tk_nr', 'Zst': 'counter_id', 'Land': 'federal_state', 'Wotag': 'weekday', 'Fahrtzw': 'day_type', 
                              'Pkw_R1': 'car_dir1_cnt', 'K_Pkw_R1': 'car_dir1_validity', 'Mot_R1': 'bike_dir1_cnt',
                              'K_Mot_R1': 'bike_dir1_validity', 'Bus_R1': 'bus_dir1_cnt', 'K_Bus_R1': 'bus_dir1_validity', 
                              'Pkw_R2': 'car_dir2_cnt', 'K_Pkw_R2': 'car_dir2_validity', 'Mot_R2': 'bike_dir2_cnt', 
                              'K_Mot_R2': 'bike_dir2_validity', 'Bus_R2': 'bus_dir2_cnt', 'K_Bus_R2' : 'bus_dir2_validity'}, self.dataset_df)
        
        self.dataset_df = Pipeline.reorder_columns({3: 'street', 4: 'timestamp'}, self.dataset_df)
        
        # self._remove_errornous_rows()
        self._convert_df_to_dbtable()
        
        
        
        
        
# def test_pipeline():
#     db_str = 'sqlite:///auto_traffic_counters.sqlite'
#     engine = create_engine(db_str, echo=False)
#     print('DB creation sucessfull!')
#     pipeline = AutoHourlyTrafficCounterPipeline('https://www.bast.de/videos/2011/zst1173.zip', engine, 'Moorkaten_2011')
#     pipeline.run()
    

# if __name__ == '__main__':
#     test_pipeline()

