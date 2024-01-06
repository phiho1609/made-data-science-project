

class Pipeline():
    
    # Init the pipeline
    def __init__(self, dataset_url: str, db_engine, output_table_name = ''):
        self.dataset_url = dataset_url
        self.db_engine = db_engine
        self.output_table_name = output_table_name
        
        self.dataset_df = None
    
    
    @classmethod
    def rename_columns(cls, old_new_name_mapping: dict, dataset_df):
        if dataset_df is None:
            raise RuntimeError(__name__ + ": Error: called without dataframe!")
        
        # print('Old cols:', dataset_df.columns)
        dataset_df.rename(columns=old_new_name_mapping, inplace=True)
        # print('New cols:', dataset_df.columns)
        
        return dataset_df
        
    
    @classmethod
    def reorder_columns(cls, new_col_idxs: dict, dataset_df):
        if dataset_df is None:
            raise RuntimeError(__name__ + ": Error: called without dataframe!")
        
        cols = dataset_df.columns
        # print('Old cols:', cols)
        for new_col_idx in sorted(new_col_idxs.keys()):
            new_col = new_col_idxs.get(new_col_idx)
            try:
                cols = cols.drop(new_col)
            except ValueError:
                # Not in the current cols!! -> Errornous input
                raise RuntimeError(__name__ + ": Error: called with non-existant column name '" + new_col +"'")
    
            cols = cols.insert(new_col_idx, new_col)
            
        # print('New cols:', cols)
        # self.dataset_df.columns = cols
        dataset_df = dataset_df[cols]

        return dataset_df
    
    
    def run(self):
        pass
    
    