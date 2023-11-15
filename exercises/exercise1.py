# Group B -> python
import pandas as pd
from sqlalchemy import create_engine


def get_csv_from_url(url: str, sep: str):
    csv_df = pd.read_csv(url, sep=sep)
    csv_df = csv_df.infer_objects()
    return csv_df

def create_sqlite_db(name: str) :
    db_str = 'sqlite:///' + name + '.sqlite'
    engine = create_engine(db_str, echo=True)
    return engine


def csv_to_sqlite_db(csv_df, table_name: str, db_engine):
    csv_df.to_sql(table_name, db_engine, if_exists='replace', index=False)     # do not include index as column
    
    
    
if __name__ == '__main__':
    csv_df = get_csv_from_url('https://opendata.rhein-kreis-neuss.de/api/v2/catalog/datasets/rhein-kreis-neuss-flughafen-weltweit/exports/csv', sep=';')
    db_engine = create_sqlite_db('airports')
    csv_to_sqlite_db(csv_df, 'airports', db_engine)