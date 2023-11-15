# Group B -> python
import pandas as pd
from sqlalchemy import create_engine, Column, Integer, String, TEXT, REAL, INTEGER
from sqlalchemy.ext.declarative import declarative_base


def get_csv_from_url(url: str, sep: str):
    csv_df = pd.read_csv(url, sep=sep)
    csv_df = csv_df.infer_objects()
    return csv_df

def create_sqlite_db(name: str) :
    db_str = 'sqlite:///' + name + '.sqlite'
    engine = create_engine(db_str, echo=True)
    return engine


def csv_to_sqlite_db(csv_df, table_name: str, db_engine):
    dtype_to_SQL_dtype_dict = {'int64': INTEGER, 'float64': REAL, 'object': TEXT}

    # Create class representing DB table
    Base = declarative_base()
    class Airports(Base):
        
        __tablename__ = table_name
        
        id = Column(Integer, primary_key=True)
        # Automatically infer SQL types from panda types
        columns = [Column(column_name, dtype_to_SQL_dtype_dict[csv_df[column_name].dtype.name]) for column_name in csv_df.columns]
        
        __table_args__ = {'extend_existing': False}             # If true, new data will not overwrite the table, but be appended
        locals().update({col.name: col for col in columns})     # Allow object-variable like access to the columns (for this python script)
        
        
    Base.metadata.create_all(db_engine)

    csv_df.to_sql(table_name, db_engine, if_exists='replace', index=False)     # do not include index as column
    
    
    
if __name__ == '__main__':
    csv_df = get_csv_from_url('https://opendata.rhein-kreis-neuss.de/api/v2/catalog/datasets/rhein-kreis-neuss-flughafen-weltweit/exports/csv', sep=';')
    db_engine = create_sqlite_db('airports')
    csv_to_sqlite_db(csv_df, 'airports', db_engine)