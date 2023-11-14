# Group B -> python
import pandas as pd
from sqlalchemy import create_engine, Column, Integer, String, TEXT, REAL, INTEGER
from sqlalchemy.ext.declarative import declarative_base

print('lol')

url = 'https://opendata.rhein-kreis-neuss.de/api/v2/catalog/datasets/rhein-kreis-neuss-flughafen-weltweit/exports/csv'
csv_df = pd.read_csv(url, sep=';')
print(csv_df.head())
csv_df = csv_df.infer_objects()
print(csv_df.head())

# sqla.create_engine('airports.sqlite', echo=True)
engine = create_engine('sqlite:///airports.sqlite', echo=True)
# conn = engine.connect()

# types = [csv_df[column_name].dtype.name for column_name in csv_df.columns]
# print('types:', types)
dtype_to_SQL_dtype_dict = {'int64': INTEGER, 'float64': REAL, 'object': TEXT}

Base = declarative_base()
class Airports(Base):
    __tablename__ = 'airports'
    
    id = Column(Integer, primary_key=True)
    # columns = [Column(column_name, dtype_to_SQL_dtype_dict[csv_df[column_name].dtype.name] getattr(String, csv_df[column_name].dtype.name)) for column_name in csv_df.columns]
    columns = [Column(column_name, dtype_to_SQL_dtype_dict[csv_df[column_name].dtype.name]) for column_name in csv_df.columns]
    
    __table_args__ = {'extend_existing': False}             # If true, new data will not overwrite the table, but be appended
    locals().update({col.name: col for col in columns})     # Allow object-variable like access to the columns (for this python script)
    
    
Base.metadata.create_all(engine)

csv_df.to_sql('airports', engine, if_exists='replace', index=False)     # do not include index as column