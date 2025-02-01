# Assignment :
# 1. Complete the comparision for data from oracle stores table to staging_stores in mysql
# 2. In the event of failure , capture the differences , implement a code
# 3. Sales_data.csv to be sourced from Linux sytem ( Only for testing purpose )

import logging
import pandas as pd
import pytest
from sqlalchemy import create_engine

# The logger configuration
logging.basicConfig(
    filename='Logs/commonUtilities.log',  # Name of the log file
    filemode='w',  # 'a' to append, 'w' to overwrite
    format='%(asctime)s - %(levelname)s - %(message)s',  # Log format
    level=logging.INFO  # Set the logging level
)
logger = logging.getLogger(__name__)

# Create mysql engine
#mysql_engine = create_engine('mysql+pymysql://root:Admin%40143@localhost:3308/retaildwh')
from ApplicationConfiguration.config import *

# Create mysql engine
mysql_engine = create_engine(f'mysql+pymysql://{MYSQL_USER}:{MYSQL_PASSWORD}@{MYSQL_HOST}:{MYSQL_PORT}/{MYSQL_DATABASE}')

# Create Oracle engine
oracle_engine = create_engine(f'oracle+cx_oracle://{ORACLE_USER}:{ORACLE_PASSWORD}@{ORACLE_HOST}:{ORACLE_PORT}/{ORACLE_SERVICE}')

def reconcile_file_to_db(file_path,file_type,table_name,db_engine):
    if file_type == 'json':
        df_expected = pd.read_json(file_path)
    elif file_type == 'csv':
        df_expected = pd.read_csv(file_path)
    elif file_type == 'xml':
        df_expected = pd.read_xml(file_path,xpath="//item")
    else:
        raise ValueError(f"Unsupported file type passed {file_type}")
    logger.info(f"The expected data is: {df_expected}")
    query = f"select * from {table_name}"
    df_actual = pd.read_sql(query, db_engine)
    logger.info(f"The actual data is: {df_actual}")
    assert df_actual.equals(df_expected), f"data in file {file_path} and table {table_name} not matching"