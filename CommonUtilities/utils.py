# Assignment :
# 2. In the event of failure , capture the differences , implement a code in the utils.py
# 4. Implement seperate execution log file for each test cases types ( extraction.,, Transfromation,loading)
# 5 . Install and ready with Jenkins

#  => done in the class => 1. Complete the comparision for data from oracle stores table to staging_stores in mysql
# done in the class => 3. Sales_data.csv to be sourced from Linux sytem ( Only for testing purpose )

import logging
import os.path

import pandas as pd
import paramiko
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

# this is used for all the other file to db verification expecte for supplier_data
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

# this is being used only for supplier_data
def reconcile_file_to_db_supplier(file_path,file_type,query,db_engine):
    if file_type == 'json':
        df_expected = pd.read_json(file_path)
    elif file_type == 'csv':
        df_expected = pd.read_csv(file_path)
    elif file_type == 'xml':
        df_expected = pd.read_xml(file_path,xpath="//item")
    else:
        raise ValueError(f"Unsupported file type passed {file_type}")
    logger.info(f"The expected data is: {df_expected}")
    df_actual = pd.read_sql(query, db_engine).astype(str)
    logger.info(f"The actual data is: {df_actual}")
    assert df_actual.equals(df_expected), f"data in file {file_path} and {query} not matching"


def reconcile_db_to_db(query1,db_engine1,query2,db_engine2):
    df_expected = pd.read_sql(query1, db_engine1).astype(str)
    logger.info(f"The expected data is: {df_expected}")
    df_actual = pd.read_sql(query2, db_engine2).astype(str)
    logger.info(f"The actual data is: {df_actual}")
    assert df_actual.equals(df_expected), f"data in  {query1} and table {query2} not matching"


# utility function for file exists
def check_file_exists(file_path):
    try:
        if os.path.isfile(file_path):
            return True
        else:
            return False
    except Exception as e:
        logger.error(f"File :{file_path}  does not exist {e}")


# Chck if the files are not zero byte
def check_file_size(file_path):
    try:
        if os.path.getsize(file_path) != 0:
            return True
        else:
            return False
    except Exception as e:
        logger.error(f"File :{file_path} is zero byte file {e}")


# Check if any duplicates in the files across the file
def check_for_duplicates_across_the_columns(file_path,file_type):
    try:
        if file_type == 'json':
            df = pd.read_json(file_path)
        elif file_type == 'csv':
            df = pd.read_csv(file_path)
        elif file_type == 'xml':
            df = pd.read_xml(file_path,xpath="//item")
        else:
            raise ValueError(f"Unsupported file type passed {file_type}")
        logger.info(f"The expected data is: {df}")

        if df.duplicated().any():
            return False
        else:
            return True

    except Exception as e:
        logger.error(f"Error while reading the file {file_path}:{e}")

# Check if any duplicates in the files for a specific column

def check_for_duplicates_for_specific_column(file_path,file_type,column_name):
    try:
        if file_type == 'json':
            df = pd.read_json(file_path)
        elif file_type == 'csv':
            df = pd.read_csv(file_path)
        elif file_type == 'xml':
            df = pd.read_xml(file_path,xpath="//item")
        else:
            raise ValueError(f"Unsupported file type passed {file_type}")
        logger.info(f"The expected data is: {df}")

        if df[column_name].duplicated().any():
            return False
        else:
            return True

    except Exception as e:
        logger.error(f"Error while reading the file {file_path}:{e}")


# check if there are missing values in the file across the columns

def check_for_null_values(file_path,file_type):
    try:
        if file_type == 'json':
            df = pd.read_json(file_path)
        elif file_type == 'csv':
            df = pd.read_csv(file_path)
        elif file_type == 'xml':
            df = pd.read_xml(file_path,xpath="//item")
        else:
            raise ValueError(f"Unsupported file type passed {file_type}")
        logger.info(f"The expected data is: {df}")

        if df.isnull().values.any():
            return False
        else:
            return True

    except Exception as e:
        logger.error(f"Error while reading the file {file_path}:{e}")


def getDataFromLinuxBox():
    return None