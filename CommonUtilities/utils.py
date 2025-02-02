# Assignment :
# 2. In the event of failure , capture the differences , implement a code in the utils.py
# 4. Implement seperate execution log file for each test cases types ( extraction.,, Transfromation,loading)
# 5 . Install and ready with Jenkins

#  => done in the class => 1. Complete the comparision for data from oracle stores table to staging_stores in mysql
# done in the class => 3. Sales_data.csv to be sourced from Linux sytem ( Only for testing purpose )

import logging
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

# Utility to connect to Linux and fetch the data and store in local
def getDataFromLinuxBox(linux_file_path,local_file_path):
    try:
        # connect to ssh
        ssh_client = paramiko.SSHClient()
        ssh_client.set_missing_host_key_policy(paramiko.AutoAddPolicy)
        # to conenct to linux server
        ssh_client.connect(hostname,username=username,password=password)
        sftp = ssh_client.open_sftp()
        # download the file from linux server to local
        sftp.get(linux_file_path,local_file_path)
        logger.info("The file from Linux is downlaoded to local")
    except Exception as e:
        logger.error(f"Error whilee connecting Linux {e}")
'''
def reconcile_db_to_db(table_name1,db_engine1,table_name2,db_engine2):
    query_expected = f"select * from {table_name1}"
    df_expected = pd.read_sql(query_expected, db_engine1)
    logger.info(f"The expected data is: {df_expected}")
    query_actual = f"select * from {table_name2}"
    df_actual = pd.read_sql(query_actual, db_engine2)
    logger.info(f"The actual data is: {df_actual}")
    assert df_actual.equals(df_expected), f"data in  {table_name1} and table {table_name2} not matching"
'''

def reconcile_db_to_db(query1,db_engine1,query2,db_engine2):
    df_expected = pd.read_sql(query1, db_engine1).astype(str)
    logger.info(f"The expected data is: {df_expected}")
    df_actual = pd.read_sql(query2, db_engine2).astype(str)
    logger.info(f"The actual data is: {df_actual}")
    assert df_actual.equals(df_expected), f"data in  {query1} and table {query2} not matching"





