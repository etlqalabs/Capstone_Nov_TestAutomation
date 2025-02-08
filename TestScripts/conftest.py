import logging
import pandas as pd
import paramiko
import pytest
from sqlalchemy import create_engine
from ApplicationConfiguration.config import *
import os


# Ensure the Logs directory exists
log_dir = 'Logs'
if not os.path.exists(log_dir):
    os.makedirs(log_dir)

# Set up logging to write to a log file in the 'Logs' directory
log_file = os.path.join(os.getcwd(), 'Logs', 'testDataExtraction.log')
print(f"Log file path: {log_file}")  # Confirm the path

logging.basicConfig(
    filename=log_file,
    level=logging.INFO,  # Set to DEBUG if you need more detailed logging
    format='%(asctime)s - %(levelname)s - %(message)s'
)

# Create a logger instance
logger = logging.getLogger(__name__)


# fixture for creating mysql database conenction
@pytest.fixture()
def mysql_engine():
    logger.info("my sql db connection is being establish")
    mysql_engine = create_engine(f'mysql+pymysql://{MYSQL_USER}:{MYSQL_PASSWORD}@{MYSQL_HOST}:{MYSQL_PORT}/{MYSQL_DATABASE}').connect()
    logger.info("my sql db connection is successfully established")
    yield mysql_engine
    mysql_engine.close()
    logger.info("my sql connection is closed...")


# fixture for creating mysql database conenction
@pytest.fixture()
def oracle_engine():
    logger.info("oracle db connection is being establish")
    oracle_engine = create_engine(
        f'oracle+cx_oracle://{ORACLE_USER}:{ORACLE_PASSWORD}@{ORACLE_HOST}:{ORACLE_PORT}/{ORACLE_SERVICE}')
    logger.info("oracle db connection is being establish")
    return oracle_engine

# Utility to connect to Linux and fetch the data and store in local
@pytest.fixture()
def getDataFromLinuxBox():
    try:
        logger.info("Linux  connection is being establish")
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
