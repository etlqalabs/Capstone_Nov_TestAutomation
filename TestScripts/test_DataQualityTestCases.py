'''
Assignemnt:
1. Implement all these test cases for ALL the other different files
2. Implement the class for each of the other test files ( transfirmation, data quality, loading)
3. Implement the after test ( yiled ) for other fixtures ( we have done for mysql )
4. implment markets ( tags ) for each test cases
5. create a jenkins job for dev process and configure smoke test job to trigger it automation
   when dev job is complete successfully


'''

import logging
import pandas as pd
import pytest
from sqlalchemy import create_engine

from CommonUtilities.utils import check_file_exists, check_file_size, check_for_duplicates_across_the_columns, \
    check_for_duplicates_for_specific_column, check_for_null_values

logging.basicConfig(
    filename='Logs/testDataLoading.log',  # Name of the log file
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

# Verify the DQ checks for suplier_data.json file
def test_DQ_Supplier_data_file_availabilty():
    try:
        logger.info(f"File availabilty check for  initiated...")
        assert check_file_exists("TestData/supplier_data.json"),"Check why file is not available in the path"
        logger.info(f"File availabilty check for completed...")
    except Exception as e:
        logger.error("Error while checking the file availablity")
        pytest.fail("test for file availability check is failed")

# Verify the DQ checks for suplier_data.json file
def test_DQ_Supplier_data_file_size():
    try:
        logger.info(f"File size check for  initiated...")
        assert check_file_size("TestData/supplier_data.json"),"Check why file is is blank"
        logger.info(f"File size check for  completed...")
    except Exception as e:
        logger.error("Error while checking the file size")
        pytest.fail("test for file size check is failed")

def test_DQ_Supplier_data_duplication():
    try:
        logger.info(f"Data duplication check initiated...")
        assert check_for_duplicates_across_the_columns("TestData/supplier_data.json","json"),"Check why there are duplicates"
        logger.info(f"Data duplication check completed..")
    except Exception as e:
        logger.error("Error while reading the file ")
        pytest.fail("test for duplicate  check is failed")

def test_DQ_Supplier_data_duplication_for_supplier_id_column():
    try:
        logger.info(f"Data duplication check  for supplier_id initiated...")
        assert check_for_duplicates_for_specific_column("TestData/supplier_data.json","json","supplier_id"),"Check why there are duplicates"
        logger.info(f"Data duplication check for supplier_id completed..")
    except Exception as e:
        logger.error("Error while reading the file ")
        pytest.fail("test for duplicate  check for supplier_id is failed")



def test_DQ_Supplier_data_missing_value_check():
    try:
        logger.info(f"missing data  check initiated...")
        assert check_for_null_values("TestData/supplier_data.json","json"),"Check why there are missing values"
        logger.info(f"misssing data check completed..")
    except Exception as e:
        logger.error("Error while reading the file ")
        pytest.fail("test for missign data check is failed")

