import logging
import pandas as pd
import pytest
from sqlalchemy import create_engine

# The logger configuration
from CommonUtilities.utils import reconcile_file_to_db, getDataFromLinuxBox, reconcile_db_to_db

logging.basicConfig(
    filename='Logs/testExecution.log',  # Name of the log file
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

# test cases is a special fucntion which starts with test_<<functionName>>

@pytest.mark.skip
def test_DataExtractionFromSupplierDataJsonToStaging():
    try:
        logger.info("test case for Supplier Data extraction has started ....")
        reconcile_file_to_db("TestData/supplier_data.json","json","staging_supplier", mysql_engine)
        logger.info("test case for Supplier Data extraction has completed ....")
    except Exception as e:
        logger.error(f"test case for Supplier Data extraction has failed{e}")
        pytest.fail("Data Extraction failed during ETL extraction process")
@pytest.mark.skip
def test_DataExtractionFromSalesDataCSVToStaging():
    try:
        logger.info("test case for Sales Data extraction has started ....")
        #getDataFromLinuxBox("/home/etlqalabs/Data/sales_data.csv","TestData/sales_data_from_Linux.csv")
        reconcile_file_to_db("TestData/sales_data_from_Linux.csv","csv","staging_sales", mysql_engine)
        logger.info("test case for Sales Data extraction has completed ....")
    except Exception as e:
        logger.error(f"test case for Sales Data extraction has failed{e}")
        pytest.fail("Data Extraction failed during ETL extraction process")

@pytest.mark.skip
def test_DataExtractionFromProductDataCSVToStaging():
    try:
        logger.info("test case for product Data extraction has started ....")
        reconcile_file_to_db("TestData/product_data.csv","csv","staging_product", mysql_engine)
        logger.info("test case for product Data extraction has completed ....")
    except Exception as e:
        logger.error(f"test case for product Data extraction has failed{e}")
        pytest.fail("Data Extraction failed during ETL extraction process")

@pytest.mark.skip
def test_DataExtractionFromInventoryDataXMLToStaging():
    try:
        logger.info("test case for Inventory Data extraction has started ....")
        reconcile_file_to_db("TestData/inventory_data.xml","xml","staging_inventory", mysql_engine)
        logger.info("test case for Inventory Data extraction has completed ....")
    except Exception as e:
        logger.error(f"test case for Inventory Data extraction has failed{e}")
        pytest.fail("Data Extraction failed during ETL extraction process")

def test_DataExtractionFromOracleToStagingmySQL():
    try:
        logger.info("test case for Stores Data extraction has started ....")
        query_expected = """select * from stores"""
        query_actual= """select * from staging_stores"""
        reconcile_db_to_db(query_expected,oracle_engine,query_actual, mysql_engine)
        logger.info("test case for Stores Data extraction has completed ....")
    except Exception as e:
        logger.error(f"test case for Stores Data extraction has failed{e}")
        pytest.fail("Data Extraction failed during ETL extraction process")
