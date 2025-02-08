import logging
import pandas as pd
import pytest
from sqlalchemy import create_engine
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


# The logger configuration
from CommonUtilities.utils import reconcile_file_to_db, getDataFromLinuxBox, reconcile_db_to_db, \
    reconcile_file_to_db_supplier

'''
log_file = 'Logs/testDataExtraction.log'
logging.basicConfig(
    filename=log_file,  # Name of the log file
    filemode='w',  # 'a' to append, 'w' to overwrite
    format='%(asctime)s - %(levelname)s - %(message)s',  # Log format
    level=logging.INFO  # Set the logging level
)
logger = logging.getLogger(__name__)
'''


# test cases is a special fucntion which starts with test_<<functionName>>

@pytest.mark.usefixtures("mysql_engine")
class TestDataExtractionTestCases:
    @pytest.mark.smoke
    @pytest.mark.regression
    def test_DataExtractionFromSalesDataCSVToStaging(self,mysql_engine,getDataFromLinuxBox):
        try:
            logger.info("test case for Sales Data extraction has started ....")
            #getDataFromLinuxBox("/home/etlqalabs/Data/sales_data.csv","TestData/sales_data_from_Linux.csv")
            reconcile_file_to_db("TestData/sales_data_from_Linux.csv","csv","staging_sales", mysql_engine)
            logger.info("test case for Sales Data extraction has completed ....")
        except Exception as e:
            logger.error(f"test case for Sales Data extraction has failed{e}")
            pytest.fail("Data Extraction failed during ETL extraction process")

    def test_DataExtractionFromProductDataCSVToStaging(self,mysql_engine):
        try:
            logger.info("test case for product Data extraction has started ....")
            reconcile_file_to_db("TestData/product_data.csv","csv","staging_product", mysql_engine)
            logger.info("test case for product Data extraction has completed ....")
        except Exception as e:
            logger.error(f"test case for product Data extraction has failed{e}")
            pytest.fail("Data Extraction failed during ETL extraction process")



    @pytest.mark.skip
    def test_DataExtractionFromSupplierDataJsonToStaging(self,mysql_engine):
        try:
            logger.info("test case for Supplier Data extraction has started ....")
            query ="""select * from staging_supplier"""
            reconcile_file_to_db_supplier("TestData/supplier_data.json","json",query, mysql_engine)
            logger.info("test case for Supplier Data extraction has completed ....")
        except Exception as e:
            logger.error(f"test case for Supplier Data extraction has failed{e}")
            pytest.fail("Data Extraction failed during ETL extraction process")

    @pytest.mark.regression
    def test_DataExtractionFromInventoryDataXMLToStaging(self,mysql_engine):
        try:
            logger.info("test case for Inventory Data extraction has started ....")
            reconcile_file_to_db("TestData/inventory_data.xml","xml","staging_inventory", mysql_engine)
            logger.info("test case for Inventory Data extraction has completed ....")
        except Exception as e:
            logger.error(f"test case for Inventory Data extraction has failed{e}")
            pytest.fail("Data Extraction failed during ETL extraction process")

    @pytest.mark.regression
    def test_DataExtractionFromOracleToStagingmySQL(self,mysql_engine,oracle_engine):
        try:
            logger.info("test case for Stores Data extraction has started ....")
            query_expected = """select * from stores"""
            query_actual= """select * from staging_stores"""
            reconcile_db_to_db(query_expected,oracle_engine,query_actual, mysql_engine)
            logger.info("test case for Stores Data extraction has completed ....")
        except Exception as e:
            logger.error(f"test case for Stores Data extraction has failed{e}")
            pytest.fail("Data Extraction failed during ETL extraction process")
