import logging
import pandas as pd
import pytest
from sqlalchemy import create_engine

# The logger configuration
from CommonUtilities.utils import reconcile_file_to_db, getDataFromLinuxBox, reconcile_db_to_db

logging.basicConfig(
    filename='Logs/testExecutionTransfromation.log',  # Name of the log file
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


def test_DataTransfromation_FilterCheck():
    try:
        logger.info("test case for Filter Data Tranformation has started ....")
        query_expected = """select * from staging_sales where sale_date >='2024-09-10'"""
        query_actual= """select * from filtered_sales_data"""
        reconcile_db_to_db(query_expected,mysql_engine,query_actual, mysql_engine)
        logger.info("test case for Filter Data Tranformation has completed ....")
    except Exception as e:
        logger.error(f"test case for Filter Data Tranformation has failed{e}")
        pytest.fail("Data Extraction failed during ETL extraction process")

def test_DataTransfromation_Router_Low_Check():
    try:
        logger.info("test case for Router - Low Data Tranformation has started ....")
        query_expected = """select * from filtered_sales_data where region='Low'"""
        query_actual= """select * from low_sales"""
        reconcile_db_to_db(query_expected,mysql_engine,query_actual, mysql_engine)
        logger.info("test case for Router - Low Data Tranformation has completed ....")
    except Exception as e:
        logger.error(f"test case for Router - Low Data Tranformation has failed{e}")
        pytest.fail("Data Extraction failed during ETL extraction process")

def test_DataTransfromation_Router_High_Check():
    try:
        logger.info("test case for Router - High Data Tranformation has started ....")
        query_expected = """select * from filtered_sales_data where region='High'"""
        query_actual= """select * from high_sales"""
        reconcile_db_to_db(query_expected,mysql_engine,query_actual, mysql_engine)
        logger.info("test case for Router - High Data Tranformation has completed ....")
    except Exception as e:
        logger.error(f"test case for Router - High Data Tranformation has failed{e}")
        pytest.fail("Data Extraction failed during ETL extraction process")

def test_DataTransfromation_aggregator_sales_Check():
    try:
        logger.info("test case for aggregator_sales Data Tranformation has started ....")
        query_expected = """select product_id,month(sale_date) as month ,year(sale_date) as year,sum(price*quantity) as total_sales from filtered_sales_data group by product_id,month(sale_date),year(sale_date);"""
        query_actual= """select * from monthly_sales_summary_source"""
        reconcile_db_to_db(query_expected,mysql_engine,query_actual, mysql_engine)
        logger.info("test case for aggregator_sales Data Tranformation has completed ....")
    except Exception as e:
        logger.error(f"test case for aggregator_sales Data Tranformation has failed{e}")
        pytest.fail("Data Extraction failed during ETL extraction process")

def test_DataTransfromation_aggregator_inventory_level_Check():
    try:
        logger.info("test case for aggregator_inventory_level Data Tranformation has started ....")
        query_expected = """select store_id,sum(quantity_on_hand) as total_quantity_per_store from staging_inventory group by store_id;"""
        query_actual= """select * from aggregated_inventory_level"""
        reconcile_db_to_db(query_expected,mysql_engine,query_actual, mysql_engine)
        logger.info("test case for aggregator_inventory_level Data Tranformation has completed ....")
    except Exception as e:
        logger.error(f"test case for aggregator_inventory_level Data Tranformation has failed{e}")
        pytest.fail("Data Extraction failed during ETL extraction process")


def test_DataTransfromation_joiner_sale_data_Check():
    try:
        logger.info("test case for aggregator_inventory_level Data Tranformation has started ....")
        query_expected = """select fs.sales_id,fs.quantity,fs.sale_date,fs.price,fs.quantity*fs.price as total_sales,p.product_id,
            p.product_name,s.store_id,s.store_name
            from filtered_sales_data as fs  
            inner join staging_product as p on p.product_id = fs.product_id
            inner join staging_stores as s on s.store_id = fs.store_id
            """
        query_actual= """select * from sales_with_details"""
        reconcile_db_to_db(query_expected,mysql_engine,query_actual, mysql_engine)
        logger.info("test case for joiner_sale_data Data Tranformation has completed ....")
    except Exception as e:
        logger.error(f"test case for joiner_sale_data Data Tranformation has failed{e}")
        pytest.fail("Data Extraction failed during ETL extraction process")
