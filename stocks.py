from airflow import DAG
from airflow.operators.python_operator import PythonOperator

from datetime import datetime, timedelta  
import datetime as dt
import pandas as pd
import yfinance as yf
import requests
import lxml
from functools import reduce

tickers = ['SUNPHARMA', 'TATAMOTORS', 'TCS', 'TECHM', 'BHEL', 'RELIANCE', 'INFY', 'HCLTECH' 'BHARTIARTL'] # <-- Initial Tickers List. It will be available globally for all functions.


####################################################
# 1. DEFINE PYTHON FUNCTIONS
####################################################

def fetch_prices_function(**kwargs): # <-- Remember to include "**kwargs" in all the defined functions 
    print('1 Fetching stock prices and remove duplicates...')
    stocks_prices = []
    for i in range(0, len(tickers)):
        prices = yf.download(tickers[i], period = 'max').iloc[: , :5].dropna(axis=0, how='any')
        prices = prices.loc[~prices.index.duplicated(keep='last')]
        prices = prices.reset_index()
        prices.insert(loc = 1, column = 'Stock', value = tickers[i])
        stocks_prices.append(prices)
    return stocks_prices  # <-- This list is the output of the fetch_prices_function and the input for the functions below
    print('Completed \n\n')
        
 
def stocks_plot_function(**kwargs): 
    print('2 Pulling stocks_prices to concatenate sub-lists to create a combined dataset + write to CSV file...')
    ti = kwargs['ti']
    stocks_prices = ti.xcom_pull(task_ids='fetch_prices_task') # <-- xcom_pull is used to pull the stocks_prices list generated above
    stock_plots_data = pd.concat(stocks_prices, ignore_index = True)
    stock_plots_data.to_csv('/home/aj/new/stocks_plots_data.csv', index = False)
    
    print('DF Shape: ', stock_plots_data.shape)
    print(stock_plots_data.head(5))
    print('Completed \n\n')

#__________________________________________________________________________________

default_args = {
     'owner': 'airflow',
     'depends_on_past': False,
     'email': ['user@gmail.com'],
     'email_on_failure': False,
     'email_on_retry': False,
     'retries': 1
    }


from pyspark import SparkFiles
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
#from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DateType, TimestampType, FloatType
import findspark
findspark.init()
from datetime import datetime
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator


def airflow_dump():


	mongodb_spark_jars_path="/home/aj/dump_mongo"
	spark=SparkSession.builder.appName("stocks")\
    	  	.master("local")\
	       	.config("spark.mongodb.input.uri","mongodb://localhost:27017/Project1.stocks")\
		.config("spark.mongodb.output.uri","mongodb://localhost:27017/Project1.stocks")\
	        .config("spark.jars", ""+mongodb_spark_jars_path+"/mongo-spark-connector_2.12-3.0.1.jar,"
            	+mongodb_spark_jars_path+"/bson-4.0.5.jar,"
           	+mongodb_spark_jars_path+"/mongodb-driver-core-4.0.5.jar,"
            	+mongodb_spark_jars_path+"/mongodb-driver-sync-4.0.5.jar")\
        	.getOrCreate()
         

	df = spark.read.csv(path = "/home/aj/new/stocks_plots_data.csv", header=True, inferSchema=True)
																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																														


	df.write.format("mongo").mode("overwrite").option("uri","mongodb://localhost:27017/Project1.stocks").save()
	return "Data Saved To MongoDB Successfully.. "
   



dag = DAG( 'stocks_analysis_ETL_7AM',
            default_args=default_args,
            description='Collect Stock Prices For Analysis',
            catchup=True,
            start_date= datetime(2020, 6, 23), 
            schedule_interval= '*/15 * * * 1-5'  
          )  

##########################################
#3. DEFINE AIRFLOW OPERATORS
##########################################

fetch_prices_task = PythonOperator(task_id = 'fetch_prices_task', 
                                   python_callable = fetch_prices_function, 
                                   provide_context = True,
                                   dag= dag )


stocks_plot_task= PythonOperator(task_id = 'stocks_plot_task', 
                                 python_callable = stocks_plot_function,
                                 provide_context = True,
                                 dag= dag)


stocks_dump_mongodb=PythonOperator(task_id='Stocks_Data',python_callable=airflow_dump,dag=dag)

'''
stocks_dump_mongodb=PythonOperator(task_id = 'airflow_dump', 
                                   python_callable = airflow_dump, 
                                   provide_context = True,
                                   dag= dag )
'''
##########################################
#4. DEFINE OPERATORS HIERARCHY
##########################################

fetch_prices_task  >> stocks_plot_task >> stocks_dump_mongodb
