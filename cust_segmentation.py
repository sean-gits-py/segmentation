import pandas as pd
import datetime
import pytz
import time
import pyspark
import io
import boto3
import psycopg2 as pg
from sshtunnel import SSHTunnelForwarder

scrub = pd.read_csv('scrub_list.csv', encoding = "ISO-8859-1")
scrub_list = list(scrub.acct_org_id.unique())

def list_to_sql(py_list):
    """Takes a list generated in python and converst it into a string suitable for use in SQL queries.
    This function is designed to be used in WHERE _ IN {result}.
    """
    result = '('
    for rec in py_list[:-1]:
        result += str(rec) + ", "
    result += str(py_list[-1]) + ')'
    return result

def segment(row):
    if row['sales'] == 0:
        text = 'No Purchases'
    elif row['running_pct'] < 0.5:
        text = 'A'
    elif row['running_pct'] < 0.75:
        text = 'B'
    else:
        text = 'C'
    return text
    
    
def quarters_range(date_to, date_from=None):
    result = []
    if date_from is None:
        date_from = datetime.now()
    quarter_to = (date_to.month/4)+1
    for year in range(date_from.year, date_to.year+1):
        for quarter in range(1, 5):
            if date_from.year == year and quarter <= quarter_to:
                continue
            if date_to.year == year and quarter > quarter_to:
                break
            result.append([quarter, year])
    return result


conf = pyspark.SparkConf().setAll([('spark.executor.memory', '8g'), ('spark.executor.cores', '8'), ('spark.cores.max', '8'), ('spark.driver.memory','8g')])
spark = pyspark.sql.SparkSession.builder \
    .master('local[*]') \
    .appName('kpi') \
    .getOrCreate()

## Read in data 
kpi_metrics_2017 = "s3a://censored/metrics/kpi/2017//*/*"
kpi_metrics_2018 = "s3a://censored/metrics/kpi/2018//*/*"


kpi_metrics_df = spark.read.csv([kpi_metrics_2017, kpi_metrics_2018], header=True, sep='^', inferSchema=True)
kpi_metrics_df.createOrReplaceTempView('kpi_metrics_df')

kpi_metrics_sql = """SELECT date
                           ,acct_org_id
                           ,purchase_ct as sales
                     FROM kpi_metrics_df
                     WHERE acct_org_id NOT IN {customer_list} 

           """.format(customer_list = list_to_sql(scrub_list))

kpi_df = spark.sql(kpi_metrics_sql).toPandas()

#Capture the most recent date of sales
kpi_df['through_dt'] = kpi_df.date.max()

#Create year and quarter fields
kpi_df['quarter'] = pd.to_datetime(kpi_df['date']).dt.quarter
kpi_df['year'] = pd.to_datetime(kpi_df['date']).dt.year

#Roll up sales to acct_org_id/year/quarter level 
qtr_sum_df = kpi_df.groupby(['acct_org_id','year','quarter','through_dt']).agg({'sales':'sum'}).reset_index()

#Sort year/quarter subsets by sales in descending order
qtr_sum_df_ord = qtr_sum_df.groupby(['year','quarter','through_dt'], sort=False).apply(lambda x: x.sort_values(['sales','acct_org_id'],ascending= [False, True])).reset_index(drop=True)

#Calculate cumulative sales per year/quarter
qtr_sum_df_ord['cumulsales'] = qtr_sum_df_ord.groupby(['year','quarter','through_dt'])['sales'].transform('cumsum')

#Calculate cumulative percent of sales per year/quarter
qtr_sum_df_ord['running_pct'] = qtr_sum_df_ord['cumulsales'] / qtr_sum_df_ord.groupby(['year','quarter','through_dt'])['sales'].transform('sum')

#Rank acct_org_ids within year/quarter by sales (from highest to lowest)
qtr_sum_df_ord['rank'] = qtr_sum_df_ord.groupby(['year','quarter','through_dt']).cumcount()+1

#Assign segments to acct_org_ids based on cumulative percent within year/quarter
qtr_sum_df_ord['segment'] = qtr_sum_df_ord.apply(segment, axis = 1)

#Create a dataframe of all quarters within a reporting window
rpt_qrt = pd.DataFrame.from_records(quarters_range(kpi_df.date.max(), date_from=kpi_df.date.min()), columns=['quarter', 'year']) 

qtr_sum_df_ord_ = rpt_qrt.merge(qtr_sum_df_ord, how = 'left', on=['quarter', 'year'])
