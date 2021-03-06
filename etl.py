'''
This script implements the ETL pipeline that extracts the data from S3, loads it into
staging tables in Redshift and populates the star schema tables with the transformed
data.
'''

import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries


def load_staging_tables(cur, conn):
    '''
    Loads the data from S3 into staging tables in Redshift
    using the queries in `copy_table_queries` list.
    '''
    for query in copy_table_queries:
        cur.execute(query)
        conn.commit()


def insert_tables(cur, conn):
    '''
    Inserts the transformed data into the star schema tables
    using the queries in `insert_table_queries` list.
    '''
    for query in insert_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    '''
    - Reads the configuration file. 
    
    - Establishes connection with Redshift.
    
    - Loads the data from S3 into staging tables in Redshift.
    
    - Inserts the transformed data into the star schema tables. 
    
    - Finally, closes the connection.
    '''    
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()
    
    load_staging_tables(cur, conn)
    insert_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()