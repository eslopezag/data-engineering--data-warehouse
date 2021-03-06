'''
This script deletes the Redshift cluster that serves as Sparkify's data warehouse.
'''

import boto3
import configparser

# Read configuration file:
config = configparser.ConfigParser()
config.read_file(open('dwh.cfg'))

# Define configuration variables:
KEY = config.get('AWS', 'KEY')
SECRET = config.get('AWS', 'SECRET')

DWH_CLUSTER_IDENTIFIER = config.get('DWH', 'CLUSTER_IDENTIFIER')

# Define the Redshift client:
redshift = boto3.client('redshift',
                       region_name='us-west-2',
                       aws_access_key_id=KEY,
                       aws_secret_access_key=SECRET
                       )

# Delete the Redshift cluster:
redshift.delete_cluster( ClusterIdentifier=DWH_CLUSTER_IDENTIFIER,  SkipFinalClusterSnapshot=True)