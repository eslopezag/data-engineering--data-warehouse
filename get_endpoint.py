'''
This script gets the address to the Redshift cluster's endpoint
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

# Get the cluster description and print its endpoint address:
cluster = redshift.describe_clusters(ClusterIdentifier=DWH_CLUSTER_IDENTIFIER)['Clusters'][0]

endpoint = cluster['Endpoint']['Address']
print("The cluster's endpoint address is:", endpoint, sep=' ')