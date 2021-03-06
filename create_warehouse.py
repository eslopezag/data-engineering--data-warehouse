'''
This script creates the Redshift cluster that will server as Sparkify's data warehouse.
'''

import boto3
import configparser

# Read configuration file:
config = configparser.ConfigParser()
config.read_file(open('dwh.cfg'))

# Get configuration variables:
KEY = config.get('AWS', 'KEY')
SECRET = config.get('AWS', 'SECRET')

DWH_CLUSTER_TYPE = config.get('DWH', 'CLUSTER_TYPE')
DWH_NUM_NODES = config.get('DWH', 'NUM_NODES')
DWH_NODE_TYPE = config.get('DWH', 'NODE_TYPE')

DWH_CLUSTER_IDENTIFIER = config.get('DWH', 'CLUSTER_IDENTIFIER')
DWH_DB_NAME = config.get('CLUSTER', 'DB_NAME')
DWH_DB_USER = config.get('CLUSTER', 'DB_USER')
DWH_DB_PASSWORD = config.get('CLUSTER', 'DB_PASSWORD')
DWH_PORT = config.get('CLUSTER', 'DB_PORT')

IAM_ROLE = config.get('IAM_ROLE', 'ARN')

# Define the Redshift client:
redshift = boto3.client('redshift',
                       region_name='us-west-2',
                       aws_access_key_id=KEY,
                       aws_secret_access_key=SECRET
                       )

# Create the Redshift cluster:
try:
    response = redshift.create_cluster(        
        ClusterType=DWH_CLUSTER_TYPE,
        NodeType=DWH_NODE_TYPE,
        NumberOfNodes=int(DWH_NUM_NODES),

        # Identifiers & credentials:
        DBName = DWH_DB_NAME,
        ClusterIdentifier = DWH_CLUSTER_IDENTIFIER,
        MasterUsername = DWH_DB_USER,
        MasterUserPassword = DWH_DB_PASSWORD,
        
        # Roles (for S3 access):
        IamRoles=[IAM_ROLE]  
    )
except Exception as e:
    print(e)
    