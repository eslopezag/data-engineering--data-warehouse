'''
This script opens the port of the VPC where the Redshift cluster operates so
that it can be accessed through the web.

DO NOT RUN THIS unless the cluster status becomes "Available".
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
DWH_PORT = config.get('CLUSTER', 'DB_PORT')

IAM_ROLE = config.get('IAM_ROLE', 'ARN')

# Define the Redshift and EC2 clients:
redshift = boto3.client('redshift',
                       region_name='us-west-2',
                       aws_access_key_id=KEY,
                       aws_secret_access_key=SECRET
                       )

ec2 = boto3.resource('ec2',
                       region_name='us-west-2',
                       aws_access_key_id=KEY,
                       aws_secret_access_key=SECRET
                    )

# Get the cluseter description and its VPC ID:
cluster = redshift.describe_clusters(ClusterIdentifier=DWH_CLUSTER_IDENTIFIER)['Clusters'][0]

vpc_id = cluster['VpcId']

# Open an incoming TCP port to access the cluster ednpoint:
try:
    vpc = ec2.Vpc(id=vpc_id)
    defaultSg = list(vpc.security_groups.all())[0]
    print(defaultSg)
    defaultSg.authorize_ingress(
        GroupName=defaultSg.group_name,
        CidrIp='0.0.0.0/0',
        IpProtocol='TCP',
        FromPort=int(DWH_PORT),
        ToPort=int(DWH_PORT)
    )
except Exception as e:
    print(e)