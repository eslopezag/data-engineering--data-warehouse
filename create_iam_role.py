'''
This script creates the IAM Role that allows Redshift to read files from S3.
'''

import boto3
import configparser
import json

# Read configuration file:
config = configparser.ConfigParser()
config.read_file(open('dwh.cfg'))

# Get configuration variables:
KEY = config.get('AWS', 'KEY')
SECRET = config.get('AWS', 'SECRET')

DWH_IAM_ROLE_NAME = config.get('IAM_ROLE', 'ROLE_NAME')

# Define IAM client:
iam = boto3.client('iam',
                   region_name='us-west-2',
                   aws_access_key_id=KEY,
                   aws_secret_access_key=SECRET
)

# Create the role:
try:
    dwhRole = iam.create_role(
        Path = '/',
        RoleName = DWH_IAM_ROLE_NAME,
        Description = 'Allows Redshift clusters to call AWS services on your behalf.',
        AssumeRolePolicyDocument=json.dumps(
            {'Statement': [{'Action': 'sts:AssumeRole',
               'Effect': 'Allow',
               'Principal': {'Service': 'redshift.amazonaws.com'}}],
             'Version': '2012-10-17'})
    )    
except Exception as e:
    print(e)
    
    
# Attach S3 policy:
iam.attach_role_policy(RoleName=DWH_IAM_ROLE_NAME,
                       PolicyArn="arn:aws:iam::aws:policy/AmazonS3ReadOnlyAccess"
                      )

# Get ARN:
roleArn = iam.get_role(RoleName=DWH_IAM_ROLE_NAME)['Role']['Arn']
print('ARN:', roleArn, sep=' ')