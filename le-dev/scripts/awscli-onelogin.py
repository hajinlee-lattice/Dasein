#!/usr/bin/env python3
### !/usr/bin/python3
# -*- coding: utf-8 -*-

import sys
import boto
from  botocore.exceptions import ClientError
import boto3
import requests
import getpass
import configparser
import base64
import logging
import xml.etree.ElementTree as ET
import re
import os
import subprocess
from os.path import expanduser
from urllib.parse import urlparse, urlunparse
import json

"""
Version: 0.9.0
Author: Neil Palmere
Author: Tom Ockenhouse

Adapted from the script found at:
https://s3.amazonaws.com/awsiammedia/public/sample/SAMLAPICLIADFS/samlapi_formauth.py
(Retrieved: 11/7/2016)

Required installs:
Python3 (Tested against Python 3.5.2)
pip install requests-ntlm
pip install boto
pip install boto3

Program Flow
---------------------
# Get username and password from user
# Generate an oAuth2 token using the pre-generated keypair
# Send the oAuth2 token and the entered username and password to OneLogin
# (optional) Handle Multi-Factor authentication
# Get back the SAML assertion
# Decide what ARN/Role/Account to use
# Send ARN and Assertion to AWS
# Write the received keypair to the credentials file
# Do any automated tasks since we have the keypair in memory already

BOTO3 reference for automation:
http://boto3.readthedocs.io/en/latest/reference/services/index.html
"""


##########################################################################
# Variables

# region: The default AWS region that this script will connect
# to for all API calls
region = 'us-east-1'

# output format: The AWS CLI output format that will be configured in the
# saml profile (affects subsequent CLI calls)
outputformat = 'json'

# awsconfigfile: The file where this script will store the temp
# credentials under the saml profile
awsconfigfile = 'credentials'
# filename = os.path.join(os.getcwd(), awsconfigfile)

if 'HOME' in os.environ:
    home = os.environ['HOME']
elif os.name == 'posix':
    home = os.path.expanduser("~/")
elif os.name == 'nt':
    if 'HOMEPATH' in os.environ and 'HOMEDRIVE' in os.environ:
        home = os.environ['HOMEDRIVE'] + os.environ['HOMEPATH']
else:
    home = os.environ['HOMEPATH']

filename = os.path.join(home,'.aws', awsconfigfile)

filepath = os.path.normpath(filename)

# First time users of the AWS CLI will not have .aws folder as user will be unable to configure their
# CLI tool without the access and secret keys which are issued by this script. The following lines creates 
# the .aws folder if it does not exist. --mmoro

if not os.path.exists(os.path.dirname(filepath)):
    os.mkdir(os.path.dirname(filepath))

# SSL certificate verification: Whether or not strict certificate
# verification is done, False should only be used for dev/test
sslverification = True

# Uncomment to enable low level debugging
#logging.basicConfig(level=logging.DEBUG)
#boto.set_stream_logger('boto')

# Don't output traceback on errors
# Should be 0, but been an issue since 2011... https://bugs.python.org/issue12276
sys.tracebacklimit = 1

"""OneLogin Settings"""
# idpentryurl: 			| The initial url that starts the authentication process.
# API ID:				| The OneLogin API client_id
# API Key:				| 
# OneLogin App ID:		|
# OneLogin Subdomain:	|
idpentryurl = 'https://dnb.onelogin.com/trust/saml2/http-post/sso/627857'
clientid = 'd18e88f776d92b765dfd3e45ae29c9627ef5fa767eef830437167f147f7d3ce2'
clientsecret = '82ee31f85a340ca559b0819cc67c14be87d3666f9c98a8d6737241f9fd40b0e0'
appid = '627857'
subdomain = 'dnb'

##########################################################################

# Print Banner
# http://patorjk.com/software/taag/#p=display&c=c&f=ANSI Shadow&t= AWS%0AOneLogin
print('                                                                      ')
print('                         █████╗ ██╗    ██╗███████╗                    ')
print('                        ██╔══██╗██║    ██║██╔════╝                    ')
print('                        ███████║██║ █╗ ██║███████╗                    ')
print('                        ██╔══██║██║███╗██║╚════██║                    ')
print('                        ██║  ██║╚███╔███╔╝███████║                    ')
print('                        ╚═╝  ╚═╝ ╚══╝╚══╝ ╚══════╝                    ')
print('                                                                      ')
print('     ██████╗ ███╗   ██╗███████╗██╗      ██████╗  ██████╗ ██╗███╗   ██╗')
print('    ██╔═══██╗████╗  ██║██╔════╝██║     ██╔═══██╗██╔════╝ ██║████╗  ██║')
print('    ██║   ██║██╔██╗ ██║█████╗  ██║     ██║   ██║██║  ███╗██║██╔██╗ ██║')
print('    ██║   ██║██║╚██╗██║██╔══╝  ██║     ██║   ██║██║   ██║██║██║╚██╗██║')
print('    ╚██████╔╝██║ ╚████║███████╗███████╗╚██████╔╝╚██████╔╝██║██║ ╚████║')
print('     ╚═════╝ ╚═╝  ╚═══╝╚══════╝╚══════╝ ╚═════╝  ╚═════╝ ╚═╝╚═╝  ╚═══╝')
print('                                                                      ')



# Get the federated credentials from the user
username = input("Username: ").lower()
password = getpass.getpass()
print('')

# Generate token
token_url = 'https://api.us.onelogin.com/auth/oauth2/token'
token_auth_header = 'client_id:' + clientid + ',client_secret:' + clientsecret
token_headers = {
	'Content-Type':'application/json', 
	'Authorization':token_auth_header
	}
token_json = {'grant_type':'client_credentials'}
token_response = requests.post(token_url, headers=token_headers, json=token_json)

# Use token to generate assertion
token_response_json = json.loads(token_response.text)
accesstoken = token_response_json['data'][0]['access_token']
print('')

assertion_url='https://api.us.onelogin.com/api/1/saml_assertion'
assertion_auth_header = 'bearer:' + accesstoken
assertion_headers = {'Content-Type':'application/json', 'Authorization':assertion_auth_header}
assertion_json = {
	"username_or_email": username,
    "password": password,
    "app_id": appid,
    "subdomain": subdomain
	}
assertion_response = requests.post(assertion_url, headers=assertion_headers, json=assertion_json)

assertion_response_json = json.loads(assertion_response.text)

# Check for MFA or Failure
assertion_message = assertion_response_json['status']['message']

assertion = ''
if assertion_message == 'Success':
	#print('Assertion request successful')
	assertion = assertion_response_json['data']
elif assertion_message == 'MFA is required for this user':
	mfa_state_token = assertion_response_json['data'][0]['state_token']
	# TODO: Should loop over these and present a list of options
	first_mfa_name = assertion_response_json['data'][0]['devices'][0]['device_type']
	first_mfa_id = str( assertion_response_json['data'][0]['devices'][0]['device_id'] )
	mfa_callback_url = assertion_response_json['data'][0]['callback_url']
	print('MFA Factor requested: ' + first_mfa_name)
	otp = input("Please enter the One Time Password (OTP): ")
	
	mfa_auth_header = 'bearer:' + accesstoken
	mfa_headers = {'Content-Type':'application/json', 'Authorization':mfa_auth_header}
	mfa_json = {
		"app_id": appid,
		"otp_token": otp,
		"device_id": first_mfa_id,
		"state_token": mfa_state_token
		}
	mfa_response = requests.post(mfa_callback_url, headers=mfa_headers, json=mfa_json)
	mfa_response_json = json.loads(mfa_response.text)
	mfa_message = mfa_response_json['status']['message']
	#print(mfa_message)
	if mfa_message == 'Success':
		print('')
		#print('Multi-factor Authentication request successful')
		assertion = mfa_response_json['data']
else:
	print(assertion_message)
	sys.exit(0)
	
# Better error handling is required for production use
# We can let AWS deal with it for now
if (assertion == ''):
    print('Response did not contain a valid SAML assertion')
    sys.exit(0)
else:
	#TODO: Insert valid error checking/handling
	print('Valid SAML Assertion received')

# Debug the SAML assertion
#print(base64.b64decode(assertion))

# Parse the returned assertion and extract the authorized roles
awsroles = []
root = ET.fromstring(base64.b64decode(assertion))
for saml2attribute in root.iter('{urn:oasis:names:tc:SAML:2.0:assertion}Attribute'):
    if (saml2attribute.get('Name') == 'https://aws.amazon.com/SAML/Attributes/Role'):
        for saml2attributevalue in saml2attribute.iter('{urn:oasis:names:tc:SAML:2.0:assertion}AttributeValue'):
            awsroles.append(saml2attributevalue.text)

# Note the format of the attribute value should be role_arn,principal_arn
# but lots of blogs list it as principal_arn,role_arn so let's reverse
# them if needed
for awsrole in awsroles:
    chunks = awsrole.split(',')
    if'saml-provider' in chunks[0]:
        newawsrole = chunks[1] + ',' + chunks[0]
        index = awsroles.index(awsrole)
        awsroles.insert(index, newawsrole)
        awsroles.remove(awsrole)

# If I have more than one role, ask the user which one they want,
# otherwise just proceed
print("")
if len(awsroles) > 1:
    i = 0
    print("Please choose the role you would like to assume:")
    for awsrole in awsroles:
        aws_account_num = awsrole.split(':')[4]
        aws_role_name = awsrole.split(',')[0].split('/')[1]
        #print('[', i, ']: ', awsrole.split(',')[0]
        ## Format role information asthetically --mmoro
        print('[', i, ']: ', aws_role_name + ' (' + aws_account_num + ')')
        i += 1
    selectedroleindex = input("Selection: ")

    # Basic sanity check of input
    if int(selectedroleindex) > (len(awsroles) - 1):
        print('You selected an invalid role index, please try again')
        sys.exit(0)

    role_arn = awsroles[int(selectedroleindex)].split(',')[0]
    principal_arn = awsroles[int(selectedroleindex)].split(',')[1]
else:
    role_arn = awsroles[0].split(',')[0]
    principal_arn = awsroles[0].split(',')[1]
    print('Only one role available. role_arn: ' + role_arn + ' principal_arn: ' + principal_arn)

# Use the assertion to get an AWS STS token using Assume Role with SAML
client = boto3.client('sts', region_name=region)
print('')
#print('Connection to AWS established')

aws_sts_token_response = None
try:
    aws_sts_token_response = client.assume_role_with_saml(
        RoleArn = role_arn,
        PrincipalArn = principal_arn,
        SAMLAssertion = assertion,
        DurationSeconds=14400
    )
except ClientError as ex:
    # dirty assumption:  Account only allows 1 hour keys
    aws_sts_token_response = client.assume_role_with_saml(
        RoleArn = role_arn,
        PrincipalArn = principal_arn,
        SAMLAssertion = assertion
    )
    
print('Role assumed, writing credential')

# Write the AWS STS token into the AWS credential file

# Read in the existing config file
config = configparser.RawConfigParser()
config.read(filepath)

# Put the credentials into default section
if not config.has_section('default'):
    config.add_section('default')

config.set('default', 'output', outputformat)
config.set('default', 'region', region)
config.set('default', 'aws_access_key_id', aws_sts_token_response['Credentials']['AccessKeyId'])
config.set('default', 'aws_secret_access_key', aws_sts_token_response['Credentials']['SecretAccessKey'])
config.set('default', 'aws_session_token', aws_sts_token_response['Credentials']['SessionToken'])
config.set('default', 'expiration', aws_sts_token_response['Credentials']['Expiration'])

# Write the updated config file
with open(filename, 'w+') as configfile:
    config.write(configfile)

# Give the user some basic info as to what has just happened
print('\n\n-------------------------------------------------------------------')
print('Your new access key pair has been stored in the AWS configuration file:')
print('{0} under the default profile.'.format(filename))
print('Note that it will expire at: {0}.'.format(aws_sts_token_response['Credentials']['Expiration']))
print('Re-run script to refresh your credentials                              ')
print('-------------------------------------------------------------------\n\n')

