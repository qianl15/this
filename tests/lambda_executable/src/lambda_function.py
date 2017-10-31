######################################################
# -*- coding: utf-8 -*-
# File Name: lambda_function.py
# Author: Qian Li
# Created Date: 2017-10-26
# Description: demonstrate how to call an executable 
# and pass the parameters from Lambda function
######################################################
import json
import urllib
import boto3
import os

from executable import Executable

def lambda_handler(event, context):
  user_str = 'aaa'
  exe_file = 'hello'
  try:
    # API Gateway GET method
    if event['httpMethod'] == 'GET':
      user_str = event['queryStringParameters']['str']
      exe_file = event['queryStringParameters']['bin']
    # API Gateway POST method
    elif event['httpMethod'] == 'POST':
      data = json.loads(event['body'])
      user_str = data['str']
      exe_file = data['bin']
  except KeyError:
    # direct invocation
    user_str = event['str']
    exe_file = event['bin']

  user_str = '\"' + user_str + '\"'
  exe = Executable('executables/' + exe_file)
  result = exe.run('{}'.format(user_str))
  print('OUT: {}\nERR: {}\nRET: {}'.format(
    exe.stdout, exe.stderr, exe.returncode))
  out = {
          "headers": {
              "content-type": "application/json",
              "Access-Control-Allow-Origin": "*"
              },
          "body": exe.stdout,  
          "statusCode": 200
        }
  return out