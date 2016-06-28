#!/usr/bin/env python

import argparse
import boto3
import sys
from environment import Environment
from botocore.exceptions import ClientError

from utils import log
import utils

REGION = 'us-west-1'
STACK_NAME = 'LatticeEngines'
CONF_DIR = 'conf'


def main(args):
    args = args[1:]
    parser = argparse.ArgumentParser(description="Setup AWS stack")
    parser.add_argument('environment', help="Environment name (prod, prodstaging, qa)")
    parser.add_argument('aws_access_key', help="AWS access key tied to the account being deployed")
    parser.add_argument('aws_secret_key', help="AWS secret key tied to the account being deployed")
    namespace = parser.parse_args(args)
    with open(CONF_DIR + '/template.json', 'r') as template_file:
        template = template_file.read()

    environment = Environment(namespace.environment)

    client = boto3.client('cloudformation',
                          aws_access_key_id=namespace.aws_access_key,
                          aws_secret_access_key=namespace.aws_secret_key,
                          region_name=REGION)

    stack = utils.get_stack(client, STACK_NAME)

    if stack is not None:
        log("Updating stack {0} in {1}...", STACK_NAME, namespace.environment)
        try:
            client.update_stack(
                StackName=STACK_NAME,
                TemplateBody=template,
                Parameters=environment.parameters)
            waiter = client.get_waiter('stack_update_complete')
            waiter.wait(StackName=STACK_NAME)
        except ClientError as environment:
            if environment.message == "An error occurred (ValidationError) when calling the UpdateStack operation: No updates are to be performed.":
                log("No updates detected")
                exit()
            else:
                raise
        log("Successfully updated stack {0}", STACK_NAME)

    else:
        log("Creating stack {0} in {1}...", STACK_NAME, namespace.environment)
        client.create_stack(
            StackName=STACK_NAME,
            TemplateBody=template,
            Parameters=environment.parameters)
        waiter = client.get_waiter('stack_create_complete')
        waiter.wait(StackName=STACK_NAME)

        log("Successfully created stack {0}", STACK_NAME)

if __name__ == '__main__':
    main(sys.argv)
