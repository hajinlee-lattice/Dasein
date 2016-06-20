import boto3
import json
import os
import time
from boto3.s3.transfer import S3Transfer

from .ec2 import _ec2_mappings, _ec2_params, _ec2_security_group, SECURITY_GROUP, EC2Instance
from .resource import Resource
from .template import Template, TEMPLATE_DIR

S3_BUCKET= 'yintaosong'

class Stack(Template):
    def __init__(self, description):
        Template.__init__(self)
        self._template["AWSTemplateFormatVersion"] = "2010-09-09"
        self._template["Description"] = description
        self._template["Parameters"] = Stack.__common_params()
        self.__ec2_params()
        self.__ec2_mappings()

    def add_ec2(self, instance, create_sg=False):
        assert isinstance(instance, EC2Instance)
        name = instance.logical_id()

        # security group
        if create_sg:
            sg = _ec2_security_group()
            data = {
                SECURITY_GROUP: sg
            }
            self._merge_into_attr('Resources', data)

        # ec2 instance
        data = {
            name: instance.template(),
        }
        self._merge_into_attr('Resources', data)

        # output info
        output = {
            name + "URL": {
                "Description" : "URL for EC2 instance " + name,
                "Value" : { "Fn::GetAtt" : [ name, "PublicDnsName" ]}
            },
            name + "PublicIp": {
                "Description" : "Public IP for EC2 instance " + name,
                "Value" : { "Fn::GetAtt" : [ name, "PublicIp" ]}
            },
            name + "PrivateIp": {
                "Description" : "Private IP for EC2 instance " + name,
                "Value" : { "Fn::GetAtt" : [ name, "PrivateIp" ]}
            }
        }
        self._merge_into_attr('Outputs', output)
        return self

    def add_resource(self, resource):
        assert isinstance(resource, Resource)
        self._merge_into_attr("Resources", {resource.logical_id(): resource.template()})

    def add_resources(self, resources):
        for resource in resources:
            self.add_resource(resource)
        return self

    def add_params(self, params):
        for k, v in params.items():
            self._template["Parameters"][k] = v
        return self

    def validate(self):
        print 'Validating template against AWS ...'
        client = boto3.client('cloudformation')
        client.validate_template(
            TemplateBody=self.json()
        )
        print 'Stack template is valid.'

    def upload(self, prefix):
        temp_file = "/tmp/zookeeper.json"
        with open(temp_file, 'w') as tf:
            tf.write(self.json())
        print 'uploading template to %s' % (os.path.join("https://s3.amazonaws.com", S3_BUCKET, prefix, 'template.json') + ' ..')
        client = boto3.client('s3')
        transfer = S3Transfer(client)
        transfer.upload_file(temp_file, S3_BUCKET, os.path.join(prefix, 'template.json'))
        print 'done.'

    def __ec2_mappings(self):
        data = _ec2_mappings()
        self._merge_into_attr('Mappings', data)
        return self

    def __ec2_params(self):
        data = _ec2_params()
        self._merge_into_attr('Parameters', data)
        return self

    @classmethod
    def __common_params(cls):
        json_file = os.path.join(TEMPLATE_DIR, 'common', 'common_params.json')
        with open(json_file) as f:
            return json.load(f)

def check_stack_not_exists(client, stackname):
    print 'verifying stack name "%s"...' % stackname
    response = client.list_stacks()
    for stack in response['StackSummaries']:
        if stack['StackStatus'] != 'DELETE_COMPLETE' and stack['StackName'] == stackname:
            raise ValueError('There is already a stack named "%s": %s' % (stackname, stack))
    print 'Great! The name "%s" has not been used.' % stackname

def wait_for_stack_creation(client, stackname):
    print 'Waiting for the stack to be ready ...'
    t1 = time.time()
    waiter = client.get_waiter('stack_create_complete')
    waiter.wait(StackName=stackname)
    t2 = time.time()
    print 'Done. %.2f seconds.' % (t2 -t1)

def teardown_stack(client, stackname):
    client.delete_stack(StackName=stackname)
    wait_for_stack_teardown(client, stackname)

def wait_for_stack_teardown(client, stackname):
    print 'Waiting for the stack to be teardown ...'
    t1 = time.time()
    waiter = client.get_waiter('stack_delete_complete')
    waiter.wait(StackName=stackname)
    t2 = time.time()
    print 'Done. %.2f seconds.' % (t2 -t1)