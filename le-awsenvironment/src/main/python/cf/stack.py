import json
import os
import time

from .ec2 import _ec2_mappings, _ec2_params, _ec2_security_group, SECURITY_GROUP, EC2Instance
from .template import Template, TEMPLATE_DIR


class Stack(Template):
    def __init__(self, description):
        Template.__init__(self)
        self._template["AWSTemplateFormatVersion"] = "2010-09-09"
        self._template["Description"] = description
        self._template["Parameters"] = Stack.__common_params()

    def add_ec2(self, instance, create_sg=False):
        assert isinstance(instance, EC2Instance)

        self.__ec2_params()
        self.__ec2_mappings()

        name = instance.get_name()

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

    def associate_eip(self, ec2_name, eip_name):
        eip = {
            "Type" : "AWS::EC2::EIP",
            "Properties" : {
                "InstanceId" : ec2_name,
            }
        }
        self._merge_into_attr('Resources', eip)
        ip_ass = {
            "Type" : "AWS::EC2::EIPAssociation",
            "Properties" : {
                "InstanceId" : { "Ref" : ec2_name },
                "EIP" : { "Ref" : eip_name }
            }
        }
        self._merge_into_attr('Resources', ip_ass)

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