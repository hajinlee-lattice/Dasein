import boto3

from ..elb.targetgroup import register, deregister

EC2_CLIENT = None

def get_client():
    global EC2_CLIENT
    if EC2_CLIENT is None:
        EC2_CLIENT = boto3.client('ec2')
    return EC2_CLIENT

def register_ec2_to_targetgroup(stackname, tgrp):
    ids = find_ec2_ids(stackname)
    for ec2_id in ids:
        print "registering ec2 %s to target group %s" % (ec2_id, tgrp)
        register(tgrp, ec2_id)

def deregister_ec2_from_targetgroup(stackname, tgrp):
    ids = find_ec2_ids(stackname)
    for ec2_id in ids:
        print "registering ec2 %s to target group %s" % (ec2_id, tgrp)
        deregister(tgrp, ec2_id)

def find_ec2_ids(stackname):
    stack = boto3.resource('cloudformation').Stack(stackname)
    ips = []

    if stack.outputs is None:
        return ips

    for output in stack.outputs:
        key = output['OutputKey']
        value = output['OutputValue']
        if 'PrivateIp' in key:
            ips.append(value)
            print "Added an EC2 at private ip " + value
    ec2_ids = []
    for ip in ips:
        ec2_ids.append(find_ec2_id_by_ip(ip))
    return ec2_ids


def find_ec2_id_by_ip(private_ip):
    client = get_client()
    response = client.describe_instances(
        DryRun=False,
        Filters=[
            {
                'Name': 'instance-state-name',
                'Values': [
                    'running',
                ]
            },
        ],
        MaxResults=999
    )
    reservations = response["Reservations"]
    for reservation in reservations:
        instances = reservation["Instances"]
        for instance in instances:
            if instance["PrivateIpAddress"] == private_ip:
                return instance["InstanceId"]

    raise Exception("Cannot find instance with private ip " + private_ip)


def find_ec2_id_by_sg(sg):
    client = get_client()
    response = client.describe_instances(
        DryRun=False,
        Filters=[
            {
                'Name': 'group-name',
                'Values': [
                    sg,
                ]
            },
        ],
        MaxResults=999
    )
    ids = []
    reservations = response["Reservations"]
    for reservation in reservations:
        instances = reservation["Instances"]
        for instance in instances:
            ids.append(instance["InstanceId"])

    return ids

def tag_instance(ids, key, value):
    client = get_client()
    client.create_tags(
        Resources=ids,
        Tags=[
            {
                'Key': key,
                'Value': value
            },
        ]
    )

