import argparse
import boto3
from botocore.exceptions import ClientError

from ..elb.targetgroup import find_tgrp_arn

AS_CLIENT=None

def main():
    args = parse_args()
    args.func(args)

def register(args):
    register_internal(args.asgroup, args.tgrp)

def deregister(args):
    deregister_internal(args.asgroup, args.tgrp)

def register_internal(asgroup, tgrp):
    group_name = find_full_group_name(asgroup)
    tgrp_arn = find_tgrp_arn(tgrp)

    global AS_CLIENT
    if AS_CLIENT is None:
        AS_CLIENT = boto3.client('autoscaling')
    try:
        response = AS_CLIENT.describe_load_balancer_target_groups(AutoScalingGroupName=group_name)
        tgrp_arns = []
        for t in response['LoadBalancerTargetGroups']:
            if t['LoadBalancerTargetGroupARN'] == tgrp_arn:
                print 'ASGroup %s is already registered to target group %s' % (group_name, tgrp)
                return
            tgrp_arns.append(t['LoadBalancerTargetGroupARN'])
        tgrp_arns.append(tgrp_arn)
    except ClientError as e:
        print "Error: %s" % e
 
    try:
        response = AS_CLIENT.attach_load_balancer_target_groups(
            AutoScalingGroupName=group_name,
            TargetGroupARNs=tgrp_arns
        )
        print response
    except ClientError as e:
        print "Error: %s" % e

def deregister_internal(asgroup, tgrp):
    group_name = find_full_group_name(asgroup)
    tgrp_arn = find_tgrp_arn(tgrp)

    global AS_CLIENT
    if AS_CLIENT is None:
        AS_CLIENT = boto3.client('autoscaling')

    try:
        response = AS_CLIENT.describe_load_balancer_target_groups(AutoScalingGroupName=group_name)
        for t in response['LoadBalancerTargetGroups']:
            if t['LoadBalancerTargetGroupARN'] == tgrp_arn:
                response = AS_CLIENT.detach_load_balancer_target_groups(
                    AutoScalingGroupName=group_name,
                    TargetGroupARNs=[ tgrp_arn ]
                )
            print response
    except ClientError as e:
        print "Error: %s" % e

def find_full_group_name(prefix):
    global AS_CLIENT
    if AS_CLIENT is None:
        AS_CLIENT = boto3.client('autoscaling')

    try:
        response = AS_CLIENT.describe_auto_scaling_groups()
        for group in response["AutoScalingGroups"]:
            grpname = group["AutoScalingGroupName"]
            if len(grpname) >= len(prefix) and grpname[:len(prefix)] == prefix:
                print "Found auto scaling group: " + grpname
                return grpname
        raise Exception("Cannot find auto scaling group with prefix " + prefix)
    except ClientError as e:
        print "Error: %s" % e

def parse_args():
    parser = argparse.ArgumentParser(description='Auto Scaling Group load balancing management')
    commands = parser.add_subparsers(help="commands")

    subparser = commands.add_parser("register", description="Register autoscaling group to a target group")
    subparser.add_argument('-a', dest='asgroup', type=str, required=True, help='autoscaling group name (prefix)')
    subparser.add_argument('-t', dest='tgrp', type=str, required=True, help='target group name')
    subparser.set_defaults(func=register)

    subparser = commands.add_parser("deregister", description="Deregister a target group from autoscaling group")
    subparser.add_argument('-a', dest='asgroup', type=str, required=True, help='autoscaling group name (prefix)')
    subparser.add_argument('-t', dest='tgrp', type=str, required=True, help='target group name')
    subparser.set_defaults(func=deregister)

    args = parser.parse_args()
    return args

if __name__ == '__main__':
    main()