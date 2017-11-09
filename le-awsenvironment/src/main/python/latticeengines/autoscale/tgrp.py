import argparse
import boto3
import json
import logging
import time
from botocore.exceptions import ClientError

from ..common.log import init_logging
from ..elb.targetgroup import find_tgrp_arn, targets_are_healthy, get_targets

AS_CLIENT = None
LOG = logging.getLogger(__name__)

def main():
    args = parse_args()
    args.func(args)

def register(args):
    init_logging()
    register_internal(args.asgroup, args.tgrp)

def deregister(args):
    init_logging()
    deregister_internal(args.asgroup, args.tgrp)

def verify(args):
    init_logging()
    verify_internal(args.asgroup, args.tgrp)

def register_internal(asgroup, tgrp):
    group_name = find_full_group_name(asgroup)
    tgrp_arn = find_tgrp_arn(tgrp)

    client = get_as_client()
    try:
        response = client.describe_load_balancer_target_groups(AutoScalingGroupName=group_name)
        tgrp_arns = []
        for t in response['LoadBalancerTargetGroups']:
            if t['LoadBalancerTargetGroupARN'] == tgrp_arn:
                LOG.info('ASGroup %s is already registered to target group %s' % (group_name, tgrp))
                return
            tgrp_arns.append(t['LoadBalancerTargetGroupARN'])
        tgrp_arns.append(tgrp_arn)
    except ClientError as e:
        print "Error: %s" % e
 
    try:
        response = client.attach_load_balancer_target_groups(
            AutoScalingGroupName=group_name,
            TargetGroupARNs=tgrp_arns
        )
        LOG.info('Registered ' + json.dumps(response))
    except ClientError as e:
        LOG.error("Error: %s" % e)
        raise e

def deregister_internal(asgroup, tgrp):
    group_name = find_full_group_name(asgroup)
    tgrp_arn = find_tgrp_arn(tgrp)

    client = get_as_client()
    try:
        response = client.describe_load_balancer_target_groups(AutoScalingGroupName=group_name)
        for t in response['LoadBalancerTargetGroups']:
            if t['LoadBalancerTargetGroupARN'] == tgrp_arn:
                response = AS_CLIENT.detach_load_balancer_target_groups(
                    AutoScalingGroupName=group_name,
                    TargetGroupARNs=[ tgrp_arn ]
                )
            LOG.info('Deregistered ' + json.dumps(response))
    except ClientError as e:
        LOG.error("Error: %s" % e)
        raise e


def verify_internal(asgroup, tgrp):
    group_name = find_full_group_name(asgroup)
    tgrp_arn = find_tgrp_arn(tgrp)

    as_client = get_as_client()
    try:
        response = as_client.describe_load_balancer_target_groups(AutoScalingGroupName=group_name)
        tgrp = None
        for t in response['LoadBalancerTargetGroups']:
            if t['LoadBalancerTargetGroupARN'] == tgrp_arn:
                tgrp = t
                break
        if tgrp is None:
            raise Exception('Cannot find the target group ' + tgrp_arn)

        LOG.info('Going to check instance health in target group once per 30 second for 15 minutes.')
        all_healthy = False
        t0 = time.time()
        instances = []
        while (not all_healthy) and (time.time() - t0 < 15 * 60):
            # scan instances inside loop in case they change during verification
            response = as_client.describe_auto_scaling_groups(AutoScalingGroupNames=[group_name])
            as_grp = response['AutoScalingGroups'][0]
            instances = [ i['InstanceId'] for i in as_grp['Instances'] ]
            LOG.info('Instances in the auto scaling group: ' + ','.join(instances))
            all_healthy = targets_are_healthy(tgrp_arn, instances)
            if not all_healthy:
                LOG.info('Sleep 30 second.')
                time.sleep(30)

        if not all_healthy:
            raise Exception('Not all instances become healthy with in 15 minutes.')

        LOG.info('Wait for all other instances draining out within 10 minutes')
        t0 = time.time()
        exist_instances = []
        while time.time() - t0 < 10 * 60:
            exist_instances = get_targets(tgrp_arn)
            if len(exist_instances) > len(instances):
                LOG.warn('%d instances [ %s ] in the auto scaling group %s, while %d [ %s ] registered in the target group %s'
                         % ( len(instances), ','.join(instances), group_name, len(exist_instances), ','.join(exist_instances), tgrp ))
                time.sleep(30)
            else:
                break

        if len(exist_instances) > len(instances):
            raise Exception('Not all extra instances drained out with in 10 minutes.')

        LOG.info('Auto scaling group %s in target group %s is healthy.' % (group_name, tgrp))

    except ClientError as e:
        LOG.error("Error: %s" % e)
        raise e

def find_full_group_name(prefix):
    client = get_as_client()
    try:
        response = client.describe_auto_scaling_groups()
        for group in response["AutoScalingGroups"]:
            grpname = group["AutoScalingGroupName"]
            if len(grpname) >= len(prefix) and grpname[:len(prefix)] == prefix:
                LOG.info("Found auto scaling group: " + grpname)
                return grpname
        next_token = response["NextToken"]
        while next_token:
            LOG.info("Fetching next page with NextToken=%s" % next_token)
            response = client.describe_auto_scaling_groups(NextToken=next_token)
            for group in response["AutoScalingGroups"]:
                grpname = group["AutoScalingGroupName"]
                if len(grpname) >= len(prefix) and grpname[:len(prefix)] == prefix:
                    LOG.info("Found auto scaling group: " + grpname)
                    return grpname
            next_token = response["NextToken"] if "NextToken" in response else None
        raise Exception("Cannot find auto scaling group with prefix " + prefix)
    except ClientError as e:
        LOG.error("Error: %s" % e)
        raise e


def get_as_client():
    global AS_CLIENT
    if AS_CLIENT is None:
        AS_CLIENT = boto3.client('autoscaling')
    return AS_CLIENT


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

    subparser = commands.add_parser("verify", description="Verify a single autoscaling group is registered to a target group")
    subparser.add_argument('-a', dest='asgroup', type=str, required=True, help='autoscaling group name (prefix)')
    subparser.add_argument('-t', dest='tgrp', type=str, required=True, help='target group name')
    subparser.set_defaults(func=verify)

    args = parser.parse_args()
    return args

if __name__ == '__main__':
    # from ..common.log import init_logging
    # init_logging()
    # verify_internal('lpi-lpi-a', 'app-lpi')
    main()
