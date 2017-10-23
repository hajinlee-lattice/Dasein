import argparse
import boto3
import logging

DUMMY_TGRP="arn:aws:iam::028036828464:target-group/dummy"
ELB_CLIENT=None
LOG = logging.getLogger(__name__)

def main():
    args = parse_args()
    args.func(args)

def register_cli(args):
    register(args.tgrp, args.instance, args.port)

def register(tgrp, instance, port=443):
    group_arn = find_tgrp_arn(tgrp)

    client = get_client()
    client.register_targets(
        TargetGroupArn=group_arn,
        Targets=[
            {
                'Id': instance,
                'Port': port
            },
        ]
    )

def deregister(tgrp, instance, port=443):
    group_arn = find_tgrp_arn(tgrp)

    client = get_client()
    client.deregister_targets(
        TargetGroupArn=group_arn,
        Targets=[
            {
                'Id': instance,
                'Port': port
            },
        ]
    )

def find_tgrp_arn(name):
    client = get_client()
    response = client.describe_target_groups()
    for tgrp in response['TargetGroups']:
        if tgrp['TargetGroupName'] == name:
            tgrp_arn = tgrp['TargetGroupArn']
            LOG.info("Found target group " + tgrp_arn)
            return tgrp_arn
    raise Exception("Cannot find target group named "+ name)

def targets_are_healthy(tgrp_arn, instance_ids):
    client = get_client()
    response = client.describe_target_health(
        TargetGroupArn=tgrp_arn,
        Targets= [
            {'Id': iid} for iid in instance_ids
        ])
    all_healthy = True
    for desc in response["TargetHealthDescriptions"]:
        iid = desc['Target']['Id']
        state = desc['TargetHealth']['State']
        LOG.info('The state of instance %s is %s' % (iid, state))
        all_healthy = all_healthy and (state == 'healthy')
    return all_healthy

def get_client():
    global ELB_CLIENT
    if ELB_CLIENT is None:
        ELB_CLIENT = boto3.client('elbv2')
    return ELB_CLIENT

def parse_args():
    parser = argparse.ArgumentParser(description='Docker image management')
    commands = parser.add_subparsers(help="commands")

    subparser = commands.add_parser("register", description="Register an instance to a target group")
    subparser.add_argument('-i', dest='instance', type=str, help='ec2 instance id')
    subparser.add_argument('-g', dest='tgrp', type=str, required=True, help='name of the target group for load balancer')
    subparser.add_argument('-p', dest='port', type=int, default=443, help='port')
    subparser.set_defaults(func=register_cli)

    args = parser.parse_args()
    return args

if __name__ == '__main__':
    from ..common.log import init_logging

    init_logging()
    tgrp_arn = 'arn:aws:elasticloadbalancing:us-east-1:028036828464:targetgroup/app-lpi/139acd55ecbd292e'
    instances = [ 'i-05233af0d126e5efd', 'i-0ad3def5bc2977092' ]
    targets_are_healthy(tgrp_arn, instances)