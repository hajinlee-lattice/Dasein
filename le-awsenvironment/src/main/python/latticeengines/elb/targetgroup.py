import argparse
import boto3

DUMMY_TGRP="arn:aws:iam::028036828464:target-group/dummy"

def main():
    args = parse_args()
    args.func(args)

def register_cli(args):
    register(args.tgrp, args.instance, args.port)

def register(tgrp, instance, port=443):
    group_arn = find_tgrp_arn(tgrp)

    client = boto3.client('elbv2')
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

    client = boto3.client('elbv2')
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
    client = boto3.client('elbv2')
    response = client.describe_target_groups()
    for tgrp in response['TargetGroups']:
        if tgrp['TargetGroupName'] == name:
            tgrp_arn = tgrp['TargetGroupArn']
            print "Found target group " + tgrp_arn
            return tgrp_arn
    raise Exception("Cannot find target group named "+ name)

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
    main()