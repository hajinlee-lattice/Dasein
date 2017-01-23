import argparse

from ec2 import register_ec2_to_targetgroup

def main():
    args = parse_args()
    args.func(args)

def register(args):
    register_internal(args.stack, args.tgrp)

def register_internal(stack, tgrp):
    register_ec2_to_targetgroup(stack, tgrp)

def parse_args():
    parser = argparse.ArgumentParser(description='EC2 load balancing management')
    commands = parser.add_subparsers(help="commands")

    subparser = commands.add_parser("register", description="Register ec2 in a cloudformation stack to a target group")
    subparser.add_argument('-s', dest='stack', type=str, required=True, help='cloudformation stack name')
    subparser.add_argument('-t', dest='tgrp', type=str, required=True, help='target group name')
    subparser.set_defaults(func=register)

    args = parser.parse_args()
    return args

if __name__ == '__main__':
    main()