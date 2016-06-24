import argparse
import os

from .bundle.bundle import SourceBundle
from .bundle.dockerrun import Container, MultiDockerRun
from .bundle.ebextension import ElbListener, VpcSubnet

_S3_CF_PATH='elasticbeanstalk'
_ECR_REPO="894091243412.dkr.ecr.us-east-1.amazonaws.com"

def main():
    args = parse_args()
    args.func(args)

def bundle_cli(args):
    bundle(args.zkhosts, args.sraddr, args.upload)

def bundle(zkhosts, sraddr, upload=False):
    container = Container("kafka-rest", image_repo_path("kafka-rest"), 1024)\
        .add_env("ZK_HOSTS", zkhosts)\
        .add_env("SR_ADDRESS", sraddr)\
        .publish_port(9023, 9023)

    dockerrun = MultiDockerRun().add_container(container)
    listener = ElbListener().listen(80, instance_port=9023)
    vpc_subnet = VpcSubnet('ysong-east', 't2.medium', 'vpc-10b01977', ['sg-b3dbb0c8'], ['subnet-7550002d', 'subnet-310d5a1b'])
    bundle = SourceBundle("kafka-eb").set_dockerrun(dockerrun).add_extension(listener).add_extension(vpc_subnet)

    if upload:
        bundle.archive()
        bundle.upload(_S3_CF_PATH)
    else:
        print dockerrun.json()
        print listener.content()
        print vpc_subnet.content()

def image_repo_path(image_name):
    return os.path.join(_ECR_REPO, "latticeengines", image_name)

def parse_args():
    parser = argparse.ArgumentParser(description='Kafka ECS CloudFormation management')
    commands = parser.add_subparsers(help="commands")

    parser1 = commands.add_parser("bundle")
    parser1.add_argument('-z', dest='zkhosts', type=str, required=True, help='Zookeeper connection string')
    parser1.add_argument('-s', dest='sraddr', type=str, required=True, help='Schema registry address. e.g. http://abcd:9022')
    parser1.add_argument('-u', dest='upload', action='store_true', help='upload to S3')
    parser1.set_defaults(func=bundle_cli)

    args = parser.parse_args()
    return args

if __name__ == '__main__':
    main()