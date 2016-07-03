import argparse
import subprocess

from ..conf import AwsEnvironment

_ECR_REPO="158854640770.dkr.ecr.us-east-1.amazonaws.com"

def main():
    args = parse_args()
    args.func(args)

def push(args):
    tag(args)
    config = AwsEnvironment(args.environment)
    print "pushing image %s:%s to repo ..." % (args.image, args.remotetag)
    destination = config.ecr_registry() + "/" + args.image + ":" + args.remotetag
    subprocess.call(["docker", "push", destination])

def tag(args):
    config = AwsEnvironment(args.environment)
    print "tagging image %s:%s as %s ..." % (args.image, args.localtag, args.remotetag)
    source = "latticeengines/" +  args.image + ":" + args.localtag
    destination = config.ecr_registry() + "/" + args.image + ":" + args.remotetag
    subprocess.call(["docker", "tag", source, destination])


def parse_args():
    parser = argparse.ArgumentParser(description='Docker image management')
    commands = parser.add_subparsers(help="commands")

    subparser = commands.add_parser("push")
    subparser.add_argument('-i', dest='image', type=str, required=True, help='local docker image name. you can ignore the namespace latticeengines')
    subparser.add_argument('-e', dest='environment', type=str, default='dev', help='environment')
    subparser.add_argument('-t', dest='remotetag', type=str, default="latest", help='remote tag')
    subparser.add_argument('--local-tag', dest='localtag', type=str, default="latest", help='local tag')
    subparser.set_defaults(func=push)

    args = parser.parse_args()
    return args

if __name__ == '__main__':
    main()
