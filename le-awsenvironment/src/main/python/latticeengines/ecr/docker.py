import argparse
import base64
import boto3
import subprocess

from ..conf import AwsEnvironment


def main():
    args = parse_args()
    args.func(args)

def push(args):
    tag(args)
    config = AwsEnvironment(args.environment)
    print "pushing image %s:%s to repo ..." % (args.image, args.remotetag)
    login_cmd = login(args.environment)
    destination = config.ecr_registry() + "/" + args.image + ":" + args.remotetag
    subprocess.call(login_cmd + "; docker push %s" % destination, shell=True)

def tag(args):
    config = AwsEnvironment(args.environment)
    print "tagging image %s:%s as %s ..." % (args.image, args.localtag, args.remotetag)
    source = "latticeengines/" +  args.image + ":" + args.localtag
    destination = config.ecr_registry() + "/" + args.image + ":" + args.remotetag
    subprocess.call(["docker", "tag", source, destination])

def login(environment):
    config = AwsEnvironment(environment)
    account_id = config.aws_account_id()
    print "logging in docker registry for account %s ..." % account_id
    client = boto3.client('ecr')
    res = client.get_authorization_token(registryIds=[account_id])
    data = res['authorizationData'][0]
    username, password = base64.b64decode(data['authorizationToken']).split(':')
    return 'docker login -u %s -p %s -e none %s' % (username, password, data['proxyEndpoint'])

def parse_args():
    parser = argparse.ArgumentParser(description='Docker image management')
    commands = parser.add_subparsers(help="commands")

    subparser = commands.add_parser("push")
    subparser.add_argument('-i', dest='image', type=str, required=True, help='local docker image name. you can ignore the namespace latticeengines')
    subparser.add_argument('-e', dest='environment', type=str, default='qa', help='environment')
    subparser.add_argument('-t', dest='remotetag', type=str, default="latest", help='remote tag')
    subparser.add_argument('--local-tag', dest='localtag', type=str, default="latest", help='local tag')
    subparser.set_defaults(func=push)

    args = parser.parse_args()
    return args

if __name__ == '__main__':
    main()
