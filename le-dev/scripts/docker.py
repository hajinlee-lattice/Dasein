from __future__ import print_function

import argparse
import base64
import boto3
import re
import subprocess
import sys

NEXUS_DOCKER_REGISTRY="10.41.1.75:18666"
NAMESPACE="latticeengines"
REVISIONS_TO_KEEP=10
ECR_CLIENT={}


class AwsEnvironment:
    def __init__(self, env="dev"):
        self.env = env

    def aws_account_id(self):
        if self.env == "qa":
            return "028036828464"
        elif self.env == "prod":
            return "158854640770"

    def ecr_registry(self):
        return "%s.dkr.ecr.us-east-1.amazonaws.com" % self.aws_account_id()

def main():
    args = parse_args()
    args.func(args)

def login(args):
    print("logging in the docker repo for %s" % args.environment)
    login_cmd = login_internal(args.environment)
    subprocess.call(login_cmd, shell=True)

def push(args):
    tag_for_remote(args)
    print("pushing image %s:%s to repo ..." % (args.image, args.remotetag))

    if args.environment != "dev":
        create_ecr_if_not_exists(args.environment, args.image)
    print("login")
    login_cmd = login_internal(args.environment)

    if args.environment == 'dev':
        reg_url = NEXUS_DOCKER_REGISTRY
    else:
        config = AwsEnvironment(args.environment)
        reg_url = config.ecr_registry()
    destination = reg_url + "/" + NAMESPACE + "/" + args.image + ":" + args.remotetag
    if args.dryrun:
        print(login_cmd + "; docker push %s" % destination)
        print("docker rmi " + destination)
    else:
        subprocess.call(login_cmd + "; docker push %s" % destination, shell=True)
        subprocess.call("docker rmi " + destination, shell=True)

def pull(args):
    print("pulling image %s:%s from repo to local ..." % (args.image, args.remotetag))
    pull_internal(args.environment, args.image, args.remotetag, args.localtag, withf=args.withf)

def purge(args):
    if args.environment == 'dev':
        return

    print("purging old tags of image %s from repo ..." % args.image)
    purge_internal(args.environment, args.image, args.dryrun)

def pull_internal(environment, image, remotetag, localtag, skiplogin=False, withf=False):
    if environment == 'dev':
        registry = NEXUS_DOCKER_REGISTRY
    else:
        registry = AwsEnvironment(environment).ecr_registry()
    source = registry + "/" + NAMESPACE + "/" + image + ":" + remotetag
    if skiplogin:
        subprocess.call("docker pull %s" % source, shell=True)
    else:
        login_cmd = login_internal(environment)
        subprocess.call(login_cmd + "; docker pull %s" % source, shell=True)
    tag_for_local(registry, image, remotetag, localtag, with_foption=withf)
    subprocess.call("docker rmi " + source, shell=True)

def purge_internal(environment, image, dryrun=False):
    config = AwsEnvironment(environment)
    id = config.aws_account_id()

    client = ecr_client(environment)
    response = client.list_images(
        registryId=id,
        repositoryName=NAMESPACE + '/' + image
    )

    to_delete = []
    revisions = []

    for imageId in response['imageIds']:
        if 'imageTag' not in imageId.keys():
            to_delete.append(imageId)
            continue

        tag = imageId['imageTag']
        if re.match(r'(\d+)$', tag):
            print(tag + ' is a revision tag')
            revisions.append(tag)

    if len(revisions) > 0:
        revisions.sort()
        for i in range(len(revisions) - REVISIONS_TO_KEEP):
            to_delete.append({'imageTag': revisions[i]})

    if len(to_delete) > 0:
        print('deleting images ', to_delete)
        if dryrun:
            print("dryrun")
        else:
            client.batch_delete_image(
                registryId=id,
                repositoryName=NAMESPACE + '/' + image,
                imageIds=to_delete
            )


def tag_for_remote(args):
    source = NAMESPACE + "/" +  args.image + ":" + args.localtag
    if args.environment == 'dev':
        reg_url = NEXUS_DOCKER_REGISTRY
    else:
        config = AwsEnvironment(args.environment)
        reg_url = config.ecr_registry()
    destination = reg_url + "/" + NAMESPACE + "/" + args.image + ":" + args.remotetag
    print("tagging image %s as %s ..." % (source, destination))
    if args.withf:
        subprocess.call(["docker", "tag", "-f", source, destination])
    else:
        subprocess.call(["docker", "tag", source, destination])

def tag_for_local(registry, image, remotetag, localtag, with_foption=False):
    source = registry + "/" + NAMESPACE + "/" + image + ":" + remotetag
    print("tagging image %s as %s:%s ..." % (source, image, localtag))
    destination = "" + NAMESPACE + "/" +  image + ":" + localtag
    if with_foption:
        subprocess.call(["docker", "tag", "-f", source, destination])
    else:
        subprocess.call(["docker", "tag", source, destination])

def create_ecr_if_not_exists(env, image):
    if not repo_in_ecr(env, image):
        client = ecr_client(env)
        full_name = NAMESPACE + "/" +  image
        client.create_repository(
            repositoryName=full_name
        )

def repo_in_ecr(env, image):
    config = AwsEnvironment(env)
    id = config.aws_account_id()
    full_name = NAMESPACE + "/" +  image
    client = ecr_client(env)
    response = client.describe_repositories(registryId=id)
    for repo in response['repositories']:
        if repo['repositoryName'] == full_name:
            return True
    return False

def login_internal(environment):
    if environment == 'dev':
        return login_nexus()
    else:
        return login_aws(environment)

def login_nexus():
    username, password, url = 'deploy', 'welcome', NEXUS_DOCKER_REGISTRY
    print("logging in docker registry %s ..." % url)
    return 'docker login -u %s -p %s %s' % (username, password, url)

def login_aws(environment):
    config = AwsEnvironment(environment)
    account_id = config.aws_account_id()
    print("logging in docker registry for account %s ..." % account_id)
    client = ecr_client(environment)
    res = client.get_authorization_token(registryIds=[account_id])
    data = res['authorizationData'][0]
    token = base64.b64decode(data['authorizationToken'])
    if sys.version_info >= (3,0):
        token = token.decode("utf-8")
    username, password = token.split(':')
    return 'docker login -u %s -p %s %s' % (username, password, data['proxyEndpoint'])

def ecr_client(environment):
    global ECR_CLIENT
    if environment not in ECR_CLIENT:
        ECR_CLIENT[environment] = boto3.client('ecr', region_name="us-east-1")
    return ECR_CLIENT[environment]

def parse_args():
    parser = argparse.ArgumentParser(description='Docker image management')
    commands = parser.add_subparsers(help="commands")

    subparser = commands.add_parser("login")
    subparser.add_argument('-e', dest='environment', type=str, default='dev', choices=['dev', 'qa','prod'], help='environment')
    subparser.set_defaults(func=login)

    subparser = commands.add_parser("push")
    subparser.add_argument('image', metavar='IMAGE', type=str, help='local docker image name. you can ignore the namespace ' + NAMESPACE)
    subparser.add_argument('-e', dest='environment', type=str, default='dev', choices=['dev', 'qa','prod'], help='environment')
    subparser.add_argument('-t', dest='remotetag', type=str, default="latest", help='remote tag (default=latest)')
    subparser.add_argument('--local-tag', dest='localtag', type=str, default="latest", help='local tag (default=latest)')
    subparser.add_argument('-f', dest='withf', action="store_true", help='with -f option when tagging')
    subparser.add_argument('--dryrun', dest='dryrun', action="store_true", help='Perform a dry run')
    subparser.set_defaults(func=push)

    subparser = commands.add_parser("pull")
    subparser.add_argument('image', metavar='IMAGE', type=str, help='local docker image name. you can ignore the namespace ' + NAMESPACE)
    subparser.add_argument('-e', dest='environment', type=str, default='dev', choices=['dev', 'qa','prod'], help='environment')
    subparser.add_argument('-t', dest='remotetag', type=str, default="latest", help='remote tag (default=latest)')
    subparser.add_argument('--local-tag', dest='localtag', type=str, default="latest", help='local tag (default=latest)')
    subparser.add_argument('--skip-login', dest='skiplogin', action="store_true", help='skip docker login')
    subparser.add_argument('-f', dest='withf', action="store_true", help='with -f option when tagging')
    subparser.add_argument('--dryrun', dest='dryrun', action="store_true", help='NOOP')
    subparser.set_defaults(func=pull)

    subparser = commands.add_parser("purge")
    subparser.add_argument('image', metavar='IMAGE', type=str, help='local docker image name. you can ignore the namespace ' + NAMESPACE)
    subparser.add_argument('-e', dest='environment', type=str, default='dev', choices=['dev', 'qa','prod'], help='environment')
    subparser.add_argument('--dryrun', dest='dryrun', action="store_true", help='Perform dry run')
    subparser.set_defaults(func=purge)

    args = parser.parse_args()
    return args

if __name__ == '__main__':
    main()
