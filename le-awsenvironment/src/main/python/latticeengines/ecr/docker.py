import argparse
import base64
import boto3
import subprocess

from ..conf import AwsEnvironment

NEXUS_DOCKER_REGISTRY="bodcdevnexus75.dev.lattice.local:18555"
NAMESPACE="latticeengines"
REVISIONS_TO_KEEP=10

def main():
    args = parse_args()
    args.func(args)

def push(args):
    tag_for_remote(args)
    config = AwsEnvironment(args.environment)
    print "pushing image %s:%s to repo ..." % (args.image, args.remotetag)
    login_cmd = login(args.environment)
    reg_url = config.ecr_registry()
    if args.environment == 'dev':
        reg_url = NEXUS_DOCKER_REGISTRY
    destination = reg_url + "/" + NAMESPACE + "/" + args.image + ":" + args.remotetag
    subprocess.call(login_cmd + "; docker push %s" % destination, shell=True)
    subprocess.call("docker rmi " + destination, shell=True)

def pull(args):
    print "pulling image %s:%s from repo to local ..." % (args.image, args.remotetag)
    pull_internal(args.environment, args.image, args.remotetag, args.localtag)

def purge(args):
    if args.environment == 'dev':
        return

    print "purging old tags of image %s from repo ..." % args.image
    purge_internal(args.environment, args.image)

def pull_internal(environment, image, remotetag, localtag):
    registry = AwsEnvironment(environment).ecr_registry()
    if environment == 'dev':
        registry = NEXUS_DOCKER_REGISTRY
    source = registry + "/" + NAMESPACE + "/" + image + ":" + remotetag
    login_cmd = login(environment)
    subprocess.call(login_cmd + "; docker pull %s" % source, shell=True)
    tag_for_local(registry, image, remotetag, localtag)
    subprocess.call("docker rmi " + source, shell=True)

def purge_internal(environment, image):
    config = AwsEnvironment(environment)
    id = config.aws_account_id()

    client = boto3.client('ecr')
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
        if '.' in tag or 'latest' == tag:
            # if is a release tag or latest, do not delete
            continue

        revisions.append(tag)

    revisions.sort(key=lambda x: x['imageTag'])

    for i in xrange(len(revisions) - REVISIONS_TO_KEEP):
        to_delete.append(revisions[i])

    print to_delete

    client.batch_delete_image(
        registryId=id,
        repositoryName=NAMESPACE + '/' + image,
        imageIds=to_delete
    )


def tag_for_remote(args):
    config = AwsEnvironment(args.environment)
    source = NAMESPACE + "/" +  args.image + ":" + args.localtag
    reg_url = config.ecr_registry()
    if args.environment == 'dev':
        reg_url = NEXUS_DOCKER_REGISTRY
    destination = reg_url + "/" + NAMESPACE + "/" + args.image + ":" + args.remotetag
    print "tagging image %s as %s ..." % (source, destination)
    subprocess.call(["docker", "tag", source, destination])

def tag_for_local(registry, image, remotetag, localtag):
    source = registry + "/" + NAMESPACE + "/" + image + ":" + remotetag
    print "tagging image %s as %s:%s ..." % (source, image, localtag)
    destination = "" + NAMESPACE + "/" +  image + ":" + localtag
    subprocess.call(["docker", "tag", source, destination])

def login(environment):
    if environment == 'dev':
        return login_nexus()
    else:
        return login_aws(environment)

def login_nexus():
    username, password, url = 'ysong', 'welcome', 'bodcdevnexus75.dev.lattice.local:18555'
    print "logging in docker registry %s ..." % url
    return 'docker login -u %s -p %s %s' % (username, password, url)

def login_aws(environment):
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
    subparser.add_argument('image', metavar='IMAGE', type=str, help='local docker image name. you can ignore the namespace ' + NAMESPACE)
    subparser.add_argument('-e', dest='environment', type=str, default='dev', choices=['dev', 'qacluster','prodcluster'], help='environment')
    subparser.add_argument('-t', dest='remotetag', type=str, default="latest", help='remote tag (default=latest)')
    subparser.add_argument('--local-tag', dest='localtag', type=str, default="latest", help='local tag (default=latest)')
    subparser.set_defaults(func=push)

    subparser = commands.add_parser("pull")
    subparser.add_argument('image', metavar='IMAGE', type=str, help='local docker image name. you can ignore the namespace ' + NAMESPACE)
    subparser.add_argument('-e', dest='environment', type=str, default='dev', choices=['dev', 'qacluster','prodcluster'], help='environment')
    subparser.add_argument('-t', dest='remotetag', type=str, default="latest", help='remote tag (default=latest)')
    subparser.add_argument('--local-tag', dest='localtag', type=str, default="latest", help='local tag (default=latest)')
    subparser.set_defaults(func=pull)

    subparser = commands.add_parser("purge")
    subparser.add_argument('image', metavar='IMAGE', type=str, help='local docker image name. you can ignore the namespace ' + NAMESPACE)
    subparser.add_argument('-e', dest='environment', type=str, default='dev', choices=['dev', 'qacluster','prodcluster'], help='environment')
    subparser.set_defaults(func=purge)

    args = parser.parse_args()
    return args

if __name__ == '__main__':
    main()
