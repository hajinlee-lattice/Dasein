from __future__ import print_function

import argparse
import boto3
import datetime
import os
import subprocess
from shutil import rmtree

EMR_CLIENT = None
S3_CLIENT = None


def download_log(env, app_id, cluster_id, dest):
    print("Downloading the log for application %s on cluster %s to %s" % (app_id, cluster_id, dest))
    if os.path.isdir(dest):
        print('cleaning up destination directory %s ' % dest)
        rmtree(dest)
    my_env = os.environ
    bucket = "latticeengines-dev-emr"
    if env == "prod":
        bucket = "latticeengines-prod-emr"
        my_env['AWS_PROFILE'] = 'prod'
    src = 's3://%s/%s/containers/%s' % (bucket, cluster_id, app_id)
    commands = ['aws', 's3', 'sync',  src, dest]
    subprocess.run(commands, env=my_env)


def find_cluster(env, app_id):
    print("Searching for logs of %s in %s" % (app_id, env))
    clusters = get_clusters(True)
    print("Search %d active clusters first" % (len(clusters)))
    for cluster_id in clusters:
        if app_in_cluster(env, cluster_id, app_id):
            print("Found app-id %s in cluster %s" % (app_id, cluster_id))
            return cluster_id

    clusters = get_clusters(False)
    print("Search %d active clusters first" % (len(clusters)))
    for cluster_id in clusters:
        if app_in_cluster(env, cluster_id, app_id):
            print("Found app-id %s in cluster %s" % (app_id, cluster_id))
            return cluster_id


def app_in_cluster(env, cluster_id, app_id):
    s3 = s3_client()
    bucket = "latticeengines-dev-emr"
    if env == "prod":
        bucket = "latticeengines-prod-emr"
    resp = s3.list_objects_v2(
        Bucket=bucket,
        Prefix='%s/containers/%s' % (cluster_id, app_id)
    )
    return resp['KeyCount'] > 0


def get_clusters(active):
    emr = emr_client()
    if active:
        resp = emr.list_clusters(
            ClusterStates=['RUNNING', 'WAITING']
        )
    else:
        today = datetime.date.today()
        delta = datetime.timedelta(days=30)
        resp = emr.list_clusters(
            ClusterStates=['TERMINATING', 'TERMINATED', 'TERMINATED_WITH_ERRORS'],
            CreatedAfter=datetime.datetime.combine(today - delta, datetime.datetime.min.time())
        )
    clusters = resp['Clusters']
    return [c['Id'] for c in clusters]

def emr_client():
    global EMR_CLIENT
    if EMR_CLIENT is None:
        EMR_CLIENT = boto3.client('emr')
    return EMR_CLIENT

def s3_client():
    global S3_CLIENT
    if S3_CLIENT is None:
        S3_CLIENT = boto3.client('s3')
    return S3_CLIENT


def parseCliArgs():
    parser = argparse.ArgumentParser(description='Download emr logs from S3.')
    parser.add_argument('app-id', type=str, help='application id')
    parser.add_argument('-d', dest='dest', type=str, required=False, help='destination dir. if empty, will use ~/Downloads/{application_id}')
    parser.add_argument('-e', dest='env', type=str, default='qa', choices=['qa', 'prod'], help='qa or prod')
    parser.add_argument('--cluster-id', dest='cluster_id', type=str, default=None, help='directly specify emr cluster id.')
    args = parser.parse_args()
    return args

if __name__ == "__main__":
    args = parseCliArgs()

    if args.env == "prod":
        boto3.setup_default_session(profile_name='prod')
    else:
        boto3.setup_default_session(profile_name='default')

    app_id = args.__dict__['app-id']
    if args.dest is None:
        dest = os.path.join('~', 'Downloads', app_id)
    else:
        dest = args.dest

    if args.cluster_id is None:
        cluster_id = find_cluster(args.env, app_id)
    else:
        cluster_id = args.cluster_id

    download_log(args.env, app_id, cluster_id, dest)
