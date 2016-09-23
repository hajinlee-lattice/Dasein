import argparse
import boto3
import sys
import time


def main():
    args = parse_args()
    args.func(args)

def restart_service(args):
    restart_service_internal(args.cluster, args.service)

def restart_service_internal(cluster, service):
    client = boto3.client('ecs')
    cluster_arn, service_arn = find_service(client, cluster, service)

    count = count_tasks_in_service(client, cluster_arn, service_arn)
    update_tasks_count(client, cluster_arn, service_arn, 0)
    update_tasks_count(client, cluster_arn, service_arn, count)

def find_service(client, cluster, service):
    response = client.list_clusters()

    cluster_arn = None
    for c in response['clusterArns']:
        cluster_name = c.split(':')[5].replace('cluster/', '')
        cluster_name = cluster_name.split('-ecscluster-')[0]
        if cluster_name == cluster:
            print "Found ecs cluster " + c
            cluster_arn = c

    if cluster_arn is None:
        raise Exception("Cannot find the ecs cluster named " + cluster)

    response = client.list_services(cluster=cluster_arn)

    for s in response['serviceArns']:
        service_name = s.split(':')[5].replace('service/', '')
        random_token = service_name.split('-')[-1]
        service_name = service_name.replace(cluster + '-', '').replace('-' + random_token, '')
        if service_name == service:
            print "Found ecs service " + s
            return cluster_arn, s

    sys.stdout.flush()

    raise Exception("Cannot find the ecs service named " + service)


def count_tasks_in_service(client, cluster, service):
    response = client.describe_services(cluster=cluster, services=[service])
    return response['services'][0]['runningCount']


def update_tasks_count(client, cluster, service, target_count):
    print "Changing task count to %d" % target_count
    sys.stdout.flush()
    client.update_service(cluster=cluster, service=service, desiredCount=target_count)
    count = count_tasks_in_service(client, cluster, service)
    t1 = time.clock()
    while count != target_count and time.clock() - t1 < 3600:
        time.sleep(10)
        count = count_tasks_in_service(client, cluster, service)
    if count != target_count:
        raise Exception("Failed to wait for the tasks count to be come %d with in one hour" % target_count)

def parse_args():
    parser = argparse.ArgumentParser(description='ECS cluster management')
    commands = parser.add_subparsers(help="commands")

    subparser = commands.add_parser("restart-service", description="Clean up a log group")
    subparser.add_argument('cluster', metavar='CLUSTER', type=str, help='ecs cluster name')
    subparser.add_argument('service', metavar='SERVICE', type=str, help='ecs service name')
    subparser.set_defaults(func=restart_service)

    args = parser.parse_args()
    return args

if __name__ == '__main__':
    main()