import argparse
import boto3
import sys
import time

ECS_CLIENT=boto3.client('ecs')

def main():
    args = parse_args()
    args.func(args)

def restart_service(args):
    restart_service_internal(args.cluster, args.service)

def restart_service_internal(cluster, service):
    cluster_arn = find_cluster(cluster)
    service_arn = find_service(cluster, service)

    count = count_tasks_in_service(cluster_arn, service_arn)
    update_tasks_count(cluster_arn, service_arn, 0)
    update_tasks_count(cluster_arn, service_arn, count)

def stop_service(args):
    stop_service_internal(args.cluster, args.service)

def stop_service_internal(cluster, service):
    cluster_arn = find_cluster(cluster)
    service_arn = find_service(cluster, service)

    count_tasks_in_service(cluster_arn, service_arn)
    update_tasks_count(cluster_arn, service_arn, 0)

def find_service(cluster, service):
    cluster_arn = find_cluster(cluster)
    response = ECS_CLIENT.list_services(cluster=cluster_arn)
    for s in response['serviceArns']:
        service_name = s.split(':')[5].replace('service/', '')
        if (service_name != service) and (cluster in service_name):
            random_token = service_name.split('-')[-1]
            service_name = service_name.replace(cluster + '-', '').replace('-' + random_token, '')
        if service_name == service:
            print "Found ecs service " + s
            return s

    sys.stdout.flush()

    raise Exception("Cannot find the ecs service named " + service)

def find_service_name(cluster, service):
    cluster_arn = find_cluster(cluster)
    response = ECS_CLIENT.list_services(cluster=cluster_arn)
    for s in response['serviceArns']:
        service_name = s.split(':')[5].replace('service/', '')
        service_original_name = service_name
        if (service_name != service) and (cluster in service_name):
            random_token = service_name.split('-')[-1]
            service_name = service_name.replace(cluster + '-', '').replace('-' + random_token, '')
        if service_name == service:
            print "Found ecs service " + s
            return service_original_name

    sys.stdout.flush()

    raise Exception("Cannot find the ecs service named " + service)

def find_cluster(cluster):
    response = ECS_CLIENT.list_clusters()

    cluster_arn = None
    for c in response['clusterArns']:
        cluster_name = c.split(':')[5].replace('cluster/', '')
        cluster_name = cluster_name.split('-ecscluster-')[0]
        if cluster_name == cluster:
            cluster_arn = c

    if cluster_arn is not None:
        return cluster_arn

    sys.stdout.flush()

    raise Exception("Cannot find the ecs cluster named " + cluster)

def find_cluster_name(cluster):
    response = ECS_CLIENT.list_clusters()

    for c in response['clusterArns']:
        cluster_name = c.split(':')[5].replace('cluster/', '')
        cluster_shortname = cluster_name.split('-ecscluster-')[0]
        if cluster_shortname == cluster:
            return cluster_name

    sys.stdout.flush()

    raise Exception("Cannot find the ecs cluster named " + cluster)

def find_cluster_random_token(cluster):
    response = ECS_CLIENT.list_clusters()

    token = None
    for c in response['clusterArns']:
        full_name = c.split(':')[5].replace('cluster/', '')
        cluster_name = full_name.split('-ecscluster-')[0]
        if cluster_name == cluster:
            token = full_name.split('-ecscluster-')[1]

    if token is not None:
        return token

    sys.stdout.flush()

    raise Exception("Cannot find the ecs cluster named " + cluster)

def count_tasks_in_service(cluster_arn, service_arn):
    response = ECS_CLIENT.describe_services(cluster=cluster_arn, services=[service_arn])
    return response['services'][0]['runningCount']


def update_tasks_count(cluster_arn, service_arn, target_count):
    print "Changing task count of %s in %s to %d" % (service_arn, cluster_arn, target_count)
    sys.stdout.flush()
    ECS_CLIENT.update_service(cluster=cluster_arn, service=service_arn, desiredCount=target_count)
    time.sleep(10)
    count = count_tasks_in_service(cluster_arn, service_arn)
    t1 = time.clock()
    while count != target_count and time.clock() - t1 < 3600:
        time.sleep(5)
        ECS_CLIENT.update_service(cluster=cluster_arn, service=service_arn, desiredCount=target_count)
        count = count_tasks_in_service(cluster_arn, service_arn)
    if count != target_count:
        raise Exception("Failed to wait for the tasks count to be come %d with in one hour" % target_count)
    print "Task count of %s in %s is now %d" % (service_arn, cluster_arn, target_count)

def register_task(name, containers, volumes):
    deregister_task(name)
    print "registering task %s" % name
    ECS_CLIENT.register_task_definition(
        family=name,
        containerDefinitions=[c.template() for c in containers],
        volumes=[v.template() for v in volumes]
    )

def deregister_task(name_prefix):
    print "looking for task %s" % name_prefix
    response = ECS_CLIENT.list_task_definition_families(
        familyPrefix=name_prefix
    )

    for family in response['families']:
        response2 = ECS_CLIENT.list_task_definitions(
            familyPrefix=family
        )
        for arn in response2["taskDefinitionArns"]:
            print "deregistering task %s" % arn
            ECS_CLIENT.deregister_task_definition(
                taskDefinition=arn
            )

def create_service(cluster, service, task, count):
    delete_service(cluster, service)
    print "creating service %s in cluster %s" % (service, cluster)
    cluster_arn = find_cluster(cluster)
    ECS_CLIENT.create_service(
        cluster=cluster_arn,
        serviceName=service,
        taskDefinition=task,
        desiredCount=count
    )


def delete_service(cluster, service):
    print "looking for service %s in cluster %s" % (service, cluster)
    cluster_arn = find_cluster(cluster)
    try:
        service_arn = find_service(cluster, service)
        update_tasks_count(cluster_arn, service_arn, 0)
        print "deleting service " + service_arn
        ECS_CLIENT.delete_service(
            cluster=cluster_arn,
            service=service_arn
        )
    except Exception:
        pass


def parse_args():
    parser = argparse.ArgumentParser(description='ECS cluster management')
    commands = parser.add_subparsers(help="commands")

    subparser = commands.add_parser("restart-service", description="Clean up a log group")
    subparser.add_argument('cluster', metavar='CLUSTER', type=str, help='ecs cluster name')
    subparser.add_argument('service', metavar='SERVICE', type=str, help='ecs service name')
    subparser.set_defaults(func=restart_service)

    subparser = commands.add_parser("stop-service", description="Clean up a log group")
    subparser.add_argument('cluster', metavar='CLUSTER', type=str, help='ecs cluster name')
    subparser.add_argument('service', metavar='SERVICE', type=str, help='ecs service name')
    subparser.set_defaults(func=stop_service)

    args = parser.parse_args()
    return args

if __name__ == '__main__':
    main()