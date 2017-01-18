import argparse
import boto3

from ..ecs.manage import find_cluster_name, find_service_name

AS_CLIENT=None
AAS_CLIENT=None
CW_CLIENT=None

def main():
    args = parse_args()
    args.func(args)

def hookgroup(args):
    hookgroup_internal(args.group)

def hookgroup_internal(group):
    group_name = find_full_group_name(group)
    policies = get_all_policies(group_name)
    alarms = get_alarms(group)
    for policy in policies:
        policy_name = policy['PolicyName']
        if 'ScaleUp' in policy_name:
            alarm = alarms[group + '-high-latency']
            put_alarm_actions(alarm, policy['PolicyARN'])
            print "hook up policy %s with alarm %s" % (policy_name, alarm["AlarmName"])
        elif 'ScaleBack' in policy_name:
            alarm = alarms[group + '-low-latency']
            put_alarm_actions(alarm, policy['PolicyARN'])
            print "hook up policy %s with alarm %s" % (policy_name, alarm["AlarmName"])

def hookecs(args):
    hookecs_internal(args.cluster, args.service)

def hookecs_internal(cluster, service):
    cluster_name = find_cluster_name(cluster)
    print cluster_name
    service_name = find_service_name(cluster, service)
    print service_name
    policies = get_all_ecs_policies(cluster_name, service_name)
    print policies

    alarms = get_alarms(cluster)
    for policy in policies:
        policy_name = policy['PolicyName']
        if 'ScaleUp' in policy_name:
            alarm = alarms[cluster + '-high-latency']
            put_alarm_actions(alarm, policy['PolicyARN'])
            print "hook up policy %s with alarm %s" % (policy_name, alarm["AlarmName"])
        elif 'ScaleBack' in policy_name:
            alarm = alarms[cluster + '-low-latency']
            put_alarm_actions(alarm, policy['PolicyARN'])
            print "hook up policy %s with alarm %s" % (policy_name, alarm["AlarmName"])

def find_full_group_name(prefix):
    global AS_CLIENT
    if AS_CLIENT is None:
        AS_CLIENT = boto3.client('autoscaling')
    response = AS_CLIENT.describe_auto_scaling_groups()
    for group in response["AutoScalingGroups"]:
        grpname = group["AutoScalingGroupName"]
        if len(grpname) >= len(prefix) and grpname[:len(prefix)] == prefix:
            print "Found auto scaling group: " + grpname
            return grpname
    raise Exception("Cannot find auto scaling group with prefix " + prefix)

def get_all_policies(group):
    global AS_CLIENT
    if AS_CLIENT is None:
        AS_CLIENT = boto3.client('autoscaling')
    response = AS_CLIENT.describe_policies(AutoScalingGroupName=group)
    return response["ScalingPolicies"]

def get_all_ecs_policies(cluster, service):
    global AAS_CLIENT
    if AAS_CLIENT is None:
        AAS_CLIENT = boto3.client('application-autoscaling')
    response = AAS_CLIENT.describe_scaling_policies(
        ServiceNamespace='ecs',
        ResourceId='service/%s/%s' % (cluster, service),
        ScalableDimension='ecs:service:DesiredCount',
    )
    return response["ScalingPolicies"]

def put_alarm_actions(alarm, policy_arn):
    global CW_CLIENT
    if CW_CLIENT is None:
        CW_CLIENT = boto3.client('cloudwatch')
    CW_CLIENT.put_metric_alarm(
        AlarmName=alarm["AlarmName"],
        ActionsEnabled=True,
        AlarmActions=[policy_arn],
        MetricName=alarm["MetricName"],
        Namespace=alarm["Namespace"],
        Statistic=alarm["Statistic"],
        Period=alarm["Period"],
        EvaluationPeriods=alarm["EvaluationPeriods"],
        Threshold=alarm["Threshold"],
        ComparisonOperator=alarm["ComparisonOperator"]
    )

def get_alarms(group):
    global CW_CLIENT
    if CW_CLIENT is None:
        CW_CLIENT = boto3.client('cloudwatch')
    response = CW_CLIENT.describe_alarms(AlarmNamePrefix=group)
    alarms = {}
    for alarm in response["MetricAlarms"]:
        alarms[alarm["AlarmName"]] = alarm
    return alarms


def parse_args():
    parser = argparse.ArgumentParser(description='Auto Scaling management')
    commands = parser.add_subparsers(help="commands")

    subparser = commands.add_parser("hook-asgroup", description="Hook a scaling policy with an alarm")
    subparser.add_argument('-g', dest='group', type=str, required=True, help='name of the auto scaling group (prefix).')
    subparser.set_defaults(func=hookgroup)

    subparser = commands.add_parser("hook-ecs", description="Hook a scaling policy with an alarm")
    subparser.add_argument('-c', dest='cluster', type=str, required=True, help='name of ecs cluster. (prefix)')
    subparser.add_argument('-s', dest='service', type=str, required=True, help='name of ecs service. (prefix)')
    subparser.set_defaults(func=hookecs)

    args = parser.parse_args()
    return args

if __name__ == '__main__':
    main()