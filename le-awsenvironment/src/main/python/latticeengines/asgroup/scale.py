import argparse
import boto3

AS_CLIENT=None
CW_CLIENT=None

def main():
    args = parse_args()
    args.func(args)

def hook(args):
    hook_internal(args.group)

def hook_internal(group):
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

    subparser = commands.add_parser("hook-alarm", description="Hook a scaling policy with an alarm")
    subparser.add_argument('-g', dest='group', type=str, required=True, help='name of the auto scaling group (prefix).')
    subparser.set_defaults(func=hook)

    args = parser.parse_args()
    return args

if __name__ == '__main__':
    main()