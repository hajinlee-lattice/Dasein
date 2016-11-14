import argparse
import boto3

def main():
    args = parse_args()
    args.func(args)

def clean(args):
    clean_internal(args.group, args.prefix)

def clean_internal(group, prefix=None):
    client = boto3.client('logs')
    if prefix is None:
        response = client.describe_log_streams(logGroupName=group)
    else:
        response = client.describe_log_streams(logGroupName=group, logStreamNamePrefix=prefix)
    streams = response['logStreams']
    for stream in streams:
        client.delete_log_stream(
            logGroupName=group,
            logStreamName=stream['logStreamName']
        )

def create(args):
    create_internal(args.group)

def create_internal(grp_name):
    client = boto3.client('logs')
    response = client.describe_log_groups(
        logGroupNamePrefix=grp_name
    )
    already_exists = False
    for group in response['logGroups']:
        if group['logGroupName'] == grp_name:
            already_exists = True
    if already_exists:
        clean_internal(grp_name)
    else:
        client.create_log_group(logGroupName=grp_name)


def parse_args():
    parser = argparse.ArgumentParser(description='Docker image management')
    commands = parser.add_subparsers(help="commands")

    subparser = commands.add_parser("create", description="Create a log group. If already exists, clean it up.")
    subparser.add_argument('-g', dest='group', type=str,required=True, help='log group to be created.')
    subparser.set_defaults(func=create)

    subparser = commands.add_parser("clean", description="Clean up a log group")
    subparser.add_argument('-g', dest='group', type=str,required=True, help='log group to be cleaned up.')
    subparser.add_argument('-p', dest='prefix', type=str, help='log stream prefix to be deleted.')
    subparser.set_defaults(func=clean)

    args = parser.parse_args()
    return args

if __name__ == '__main__':
    main()