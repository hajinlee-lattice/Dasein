import argparse
import boto3

def main():
    args = parse_args()
    args.func(args)

def clean(args):
    clean_internal(args.group)

def clean_internal(group):
    client = boto3.client('logs')
    response = client.describe_log_streams(logGroupName=group)
    streams = response['logStreams']
    for stream in streams:
        client.delete_log_stream(
            logGroupName=group,
            logStreamName=stream['logStreamName']
        )


def create(args):
    create_internal(args.group)

def create_internal(group):
    client = boto3.client('logs')
    response = client.describe_log_groups(
        logGroupNamePrefix=group
    )
    already_exists = False
    for group in response['logGroups']:
        if group['logGroupName'] == group:
            already_exists = True
    if already_exists:
        clean_internal(group)
    else:
        client.create_log_group(logGroupName=group)


def parse_args():
    parser = argparse.ArgumentParser(description='Docker image management')
    commands = parser.add_subparsers(help="commands")

    subparser = commands.add_parser("create", description="Create a log group. If already exists, clean it up.")
    subparser.add_argument('group', metavar='GROUP', type=str, help='log group to be created.')
    subparser.set_defaults(func=create)

    subparser = commands.add_parser("clean", description="Clean up a log group")
    subparser.add_argument('group', metavar='GROUP', type=str, help='log group to be cleaned up.')
    subparser.set_defaults(func=clean)

    args = parser.parse_args()
    return args

if __name__ == '__main__':
    main()