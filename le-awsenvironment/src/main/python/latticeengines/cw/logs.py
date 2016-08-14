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

def parse_args():
    parser = argparse.ArgumentParser(description='Docker image management')
    commands = parser.add_subparsers(help="commands")

    subparser = commands.add_parser("clean", description="Clean up a log group")
    subparser.add_argument('group', metavar='GROUP', type=str, help='log group to be cleaned up.')
    subparser.set_defaults(func=clean)

    args = parser.parse_args()
    return args

if __name__ == '__main__':
    main()