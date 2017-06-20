import argparse
import logging

from ..ec2.ec2 import find_ec2_id_by_sg, tag_instance

logger = logging.getLogger(__name__)

def main():
    args = parse_args()
    args.func(args)

def tag(args):
    tag_internal(args.sg, args.key, args.value)

def tag_internal(sg, key, value):
    logger.info('Adding tag (%s=%s) to ec2 instances in security group %s' % (key, value, sg))
    ids = find_ec2_id_by_sg(sg)
    if ids is None or len(ids) == 0:
        logger.info("There is no instances to tag.")
    else:
        for id in ids:
            logger.info("Found instance to tag: %s" % id)
        tag_instance(ids, key, value)


def parse_args():
    parser = argparse.ArgumentParser(description='EC2 tagging tool')
    commands = parser.add_subparsers(help="commands")

    subparser = commands.add_parser("tag", description="Tag EC2, by security group")
    subparser.add_argument('-s', dest='sg', type=str, required=True, help='security group')
    subparser.add_argument('-k', dest='key', type=str, required=True, help='key')
    subparser.add_argument('-v', dest='value', type=str, required=True, help='value')
    subparser.set_defaults(func=tag)

    args = parser.parse_args()
    return args

if __name__ == '__main__':
    from ..common.log import init_logging
    init_logging()

    main()
