import argparse
import os
import sys

project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(project_root)

from db.manage import amattrs_x

def main():
    args = parse_args()
    args.func(args)

def parse_args():
    parser = argparse.ArgumentParser(description='Lattice Data Cloud Cli', prog='datacloud')
    commands = parser.add_subparsers(help="pick your sub-command. put sub-command before -h will give your more info.")

    # add parsers
    amattrs_x.add_parser(commands)

    return parser.parse_args()

if __name__ == '__main__':


    main()