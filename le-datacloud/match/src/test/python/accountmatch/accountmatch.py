from __future__ import print_function

import argparse
import random
import os
import os.path

import lib.exporters as exporters

TEST_FILE_DIR = './accountmatchtests'
OUTPUT_DIR = './output'
OUTPUT_FORMAT_CSV = 'csv'
OUTPUT_FORMAT_AVRO = 'avro'
DEFAULT_SEED = 1337

current_directory = os.path.dirname(os.path.realpath(__file__))

def parse_args():
    """parse command line arguments

    Returns:
        [dict(str, any)] -- dict of command line argument and values, which include
                            1. test_file (str)
                            2. output_format (str)
                            3. seed (int)
                            4. verbose (boolean)
                            5. output_file (str)
    """
    parser = argparse.ArgumentParser(
        description='Generate test data for account match')
    parser.add_argument(
        'output_file', help='Output file name, output file will be generated in directory {}'.format(OUTPUT_DIR))
    parser.add_argument('-t', '--test-file', dest='test_file', default=None,
                        help='Name of the file that define test groups. Should be under directory {}'.format(TEST_FILE_DIR))
    parser.add_argument('-f', '--format', dest='output_format', default=OUTPUT_FORMAT_AVRO,
                        choices=[OUTPUT_FORMAT_AVRO, OUTPUT_FORMAT_CSV], help='Output format')
    parser.add_argument('-s', '--seed', dest='seed', default=DEFAULT_SEED, help='Seed for random module')
    parser.add_argument('-v', dest='verbose', action="store_true",
                        help='Log more detail about the generated test data')
    return parser.parse_args()


def check(args):
    # make sure required arguments are provided
    if args.verbose:
        print('Command line arguments: {}'.format(args))
    if args.test_file is None:
        raise Exception(
            'Please specify name of the file that defines test groups')


def generate_test_data(test_file, output_file, output_format, seed, verbose):
    # generate test data and output to file. two files will be generated, one
    # for data that should exist before testing, the other for test data.

    # ensure output directory exists
    output_dir = os.path.join(current_directory, 'output')
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)

    # set the seed to get deterministic test data
    random.seed(seed)
    test_groups = get_test_groups(test_file)
    # required when we shuffle/append in batch
    random.shuffle(test_groups)

    if verbose:
        print('Generate test data from {} using seed={}. There are {} test groups'.format(test_file, seed, len(test_groups)))

    pre_test_file = '{}_existing.{}'.format(output_file, output_format)
    pre_test_file_path = os.path.join(output_dir, pre_test_file)
    test_file = '{}_test.{}'.format(output_file, output_format)
    test_file_path = os.path.join(output_dir, test_file)
    pre_test_exporter = get_exporter(pre_test_file_path, output_format)
    test_exporter = get_exporter(test_file_path, output_format)

    # TODO shuffle & append the test data in batch
    curr_pre_test, curr_test = [], []
    with pre_test_exporter, test_exporter:
        for group in test_groups:
            curr_pre_test.extend(group.get_pre_test_data())
            curr_test.extend(group.get_test_data())

        random.shuffle(curr_pre_test)
        random.shuffle(curr_test)
        pre_test_exporter.append(curr_pre_test)
        test_exporter.append(curr_test)

    print('==================================')
    print('Output Files:')
    print('Existing Data ({} rows): {}'.format(len(curr_pre_test), pre_test_file_path))
    print('Test Data ({} rows): {}'.format(len(curr_test), test_file_path))
    print('==================================')


def get_exporter(file_path, output_format):
    if output_format == OUTPUT_FORMAT_AVRO:
        return exporters.AvroExporter(file_path)
    elif output_format == OUTPUT_FORMAT_CSV:
        return exporters.CsvExporter(file_path)
    else:
        raise Exception('Invalid output format = {}'.format(output_format))

def get_test_groups(test_file):
    # import the specified test file and retrieve test groups
    mod = __import__('accountmatchtests.{}'.format(test_file), fromlist=[''])
    return mod.get_test_groups()


def main():
    # entry point
    args = parse_args()
    check(args)
    generate_test_data(args.test_file, args.output_file, args.output_format, args.seed, args.verbose)


if __name__ == '__main__':
    main()
