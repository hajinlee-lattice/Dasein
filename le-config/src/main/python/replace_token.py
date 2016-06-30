import argparse
import glob
import os

PROPERTY_FILE_SUFFIX = "*.properties"
NEW_SUFFIX=".new"

def main():
    args = parse_args()
    replace(args.dir, args.profile)

def replace(dir, profile):
    tokens = load_tokens(profile)
    print "Found tokens:", tokens

    files = glob.glob(dir + '/' + PROPERTY_FILE_SUFFIX)
    for f in files:
        if os.path.isfile(f + NEW_SUFFIX):
            os.remove(f + NEW_SUFFIX)
        with open(f, 'r') as old_file:
            with open(f + NEW_SUFFIX, 'w') as new_file:
                for line in old_file:
                    if len(line.strip()) > 0 and ('#' != line.strip()[0]):
                        key, value = line.strip().replace('\n', '').split('=')[:2]
                        for k, v in tokens.items():
                            value = value.replace('${%s}' % k, v).replace('\n', '').strip()
                        print 'Setting %s to %s' % (key.strip(), value)
                        new_file.write('%s=%s\n' % (key.strip(), value))
                    else:
                        new_file.write(line)
        os.rename(f + NEW_SUFFIX, f)

def load_tokens(profile):
    m = {}
    with open(profile) as file:
        for line in file:
            if '=' in line and ('#' != line.strip()[0]):
                key, value = line.strip().split('=')[:2]
                m[key.strip()] = value.strip().replace('\n', '')
    return m

def parse_args():
    parser = argparse.ArgumentParser(description='Replace tokens in properties')
    parser.add_argument('-d', dest='dir', type=str, default='.',
                        help='directory containing properties files')
    parser.add_argument('-p', dest='profile', type=str,
                        help='the stack profile file to be used to replace tokens')
    args = parser.parse_args()
    return args

if __name__ == '__main__':
    main()