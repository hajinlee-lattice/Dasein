import glob
import os
import sys

PROPERTY_FILE_SUFFIX = "*.properties"
ENV_FILE = "ENV_VARS"
NEW_SUFFIX=".new"

def main():
    dir, profile = sys.argv[1:3]
    print "dir=%s, profile=%s" % (dir, profile)
    replace(dir, profile)

def replace(dir, profile):
    tokens = load_tokens(profile)
    print "\nFound tokens:", tokens

    prop_files = glob.glob(dir + '/' + PROPERTY_FILE_SUFFIX)
    env_files = glob.glob(dir + '/' + ENV_FILE)
    files = prop_files + env_files
    for f in files:
        if os.path.isfile(f + NEW_SUFFIX):
            os.remove(f + NEW_SUFFIX)
        with open(f, 'r') as old_file:
            with open(f + NEW_SUFFIX, 'w') as new_file:
                for line in old_file:
                    if len(line.strip()) > 0 and ('#' != line.strip()[0]):
                        key = line.strip().replace('\n', '').split('=')[0]
                        value = line.strip().replace('\n', '')[len(key) + 1:]
                        new_value = value
                        for k, v in tokens.items():
                            new_value = new_value.replace('${%s}' % k, v).strip()
                        if new_value != value:
                            print 'Set %s=%s' % (key.strip(), new_value)
                        new_file.write('%s=%s\n' % (key.strip(), new_value))
                    else:
                        new_file.write(line)
        os.rename(f + NEW_SUFFIX, f)

def load_tokens(profile):
    m = {}
    with open(profile) as file:
        for line in file:
            if '=' in line and ('#' != line.strip()[0]):
                key = line.strip().replace('\n', '').split('=')[0]
                value = line.strip().replace('\n', '')[len(key) + 1:]
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