import glob
import os

PROPERTY_DIR = "/conf/env/"
PROPERTY_FILE_SUFFIX = "*.properties"
LINE_SEPERATOR = "\n=============================\n"
ENVIRONMENTS=('dev', 'devcluster', 'qacluster','prodcluster')

WSHOME=os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))))

def main():
    print "Scanning WSHOME " + WSHOME

    for environment in ENVIRONMENTS:
        try:
            dir = confdir(environment)
            os.makedirs(dir)
        except OSError:
            pass

    for environment in ENVIRONMENTS:
        aggregated=""
        for dir_name, _, _ in os.walk(WSHOME):
            end_with = PROPERTY_DIR + environment
            if environment in ('qacluster', 'prodcluster'):
                end_with += 'a'
            if dir_name[-len(end_with):] == end_with and 'le-config' not in dir_name:
                aggregated += aggregate_props(dir_name)

        target_path=os.path.join(confdir(environment), 'latticeengines.properties')
        if os.path.isfile(target_path):
            os.remove(target_path)
        with open(target_path, 'w') as f:
            print 'Writing to ' + target_path
            f.write(aggregated)

def aggregate_props(dir):
    prop_files = glob.glob(dir + '/' + PROPERTY_FILE_SUFFIX)
    aggregated = "# ========================================\n# " + dir.replace(WSHOME + "/", "") \
                 + "\n# ========================================\n"
    for prop_file in prop_files:
        if 'log4j' not in prop_file:
            with open(prop_file, 'r') as f:
                aggregated += f.read()
    return aggregated + "\n"

def confdir(environment):
    return os.path.join(WSHOME, 'le-config', 'conf', 'env', environment)

if __name__ == '__main__':
    main()