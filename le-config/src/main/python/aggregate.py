import glob
import os

PROPERTY_DIR = "/conf/env/"
PROPERTY_FILE_SUFFIX = "*.properties"
LINE_SEPERATOR = "\n=============================\n"
ENVIRONMENTS=('dev', 'devcluster', 'qacluster','prodcluster')
ENV_WITH_AWS=('qacluster','prodcluster')

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
        keys={}
        for dir_name, _, _ in os.walk(WSHOME):
            end_with = PROPERTY_DIR + environment
            if dir_name[-len(end_with):] == end_with \
                    and 'le-config' not in dir_name \
                    and 'le-docker' not in dir_name \
                    and 'le-awsenvironment' not in dir_name:
                aggregated += aggregate_props(dir_name, keys)

        target_path=os.path.join(confdir(environment), 'latticeengines.properties')
        if os.path.isfile(target_path):
            os.remove(target_path)
        with open(target_path, 'w') as f:
            print 'Writing to ' + target_path
            f.write(aggregated)

    for environment in ENV_WITH_AWS:
        aggregated=""
        keys={}
        for dir_name, _, _ in os.walk(WSHOME):
            end_with = PROPERTY_DIR + environment + "_aws"
            if dir_name[-len(end_with):] == end_with \
                    and 'le-config' not in dir_name \
                    and 'le-docker' not in dir_name \
                    and 'le-awsenvironment' not in dir_name:
                aggregated += aggregate_props(dir_name, keys)

        for dir_name, _, _ in os.walk(WSHOME):
            end_with = PROPERTY_DIR + environment
            if dir_name[-len(end_with):] == end_with \
                    and 'le-config' not in dir_name \
                    and 'le-docker' not in dir_name \
                    and 'le-awsenvironment' not in dir_name:
                aggregated += aggregate_props(dir_name, keys, quiet=True)

        target_path=os.path.join(confdir(environment + "_aws"), 'latticeengines.properties')
        if os.path.isfile(target_path):
            os.remove(target_path)
        with open(target_path, 'w') as f:
            print 'Writing to ' + target_path
            f.write(aggregated)

def aggregate_props(dir, keys, quiet=False):
    prop_files = glob.glob(dir + '/' + PROPERTY_FILE_SUFFIX)
    aggregated = ""
    for prop_file in prop_files:
        if 'log4j' not in prop_file:
            aggregated += "# ==================================================\n# " \
                          + prop_file.replace(WSHOME + "/", "") \
                          + "\n# ==================================================\n"
            with open(prop_file, 'r') as f:
                for line in f:
                    if len(line.strip()) > 0 and ('#' != line.strip()[0]):
                        key = line.strip().replace('\n', '').split('=')[0]
                        if key in keys:
                            if not quiet:
                                raise ValueError("Found duplicated key %s in %s and %s" % (key, keys[key], prop_file))
                            else:
                                continue
                        keys[key] = prop_file
                    aggregated += line
    return aggregated + "\n"

def confdir(environment):
    return os.path.join(WSHOME, 'le-config', 'conf', 'env', environment)

if __name__ == '__main__':
    main()
