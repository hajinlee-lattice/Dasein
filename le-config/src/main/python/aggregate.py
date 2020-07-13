from __future__ import print_function

import glob
import os

PROPERTY_DIR = "/conf/env/"
PROPERTY_FILE_SUFFIX = "*.properties"
LINE_SEPERATOR = "\n=============================\n"
ENVIRONMENTS=('dev', 'devcluster', 'qacluster','prodcluster', 'prodcluster_dr')

WSHOME=os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))))

def main():
    print("Scanning WSHOME " + WSHOME)

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
            print('Writing to ' + target_path)
            f.write(aggregated)

    overwrite_props('prodcluster', 'prodcluster_dr')

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

def overwrite_props(base, overlay):
    print("Overwriting %s with %s" % (base, overlay))
    base_props=os.path.join(confdir(base), 'latticeengines.properties')
    overlay_props=os.path.join(confdir(overlay), 'latticeengines.properties')
    overlay_keys = set()

    if os.path.isfile(overlay_props):
        fout = open(overlay_props, 'a')
        with open(overlay_props, 'r') as f:
            for line in f:
                if len(line.strip()) > 0 and ('#' != line.strip()[0]):
                    key = line.strip().replace('\n', '').split('=')[0]
                    overlay_keys.add(key)
    else:
        fout = open(overlay_props, 'w')

    with open(base_props, 'r') as f1:
        for line in f1:
            if len(line.strip()) > 0 and ('#' != line.strip()[0]):
                key = line.strip().replace('\n', '').split('=')[0]
                if key in overlay_keys:
                    # skip as to be overwritten by overlay
                    continue
            fout.write(line)

    fout.close()


def confdir(environment):
    return os.path.join(WSHOME, 'le-config', 'conf', 'env', environment)

if __name__ == '__main__':
    main()
