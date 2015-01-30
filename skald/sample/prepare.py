# This should be run either from the root skald folder, or from a folder
# that contains all the RTS build artifacts.

import os
import sys
import re
import urllib2
import argparse
import subprocess
import platform
import shutil
import traceback
import time


def log(text):
    print text


def make_request(url, header, data):
    request = urllib2.Request(url, headers=header, data=data)
    response = urllib2.urlopen(request)
    if response.code == 200:
        log("Call to {0} successful".format(url))
    else:
        raise Exception("Received code {0} from request".format(response.code))


def start_zookeeper(output):
    log("Starting ZooKeeper process...")
    zk_path = os.environ["zookeeper_install"]

    if (platform.system() == 'Windows'):
        return subprocess.Popen(os.path.join(zk_path, "bin", "zkServer.cmd"),
                                stdout=output, stderr=sys.stdout)

    # TODO: Get something setup to actually test this...
    if (platform.system() == 'Linux'):
        return subprocess.Popen(os.path.join(zk_path, "bin", "zkServer.sh"),
                                "start", stdout=output, stderr=sys.stdout)
    
    raise Exception("Invalid platform type: {0}".format(platform.system()))


def make_path(base, pattern):
    matches = [x for x in os.listdir(base) if re.match(pattern, x)]
    if len(matches) == 0:
        return None

    if len(matches) > 1:
        raise Exception(
            "Multiple files patching {0} found in {1}".format(pattern, base))

    return os.path.join(base, matches[0])


def main(args):
    defaultPodId = "test"
    defaultSkaldConnectionString = "http://localhost:8050"
    defaultZooKeeperConnectionString = "localhost:2181"

    default_model_name = "Q_PLS_Modeling_Lattice_Relaunch"

    defaultContractId = "Lattice_Relaunch"
    defaultTenantId = "Lattice_Relaunch"
    defaultSpaceId = "Production"

    zk_process = None
    skald_process = None

    try:
        parser = argparse.ArgumentParser()
        parser.add_argument("-zooKeeperConnectionString", "-zkcs",
                            help='Connection string for ZooKeeper. Defaults to {0}'.format(
                                defaultZooKeeperConnectionString), nargs='?', default=defaultZooKeeperConnectionString)
        parser.add_argument("-skaldConnectionString", "-scs",
                            help='Connection string for Skald. Defaults to {0}'.format(defaultSkaldConnectionString),
                            nargs='?', default=defaultSkaldConnectionString)
        parser.add_argument("-noZooKeeper", "-nzk", help='Do not start ZooKeeper', action='store_true')
        parser.add_argument("-noSkald", "-ns", help='Do not start Skald', action='store_true')
        parser.add_argument("-verbose", "-v", help='Verbose', action='store_true')
        args = parser.parse_args()
        zk_connection_string = args.zooKeeperConnectionString
        skald_connection_string = args.skaldConnectionString
        no_skald = args.noSkald
        no_zk = args.noZooKeeper
        verbose = args.verbose

        script_dir = os.path.abspath(os.path.dirname(__file__))

        output = sys.stdout
        if not verbose:
            output = open(os.devnull, 'w')

        if not no_zk:
            zk_process = start_zookeeper(output)

        if not no_skald:
            log("Starting Skald process...")
            skald_path = make_path(script_dir, "skald-.*-SNAPSHOT-shaded\\.jar")
            if skald_path is None:
                skald_path = make_path(os.path.join(script_dir, "..", "target"),
                                       "skald-.*-SNAPSHOT-shaded\\.jar")
            if skald_path is None:
                raise Exception("Could not find Skald shaded JAR")
   
            skald_process = subprocess.Popen(
                "java -jar {0}".format(skald_path),
                 stdout=output, stderr=sys.stdout)

            log("Waiting for Skald to initialize...")
            time.sleep(5)


        baton_path = make_path(script_dir, "le-baton-.*-SNAPSHOT-shaded\\.jar")
        if baton_path is None:
            baton_path = make_path(
                os.path.join(script_dir, "..", "..", "le-baton", "target"),
                "le-baton-.*-SNAPSHOT-shaded\\.jar")
        if baton_path is None:
            raise Exception("Could not find Baton shaded JAR")
        
        interface_path = os.path.join(script_dir, "stub", "interfaces")

        # This will fail if the pod already exists; that's fine.
        subprocess.call(
            "java -jar {0} -createPod --connectionString {1} --podId {2}".format(
                baton_path, zk_connection_string, defaultPodId))

        # This will fail if the tenant already exists; that's fine.
        subprocess.call(
            "java -jar {0} -createTenant --connectionString {1} --podId {2} --contractId {3} --tenantId {4} --spaceId {5}".format(
                baton_path, zk_connection_string, defaultPodId, defaultContractId, defaultTenantId, defaultSpaceId))

        subprocess.check_call(
            "java -jar {0} -loadDirectory --connectionString {1} --podId {2} --source {3} --destination Interfaces".format(
                baton_path, zk_connection_string, defaultPodId, interface_path))

        make_request("{0}/SetModelTags".format(skald_connection_string),
                     {"Content-Type": "application/json"},
                     """
                    {{
                        "space": {{"contractId": "{0}", "tenantId": "{1}", "spaceId": "{2}"}},
                        "tags": {{"{3}": {{"active": 1, "test": 2}}}}
                    }}""".format(defaultContractId, defaultTenantId, defaultSpaceId, default_model_name))

        make_request("{0}/SetModelCombination".format(skald_connection_string),
                     {"Content-Type": "application/json"},
                     """
                    {{
                        "space": {{"contractId": "{0}", "tenantId": "{1}", "spaceId": "{2}"}},
                        "name": "realtime",
                        "combination": [{{"filter": null, "model": "{3}"}}]
                    }}""".format(defaultContractId, defaultTenantId, defaultSpaceId, default_model_name))

        record = open(os.path.join(script_dir, "request.json")).read()
        make_request("{0}/ScoreRecord".format(skald_connection_string),
                     {"Content-Type": "application/json"},
                     record)


    except Exception as e:
        log(traceback.format_exc())

    finally:
        if zk_process:
            zk_process.kill()
            log("Killed ZooKeeper process")
        if skald_process:
            skald_process.kill()
            log("Killed Skald process")


if __name__ == '__main__':
    main(sys.argv)
