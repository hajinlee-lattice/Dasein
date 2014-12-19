import os
import sys
import urllib2
import argparse
import subprocess
import platform
import shutil
import traceback
from xml.dom.minidom import parse


def log(someString):
    print(someString)


def make_request(url, header, data):
    request = urllib2.Request(url, headers=header, data=data)
    response = urllib2.urlopen(request)
    if response.code != 200:
        raise Exception("Received code {0} from request".format(response.code))


def main(args):
    defaultPodId = "test"
    defaultZooKeeperConnectionString = "localhost:2181"
    defaultSkaldConnectionString = "http://localhost:8040"

    default_model_name = "Q_PLS_ModelingLattice_Relaunch"

    defaultContractId = "Lattice_Relaunch"
    defaultTenantId = "Lattice_Relaunch"
    defaultSpaceId = "Production"

    zk_Process = None
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

        curr_dir = os.path.abspath(os.path.dirname(__file__))
        # parse out version number for baton/skald jar files
        doc = parse(os.path.join(curr_dir, "..", "..", "le-parent", "pom.xml"))
        version = doc.getElementsByTagName("version")[0].firstChild.data

        if verbose:
            output = sys.stdout
        else:
            output = open(os.devnull, 'w')

        if not no_zk:
            zk_path = os.environ["zookeeper_install"]

            if (platform.system() == 'Windows'):
                zk_Process = subprocess.Popen(os.path.join(zk_path, "bin", "zkServer.cmd"), stdout=output,
                                              stderr=sys.stdout)
            # TODO: Get something setup to actually test this...
            elif (platform.system() == 'Linux'):
                zk_Process = subprocess.Popen(os.path.join(zk_path, "bin", "zkServer.sh"), "start", stdout=output,
                                              stderr=sys.stdout)
            else:
                raise Exception("Invalid platform type: {0}".format(platform.system()))
            log("Started ZooKeeper process")

        if not no_skald:
            # need to copy skald.properties for Skald to run
            shutil.copy(os.path.join(curr_dir, "..", "skald.properties"), curr_dir)
            shutil.copy(os.path.join(curr_dir, "..", "skald.properties"), os.path.join(curr_dir, "..", "target"))

            skald_path = os.path.join(curr_dir, "..", "..", "skald", "target", "skald-{0}-shaded.jar".format(version))
            skald_process = subprocess.Popen("java -jar {0}".format(skald_path), stdout=output, stderr=sys.stdout)
            log("Started Skald process")

        baton_path = os.path.join(curr_dir, "..", "..", "le-baton", "target", "le-baton-{0}-shaded.jar".format(version))
        interface_path = os.path.join(curr_dir, "stub", "interfaces")

        subprocess.Popen(
            "java -jar {0} -createPod --connectionString {1} --podId {2}".format(
                baton_path, zk_connection_string, defaultPodId))
        subprocess.Popen(
            "java -jar {0} -createTenant --connectionString {1} --podId {2} --contractId {3} --tenantId {4} --spaceId {5}".format(
                baton_path, zk_connection_string, defaultPodId, defaultContractId, defaultTenantId, defaultSpaceId))
        subprocess.Popen(
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

        record = open(os.path.join(curr_dir, "request.json")).read()
        make_request("{0}/ScoreRecord".format(skald_connection_string),
                     {"Content-Type": "application/json"},
                     record)


    except Exception as e:
        log(traceback.format_exc())

    finally:
        if zk_Process:
            zk_Process.kill()
            log("Killed ZooKeeper process")
        if skald_process:
            skald_process.kill()
            log("Killed Skald process")


if __name__ == '__main__':
    main(sys.argv)