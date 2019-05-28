from __future__ import print_function

import argparse
import os
import subprocess

WSHOME = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

def chdirToProjectDir(project):
    os.chdir(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))) + '/' + project)
    print("Change to directory: ", os.getcwd())

def propDirsOpts():
    if 'LE_ENVIRONMENT' not in os.environ or os.environ['LE_ENVIRONMENT'] == '':
        le_env = "dev"
    else:
        le_env= os.environ['LE_ENVIRONMENT']
    return ['-DLE_PROPDIR=%s/le-config/conf/env/%s' % (os.environ['WSHOME'], le_env)]


def commonOpts():
    args = [
        '-Djavax.net.ssl.trustStore=%s/le-security/certificates/cacerts' % WSHOME,
        '-Dlog4j.configurationFile=%s/le-dev/log4j2-test.xml' % WSHOME
    ]

    sp = subprocess.Popen(["java", "-version"], stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    v = sp.communicate()[1]
    if '1.8.' in v.decode("utf-8") :
        args.append('-Dmaxpermsize=')

    return args

def testOpts(args):
    testPattern = '-Dtest=%s' % args.test if (args.test[-6:] == 'TestNG') or ('#' in args.test) else '-Dtest=*%s*' % args.test
    profile_opts = ['-P%s' % p for p in args.profiles.split(',')]
    group_opts = [] if args.groups is None else ['-Dfunctional.groups=%s' % args.groups, '-Ddeployment.groups=%s' % args.groups]
    extra_opts = []
    if len(group_opts) == 0:
        if 'functional' in args.profiles.split(',') or 'functional2' in args.profiles.split(','):
            group_opts = [ '-Dfunctional.groups=functional' ]
        elif 'deployment' in args.profiles.split(',') or 'deployment2' in args.profiles.split(','):
            group_opts = [ '-Ddeployment.groups=deployment' ]
    if args.xml:
        profiles = [p for p in args.profiles.split(",") if p not in ('functional', 'deployment')]
        profiles.append('testng')
        testng_xml = '-Dtestng.xml=src/test/resources/testng/%s.xml' % args.xml
        extra_opts.append(testng_xml)
        profile_opts = ['-P%s' % p for p in profiles]
    if args.command == "verify":
        return profile_opts + group_opts + extra_opts + [testPattern, 'clean'] + args.command.split(',')
    else:
        return profile_opts + args.command.split(',')


def parseCliArgs():
    parser = argparse.ArgumentParser(description='Run test(s) using maven.')
    parser.add_argument('project', type=str, help='project name. e.g. pls, propdata, eai')
    parser.add_argument('-p', dest='profiles', type=str, default='functional', help='comma separated list of maven profiles. default is functional')
    parser.add_argument('-x', dest='xml', type=str, default='', help='testng xml in src/test/resources. when this is set, it will ignore -p and hard code profile to be testng')
    parser.add_argument('-g', dest='groups', type=str, default=None,
                        help='test groups (optional). can set multiple by comma separated list.')
    parser.add_argument('-t', dest='test', type=str, default='',
                        help='replace the token -Dtest=*{}*. For example, -t Model means -Dtest=*Model* . default is empty, meaning all tests.')
    parser.add_argument('-c', dest='command', type=str, default='verify',
                        help='the maven command to execute. e.g. verify, package, jetty:run. Default is verify')
    args = parser.parse_args()
    return args


if __name__ == "__main__":
    args = parseCliArgs()
    chdirToProjectDir('le-' + args.project)
    my_env = os.environ
    my_env["MAVEN_OPTS"] = "-Xmx1g"
    if args.command == "jetty:run":
        my_env["MAVEN_OPTS"] += " -Xdebug -Xnoagent -Djava.compiler=NONE -Xrunjdwp:transport=dt_socket,address=4002,server=y,suspend=n"
    commands = ['mvn'] + propDirsOpts() + commonOpts() + testOpts(args)
    print('Executing [with common opts added]: ', ' '.join(commands))
    subprocess.call(commands, env=my_env)
