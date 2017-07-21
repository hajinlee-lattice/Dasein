import argparse
import os
import subprocess

WSHOME = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

def chdirToProjectDir(project):
    os.chdir(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))) + '/' + project)
    print "Change to directory: " + os.getcwd()


def propDirsOpts():
    if 'LE_ENVIRONMENT' not in os.environ or os.environ['LE_ENVIRONMENT'] == '':
        le_env = "dev"
    else:
        le_env= os.environ['LE_ENVIRONMENT']
    return ['-DLE_PROPDIR=%s/le-config/conf/env/%s' % (os.environ['WSHOME'], le_env)]


def commonOpts():
    args = [
        '-Djavax.net.ssl.trustStore=%s/le-security/certificates/cacerts' % WSHOME,
        '-Djava.util.logging.config.file=../le-dev/test-logging.properties'
    ]

    sp = subprocess.Popen(["java", "-version"], stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    v = sp.communicate()[1]
    if '1.8.' in v:
        args.append('-Dmaxpermsize=')

    return args

def testOpts(args):
    opts = ['-Pgenerate'] if args.project == 'security' else []
    if args.groups is None:
        testPattern = '-Dtest=%s' % args.test if args.test[-6:] == 'TestNG' else '-Dtest=*%s*' % args.test
        return opts + ['-P%s' % p for p in args.profiles.split(',')] + [testPattern, 'clean'] +  args.command.split(',')
    else:
        return opts + ['-P%s' % p for p in args.profiles.split(',')] + ['-Dfunctional.groups=%s' % args.groups, '-Ddeployment.groups=%s' % args.groups, '-Dtest=*%s*' % args.test, 'clean'] + args.command.split(',')

def parseCliArgs():
    parser = argparse.ArgumentParser(description='Run test(s) using maven.')
    parser.add_argument('project', type=str, help='project name. e.g. pls, propdata, eai')
    parser.add_argument('-p', dest='profiles', type=str, default='functional', help='comma separated list of maven profiles. default is functional')
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
    print 'Executing [with common opts added]: ' + ' '.join(['mvn'] + testOpts(args))
    my_env = os.environ
    my_env["MAVEN_OPTS"] = "-Xmx2g" if args.project == "datacloud/etl" else "-Xmx1g"
    if args.command == "jetty:run":
        my_env["MAVEN_OPTS"] += " -Xdebug -Xnoagent -Djava.compiler=NONE -Xrunjdwp:transport=dt_socket,address=4002,server=y,suspend=n"
    subprocess.call(['mvn'] + propDirsOpts() + commonOpts() + testOpts(args), env=my_env)