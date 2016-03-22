import argparse
import os
import subprocess

WSHOME = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
PROPFILE_SUFFIX = '.properties'
CONF_EVN = '/conf/env/dev'


def chdirToProjectDir(project):
    os.chdir(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))) + '/' + project)
    print "Change to directory: " + os.getcwd()


def propDirsOpts():
    projects = []
    for dirName, subdirList, fileList in os.walk(WSHOME):
        for fileName in fileList:
            if fileName[-len(PROPFILE_SUFFIX):] == PROPFILE_SUFFIX and dirName[-len(CONF_EVN):] == CONF_EVN:
                project = dirName.replace(CONF_EVN, '').split('/')[-1]
                if project[:3] == 'le-':
                    projects.append(project[3:])
    return ['-D%s_PROPDIR=../le-%s' % (p.upper(), p) + CONF_EVN for p in projects]


def commonOpts():
    args = [
        '-Djavax.net.ssl.trustStore=../le-security/certificates/laca-ldap.dev.lattice.local.jks',
        '-Dsqoop.throwOnError=true'
    ]

    sp = subprocess.Popen(["java", "-version"], stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    v = sp.communicate()[1]
    if '1.8.' in v:
        args.append('-Dmaxpermsize=')

    return args

def testOpts(args):
    if args.groups is None:
        return ['-P%s' % args.profile, '-Dtest=*%s*' % args.test, 'verify']
    else:
        return ['-P%s' % args.profile, '-D%s.groups=%s' % (args.profile, args.groups), '-Dtest=*%s*' % args.test, 'verify']

def parseCliArgs():
    parser = argparse.ArgumentParser(description='Run test(s) using maven.')
    parser.add_argument('project', type=str, help='project name. e.g. pls, propdata, eai')
    parser.add_argument('-p', dest='profile', type=str, default='functional', help='maven profile. default is functional')
    parser.add_argument('-g', dest='groups', type=str, default=None,
                        help='test groups (optional). can set multiple by comman sepearted list.')
    parser.add_argument('-t', dest='test', type=str, default='',
                        help='replace the token -Dtest=*{}*. For example, -t Model means -Dtest=*Model* . default is empty, meaning all tests.')
    args = parser.parse_args()
    return args


if __name__ == "__main__":
    args = parseCliArgs()
    chdirToProjectDir('le-' + args.project)
    print 'Executing [with common opts added]: ' + ' '.join(['mvn'] + testOpts(args))
    subprocess.call(['mvn'] + propDirsOpts() + commonOpts() + testOpts(args))