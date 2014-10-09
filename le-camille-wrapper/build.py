import argparse, json, os, re, subprocess, sys, urllib2

class Manifest:
    def __init__(self, version, isRelease):
        self.version = version
        self.isRelease = isRelease
    @classmethod
    def read(cls):
        try:
            with open('camille.mf', 'r') as file:
                d = json.load(file)
                return cls(d['version'], d['isRelease'])
        except:
            return cls(None, None)
    def write(self):
        with open('camille.mf', 'w') as file:
            print >> file, json.dumps(self.__dict__)
    def __str__(self):
        return '{0} version {1}'.format('Release' if self.isRelease else 'Snapshot', self.version)

def tagValue(xml, tag):
    return re.search(re.compile(r"<({0})\b[^>]*>(.*?)</\1>".format(tag), re.IGNORECASE | re.DOTALL), xml).group(2)

def get(url):
    try:
        return urllib2.urlopen(url).read()
    except urllib2.HTTPError:
        return None

def metadataUrl(nexusUrl, repository, groupId, artifactId, version):
    return '{0}/service/local/artifact/maven/resolve?r={1}&g={2}&a={3}&v={4}'.format(nexusUrl, repository, groupId, artifactId, version)

def insertBeforeLast(text, insert, pattern):
    for match in re.finditer(pattern, text):
        pass
    return text[1:match.start()] + insert + text[(match.start()):]

def jarUrl(nexusUrl, repository, metadata):
    return '{0}/content/repositories/{1}/{2}'.format(nexusUrl, repository, insertBeforeLast(tagValue(metadata, 'repositoryPath'), '-jar-with-dependencies', '.jar'))

def majorVersion(snapshotVersion):
    try:
        return snapshotVersion.split('-')[0]
    except:
        return None
    
def build(nexusUrl, repoName, metadata, manifest, dll):
    url = jarUrl(nexusUrl, repoName, metadata)
    fileName = url.split('/')[-1]
    with open(fileName,'wb') as output:
        output.write(get(url))
    
    try:
        os.remove(dll)
    except OSError:
        pass
    
    p = subprocess.Popen(r"ikvmc -out:{0} -target:library {1}".format(dll, fileName), shell=True)
    p.communicate()
    if p.returncode:
        print >> sys.stderr, "IKVM failed."
        return p.returncode
    
    try:
        os.remove(fileName)
    except OSError:
        pass
    
    manifest.write()
    print 'Camille was updated to {0}.'.format(manifest)
    return 0

def main(argv):   
    parser = argparse.ArgumentParser(add_help=True)
    parser.add_argument("camilleDestinationPath", help="where Camille.dll will be deposited after this script is run")
    args = parser.parse_args()
    
    camilleDestinationPath = args.camilleDestinationPath.rstrip("\\")
    dll = r'{0}\{1}'.format(camilleDestinationPath, "Camille.dll")
 
    m = Manifest.read()
    
    nexusUrl = 'http://bodcdevvmvn63.lattice.local:8081/nexus'
    groupId = 'com.latticeengines'
    artifactId = 'le-camille-wrapper'
    snapshotsRepoName = 'snapshots'
    releasesRepoName = 'releases'
    
    snapshotMetadata = get(metadataUrl(nexusUrl, snapshotsRepoName, groupId, artifactId, 'LATEST'))
    if not snapshotMetadata:
        sys.exit('Error getting snapshot metadata.')
    snapshotVersion = tagValue(snapshotMetadata, 'version')
    
    releaseMetadata = get(metadataUrl(nexusUrl, releasesRepoName, groupId, artifactId, 'LATEST'))
    if not releaseMetadata:
        sys.exit('Error getting release metadata.')
    releaseVersion = tagValue(releaseMetadata, 'version')
    
    if (majorVersion(snapshotVersion) > releaseVersion):
        # snapshot controls
        if (majorVersion(snapshotVersion) > m.version and not m.isRelease) or m.isRelease:
            m.version = snapshotVersion
            m.isRelease = False
            return build(nexusUrl, snapshotsRepoName, snapshotMetadata, m, dll)
    else:
        # release controls
        if (releaseVersion > m.version and m.isRelease) or not m.isRelease:
            m.version = releaseVersion
            m.isRelease = True
            return build(nexusUrl, releasesRepoName, releaseMetadata, m, dll)  
    
    print 'Camille {0} is up to date.'.format(m)
    return 0

if __name__ == '__main__':
    sys.exit(main(sys.argv))