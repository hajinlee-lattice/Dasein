import json, os, re, subprocess, sys, urllib2

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

def deploy(nexusUrl, repoName, metadata, manifest):
    url = jarUrl(nexusUrl, repoName, metadata)
    fileName = url.split('/')[-1]
    with open(fileName,'wb') as output:
        output.write(get(url))
    
    dll = r"{0}\{1}".format(sys.path[0], "Camille.dll")
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
    print "The build completed successfully."
    return 0

def majorVersion(version):
    try:
        return version.split('-')[0]
    except:
        return None

def main(argv):    
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
    releaseVersion = tagValue(snapshotMetadata, 'version') if releaseMetadata else None
    
    if (majorVersion(snapshotVersion) > majorVersion(releaseVersion)):
        if snapshotVersion > m.version:
            m.version = snapshotVersion
            m.isRelease = False
            return deploy(nexusUrl, snapshotsRepoName, snapshotMetadata, m)
    else:
        if releaseVersion > m.version or not m.isRelease:
            m.version = releaseVersion
            m.isRelease = True
            return deploy(nexusUrl, releasesRepoName, releaseMetadata, m)        
    
    print "A rebuild of Camille was not required."
    return 0

if __name__ == '__main__':
    sys.exit(main(sys.argv))