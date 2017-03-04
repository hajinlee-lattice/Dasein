import atexit
import glob
import os
import shutil
import sys
import zipfile


fwkdir = "./leframework.tar.gz"
pipelinefwkdir = "./lepipeline.tar.gz"
evpipelinefwkdir = "./evpipeline.tar.gz"
swlibdir = "./le-serviceflows-leadprioritization-2.0.22-SNAPSHOT.jar"

basePath = os.path.join('..', '..') if os.path.exists("../../main/python/rulefwk.py") else os.path.join('..', '..', '..')

# # This needs to be at the scope of the module
_filesToCopy = {}

sys.path.append(fwkdir)
sys.path.append(pipelinefwkdir)
sys.path.append(evpipelinefwkdir)

def setup(filesToCopy):
    print 'Simulating HDFS'
    # These properties won't really be used since these are just unit tests.
    # Functional and end-to-end tests should be done from java
    os.environ["CONTAINER_ID"] = "container_1425511391553_3644_01_000001"
    os.environ["SHDP_HD_FSWEB"] = "http://localhost:50070"
    os.environ["DEBUG"] = "true"
    # Simulate what happens in yarn when it copies the framework code over
    # before running the python script

    filesToCopy[os.path.join(basePath, "main", "python", "launcher.py")] = "launcher.py"
    filesToCopy[os.path.join(basePath, "main", "python", "pipelinefwk.py")] = "pipelinefwk.py"
    filesToCopy[os.path.join(basePath, "main", "python", "rulefwk.py")] = "rulefwk.py"
    filesToCopy[os.path.join(basePath, "main", "python", "pipeline", "pipeline.py")] = "pipeline.py"
    filesToCopy[os.path.join(basePath, "main", "python", "configurablepipelinetransformsfromfile", "pipeline.json")] = "pipeline.json"
    filesToCopy[os.path.join(basePath, "main", "python", "datarules", "rulepipeline.json")] = "rulepipeline.json"
    filesToCopy[os.path.join(basePath, "main", "python", "configurablepipelinetransformsfromfile", "pmmlpipeline.json")] = "pmmlpipeline.json"
    filesToCopy[os.path.join(basePath, "main", "python", "evpipeline", "evpipeline.py")] = "evpipeline.py"

    for (root, dirs, files) in os.walk(os.path.join(basePath, "main", "python", "algorithm")):
        for f in files:
            if f[-3:] != '.py':
                continue
            src = os.path.join(root, f)
            tgt = f
            filesToCopy[src] = tgt

    if os.path.exists(fwkdir):
        shutil.rmtree(fwkdir)
    if os.path.exists(pipelinefwkdir):
        shutil.rmtree(pipelinefwkdir)
    if os.path.exists(evpipelinefwkdir):
        shutil.rmtree(evpipelinefwkdir)
    if os.path.exists(swlibdir):
        shutil.rmtree(swlibdir)

    for d in [fwkdir + "/leframework", pipelinefwkdir, evpipelinefwkdir, swlibdir]:
        print 'Making directory {}'.format(d)
        os.makedirs(d)

    for (src, tgt) in filesToCopy.iteritems():
        if os.path.isfile(tgt):
            os.remove(tgt)
        print 'Copying: ', src, '-->', tgt
        shutil.copy(src, tgt)

    for filename in glob.glob(os.path.join(basePath, "main", "python", "pipeline", "*.py")):
        print 'Copying: ', filename, '==>', pipelinefwkdir
        shutil.copy(filename, pipelinefwkdir)

    for filename in glob.glob(os.path.join(basePath, "main", "python", "datarules", "*.py")):
        print 'Copying: ', filename, '==>', pipelinefwkdir
        shutil.copy(filename, pipelinefwkdir)

    for filename in glob.glob(os.path.join(basePath, "main", "python", "evpipeline", "*.py")):
        print 'Copying: ', filename, '==>', evpipelinefwkdir
        shutil.copy(filename, evpipelinefwkdir)

    for (root, dirs, files) in os.walk(os.path.join(basePath, "main", "python", "leframework")):
        for dsrc in dirs:
            dtgt = os.path.join(root.replace(os.path.join(basePath, "main", "python"), os.path.join(".", "leframework.tar.gz"), 1), dsrc)
            print 'Making directory {}'.format(dtgt)
            os.makedirs(dtgt)
        for f in files:
            if f[-3:] != '.py':
                continue
            src = os.path.join(root, f)
            tgt = os.path.join(root.replace(os.path.join(basePath, "main", "python"), os.path.join(".", "leframework.tar.gz"), 1), f)
            print 'Copying: ', src, '==>', tgt
            shutil.copy(src, tgt)


    otherFilesToCopy = [ \
        {os.path.join(basePath, "main", "python", "pipeline", "encoder.py"): evpipelinefwkdir}, \
        {os.path.join(basePath, 'main', 'python', 'columntransform.py'): pipelinefwkdir}, \
        {os.path.join(basePath, 'main', 'python', 'columntransform.py'): evpipelinefwkdir}
    ]

    for m in otherFilesToCopy:
        for (src, tgt) in m.iteritems():
            print 'Copying: ', src, '==>', tgt
            shutil.copy(src, tgt)

    for filename in glob.glob(os.path.join(basePath + "/main/python/configurablepipelinetransformsfromfile", "*")):
        if filename.find("/pipelinenullconversionrate.json") >= 0:
            continue
        print 'Copying: ', filename, '==>', pipelinefwkdir
        shutil.copy(filename, pipelinefwkdir)
        print 'Copying: ', filename, '==>', evpipelinefwkdir
        shutil.copy(filename, evpipelinefwkdir)

    __unzipSoftwareLibJar()

def tearDown(filesToCopy):
    print 'Cleaning up simulated HDFS'
    for _, tgt in filesToCopy.iteritems():
        os.remove(tgt)
    if os.path.exists(fwkdir):
        shutil.rmtree(fwkdir)
    if os.path.exists(pipelinefwkdir):
        shutil.rmtree(pipelinefwkdir)
    if os.path.exists(evpipelinefwkdir):
        shutil.rmtree(evpipelinefwkdir)
    if os.path.exists(swlibdir):
        shutil.rmtree(swlibdir)

def __unzipSoftwareLibJar():
    zipFilePath = "data/le-serviceflows-leadprioritization-2.0.22-SNAPSHOT.zip" if \
        os.path.exists("data/le-serviceflows-leadprioritization-2.0.22-SNAPSHOT.zip") \
        else "../data/le-serviceflows-leadprioritization-2.0.22-SNAPSHOT.zip"
    with zipfile.ZipFile(zipFilePath) as z:
        z.extractall(swlibdir)

setup(_filesToCopy)
atexit.register(tearDown, filesToCopy=_filesToCopy)
