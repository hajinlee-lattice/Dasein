from __future__ import division
import logging
import os
import shutil
import uuid
import sys

from apploader import AppLoader
logging.basicConfig(level=logging.DEBUG, datefmt='%m/%d/%Y %I:%M:%S %p',
                    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(name='PythonApp')

if __name__ == '__main__':
    uid = str(uuid.uuid4())
    curDir = './' + uid
    if os.path.isdir('/mnt/ebs'):
        curDir = "/mnt/ebs/" + uid
    os.makedirs(curDir)
    cwd = os.getcwd() 
    logger.info("Current working dir:" + cwd)
    os.chdir(curDir)
     
    loader = AppLoader()
    loader.downloadToLocal(".")
    if len(loader.libPaths) > 0:
        sys.path.extend(loader.libPaths)
    
    logger.info('PYTHONPATH:' + str(sys.path))
    logger.info('appLauncher:' + loader.appLauncher)
    logger.info('appArgs:' + str(loader.appArgs))
    if os.environ.has_key("AWS_BATCH_JOB_ID"):
        logger.info('Job Id:' + os.environ['AWS_BATCH_JOB_ID'])
    if (loader.appArgs is not None):
        sys.argv = loader.appArgs
    execfile(loader.appLauncher, globals())
    
    os.chdir(cwd)
    shutil.rmtree(curDir, ignore_errors=True)
    
