import operator
import pandas as pd
import numpy as np
import logging
import os
import shutil

from apsdataloader import ApsDataLoader
import apsgenerator
logging.basicConfig(level=logging.DEBUG, datefmt='%m/%d/%Y %I:%M:%S %p',
                    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(name='ApsGenerator')

if __name__ == '__main__':
    os.environ['StepflowConfig']='{"inputPaths":["/Pods/Aps/input/*.avro"], "outputPath\":"/Pods/Aps/output"}'
    os.environ['PYTHON_APP']='./apsgenerator.py'
    os.environ['SHDP_HD_FSWEB']='http://webhdfs.lattice.local:14000/webhdfs/v1'

    if not os.path.isdir('./input'):
        os.mkdir("./input")
    shutil.rmtree("./output", ignore_errors=True)
    os.mkdir("./output")
     
    loader = ApsDataLoader()
#     loader.downloadToLocal()
    df = loader.readDataFrameFromAvro()
    logger.info(df.shape)
    assert df.shape == (873967, 24)
     
    apState = apsgenerator.createAps(df)
    loader.writeDataFrameToAvro(apState)
    logger.info(apState.shape)
    assert  apState.shape == (833038, 202)
    newApSatate = loader.readDataFrameFromAvro("./output")
    print newApSatate.shape
    assert(apState.shape == newApSatate.shape)
    loader.uploadFromLocal()
    
