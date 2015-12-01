package com.latticeengines.pls.service.impl.fileprocessor;

import java.io.File;
import java.util.Properties;

public interface StateProcessor {

    void processDir(File baseDir, FileProcessingState state, FileProcessingState priorState, Properties properties);
}
