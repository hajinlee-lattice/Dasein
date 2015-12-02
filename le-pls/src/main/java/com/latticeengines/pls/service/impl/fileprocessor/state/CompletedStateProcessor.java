package com.latticeengines.pls.service.impl.fileprocessor.state;

import java.io.File;
import java.util.Properties;

import org.apache.hadoop.yarn.api.records.FinalApplicationStatus;

import com.latticeengines.pls.service.impl.fileprocessor.FileProcessingState;


public class CompletedStateProcessor extends TerminalStateProcessor {
    
    @Override
    public void processDir(File baseDir, FileProcessingState state, FileProcessingState priorState, Properties properties) {
        super.processDir(baseDir, state, priorState, properties, FinalApplicationStatus.SUCCEEDED);
    }

}
