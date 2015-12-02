package com.latticeengines.pls.service.impl.fileprocessor;

import java.io.File;
import java.util.Properties;

import com.latticeengines.pls.service.impl.fileprocessor.state.CompletedStateProcessor;
import com.latticeengines.pls.service.impl.fileprocessor.state.FailedStateProcessor;
import com.latticeengines.pls.service.impl.fileprocessor.state.ProcessingStateProcessor;
import com.latticeengines.pls.service.impl.fileprocessor.state.QueuedStateProcessor;

public enum FileProcessingState {
    QUEUED(new QueuedStateProcessor(), null), //
    PROCESSING(new ProcessingStateProcessor(), QUEUED), //
    COMPLETED(new CompletedStateProcessor(), PROCESSING), //
    FAILED(new FailedStateProcessor(), PROCESSING);
    
    private final StateProcessor stateProcessor;
    private final FileProcessingState priorState;
    
    FileProcessingState(StateProcessor stateProcessor, FileProcessingState priorState) {
        this.stateProcessor = stateProcessor;
        this.priorState = priorState;
    }
    
    public void execute(File dir, Properties properties) {
        stateProcessor.processDir(dir, this, getPriorState(), properties);
    }

    public FileProcessingState getPriorState() {
        return priorState;
    }
    
}
