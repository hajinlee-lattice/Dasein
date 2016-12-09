package com.latticeengines.datacloud.yarn.runtime;

public interface BulkMatchProcessorExecutor {
    
    void execute(ProcessorContext processorContext);

    void finalize(ProcessorContext initialProcessorContext) throws Exception;

}
