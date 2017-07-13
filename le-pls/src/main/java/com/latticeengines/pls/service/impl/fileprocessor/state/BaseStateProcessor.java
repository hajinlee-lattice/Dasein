package com.latticeengines.pls.service.impl.fileprocessor.state;

import java.io.File;
import java.util.Properties;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.latticeengines.pls.service.impl.fileprocessor.FileProcessingState;
import com.latticeengines.pls.service.impl.fileprocessor.StateProcessor;
import com.latticeengines.proxy.exposed.workflowapi.WorkflowProxy;

public class BaseStateProcessor implements StateProcessor {
    private static final Logger log = LoggerFactory.getLogger(BaseStateProcessor.class);
    
    protected FileDeleter fileDeleter = new FileDeleter();
    
    protected final File mkdirForState(File baseDir, FileProcessingState state) {
        File stateDir = new File(baseDir.getAbsolutePath() + "/" + state.name().toLowerCase()); 
        if (!stateDir.exists()) {
            stateDir.mkdir();
        }
        return stateDir;
    }
    
    protected final String[] stripExtension(File file) {
        String fileName = file.getName();
        String[] tokens = fileName.split("~");
        
        int index = tokens.length - 1;
        String tenant = null;
        if (index != 0) {
            tenant = tokens[0];
        }
        return new String[] { tenant, tokens[index].substring(0, tokens[index].lastIndexOf(".")) };
    }
    
    protected WorkflowProxy getRestApiProxy(Properties props) {
        WorkflowProxy proxy = null;
        try {
            proxy = (WorkflowProxy) props.get("restApiProxy");
        } catch (Exception e) {
            log.error(e.getMessage(), e);
        }
        return proxy;
    }
    
    @Override
    public void processDir(File baseDir, FileProcessingState state, FileProcessingState priorState, Properties properties) {
    }

}
