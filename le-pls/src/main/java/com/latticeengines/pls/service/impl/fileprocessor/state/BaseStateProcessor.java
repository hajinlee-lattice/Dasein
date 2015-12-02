package com.latticeengines.pls.service.impl.fileprocessor.state;

import java.io.File;
import java.util.Properties;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.latticeengines.pls.entitymanager.impl.microservice.RestApiProxy;
import com.latticeengines.pls.service.impl.fileprocessor.FileProcessingState;
import com.latticeengines.pls.service.impl.fileprocessor.StateProcessor;

public class BaseStateProcessor implements StateProcessor {
    private static final Log log = LogFactory.getLog(BaseStateProcessor.class);
    
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
    
    protected RestApiProxy getRestApiProxy(Properties props) {
        RestApiProxy proxy = null;
        try {
            proxy = (RestApiProxy) props.get("restApiProxy");
        } catch (Exception e) {
            log.error(e);
        }
        return proxy;
    }
    
    @Override
    public void processDir(File baseDir, FileProcessingState state, FileProcessingState priorState, Properties properties) {
    }

}
