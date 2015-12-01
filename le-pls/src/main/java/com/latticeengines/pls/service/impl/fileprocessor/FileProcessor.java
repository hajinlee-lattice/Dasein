package com.latticeengines.pls.service.impl.fileprocessor;

import java.io.File;
import java.util.Properties;

import org.quartz.DisallowConcurrentExecution;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;
import org.springframework.scheduling.quartz.QuartzJobBean;
import org.springframework.stereotype.Component;

import com.latticeengines.pls.entitymanager.impl.microservice.RestApiProxy;

@DisallowConcurrentExecution
@Component("fileProcessor")
public class FileProcessor extends QuartzJobBean {

    private String fileProcessorDir;
    private RestApiProxy restApiProxy;
    

    @Override
    protected void executeInternal(JobExecutionContext context) throws JobExecutionException {
        Properties props = new Properties();
        props.put("restApiProxy", restApiProxy);
        for (FileProcessingState state : FileProcessingState.values()) {
            File dir = new File(fileProcessorDir);
            state.execute(dir, props);
        }
        
    }

    public String getFileProcessorDir() {
        return fileProcessorDir;
    }

    public void setFileProcessorDir(String fileProcessorDir) {
        this.fileProcessorDir = fileProcessorDir;
    }

    public RestApiProxy getRestApiProxy() {
        return restApiProxy;
    }

    public void setRestApiProxy(RestApiProxy restApiProxy) {
        this.restApiProxy = restApiProxy;
    }
    
}
