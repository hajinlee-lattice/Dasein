package com.latticeengines.pls.service.impl.fileprocessor.state;

import java.io.File;
import java.io.IOException;
import java.util.Collection;
import java.util.Properties;

import org.apache.commons.io.FileUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.yarn.api.records.FinalApplicationStatus;

import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.dataplatform.JobStatus;
import com.latticeengines.domain.exposed.pls.FilePayload;
import com.latticeengines.pls.entitymanager.impl.microservice.RestApiProxy;
import com.latticeengines.pls.service.impl.fileprocessor.FileProcessingState;

public class TerminalStateProcessor extends BaseStateProcessor {
    
    private static final Log log = LogFactory.getLog(TerminalStateProcessor.class);

    public void processDir(File baseDir, //
            FileProcessingState state, //
            FileProcessingState priorState, //
            Properties properties, //
            FinalApplicationStatus... statuses) {
        File stateDir = super.mkdirForState(baseDir, state);
        File processingDir = super.mkdirForState(baseDir, priorState);
        
        Collection<File> processingFiles = FileUtils.listFiles(processingDir, //
                new String[] { "json" }, false);
        RestApiProxy restApiProxy = getRestApiProxy(properties);
        
        for (File processingFile : processingFiles) {
            String payloadStr = null;
            try {
                payloadStr = FileUtils.readFileToString(processingFile);
            } catch (IOException e) {
                log.error("Cannot read file " + processingFile.getAbsolutePath(), e);
                continue;
            }
            
            FilePayload payload = JsonUtils.deserialize(payloadStr, FilePayload.class);
            
            JobStatus jobStatus = restApiProxy.getJobStatus(payload.applicationId);
            
            boolean jobStatusMatched = false;
            
            for (FinalApplicationStatus status : statuses) {
                if (status == jobStatus.getStatus()) {
                    jobStatusMatched = true;
                    break;
                }
            }
            if (jobStatusMatched) {
                try {
                    String dataFilePath = payload.filePath;
                    FileUtils.moveFile(new File(dataFilePath), new File(dataFilePath + ".processed"));
                    FileUtils.moveFileToDirectory(processingFile, stateDir, true);
                } catch (IOException e) {
                    log.error(String.format("Cannot set processed file %s to terminal state.", payload.filePath), e);
                }
            }
        }
    
    }

}
