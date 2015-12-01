package com.latticeengines.pls.service.impl.fileprocessor.state;

import java.io.File;
import java.io.IOException;
import java.util.Collection;
import java.util.HashSet;
import java.util.Properties;
import java.util.Set;

import org.apache.commons.io.FileUtils;

import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.domain.exposed.pls.FilePayload;
import com.latticeengines.pls.service.impl.fileprocessor.FileProcessingState;


public class QueuedStateProcessor extends BaseStateProcessor {

    @Override
    public void processDir(File baseDir, FileProcessingState state, FileProcessingState priorState, Properties properties) {
        File stateDir = super.mkdirForState(baseDir, state);
        
        File dataDir = new File(baseDir.getAbsolutePath() + "/data");
        Collection<File> dataFiles = FileUtils.listFiles(dataDir, new String[] { "csv" }, false);
        Collection<File> queuedFiles = FileUtils.listFiles(stateDir, new String[] { "json" }, false);
        
        Set<String> queuedFilesSet = new HashSet<>();
        
        for (File queuedFile : queuedFiles) {
            queuedFilesSet.add(stripExtension(queuedFile)[1]);
        }
        
        for (File dataFile : dataFiles) {
            String[] tenantAndFileName = stripExtension(dataFile);
            String fileName = tenantAndFileName[1];
            if (!queuedFilesSet.contains(fileName)) {
                FilePayload payload = new FilePayload();
                payload.customerSpace = tenantAndFileName[0];
                payload.filePath = dataDir.getAbsolutePath() + "/" + fileName + ".csv";
                String payloadStr = JsonUtils.serialize(payload);
                String payloadFileName = stateDir.getAbsolutePath() + "/" + fileName + ".json";
                try {
                    FileUtils.write(new File(payloadFileName), payloadStr);
                } catch (IOException e) {
                    throw new LedpException(LedpCode.LEDP_18063, e, new String[] { payloadFileName });
                }
            }
        }
    }

}
