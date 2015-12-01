package com.latticeengines.pls.service.impl.fileprocessor.state;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Properties;

import org.apache.commons.io.FileUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.domain.exposed.workflow.WorkflowConfiguration;
import com.latticeengines.pls.entitymanager.impl.microservice.RestApiProxy;
import com.latticeengines.pls.service.impl.fileprocessor.FileProcessingState;


public class ProcessingStateProcessor extends BaseStateProcessor {

    private static final Log log = LogFactory.getLog(ProcessingStateProcessor.class);

    @Override
    public void processDir(File baseDir, FileProcessingState state, FileProcessingState priorState, Properties properties) {
        File stateDir = super.mkdirForState(baseDir, state);
        File queuedDir = super.mkdirForState(baseDir, priorState);

        List<File> queuedFiles = new ArrayList<>(FileUtils.listFiles(queuedDir, new String[] { "json" }, false));
        Collections.sort(queuedFiles, new Comparator<File>() {

            @Override
            public int compare(File o1, File o2) {
                return o1.getName().compareTo(o2.getName());
            }

        });

        RestApiProxy restApiProxy = (RestApiProxy) properties.get("restApiProxy");
        for (File queuedFile : queuedFiles) {
            String[] tenantAndFileName = stripExtension(queuedFile);
            String fileName = tenantAndFileName[1];
            String payloadStr;
            try {
                payloadStr = FileUtils.readFileToString(queuedFile);
                if (!deleteFileFromQueuedDir(queuedFile)) {
                    continue;
                }
            } catch (IOException e) {
                throw new LedpException(LedpCode.LEDP_18066, e, new String[] { fileName });
            }

            FilePayload payload = JsonUtils.deserialize(payloadStr, FilePayload.class);
            payload.applicationId = restApiProxy.submitWorkflow(new WorkflowConfiguration()).toString();
            payloadStr = JsonUtils.serialize(payload);
            String payloadFileName = stateDir.getAbsolutePath() + "/" + fileName + ".json";
            try {
                FileUtils.write(new File(payloadFileName), payloadStr);
            } catch (IOException e) {
                throw new LedpException(LedpCode.LEDP_18063, e, new String[] { payloadFileName });
            }
        }
    }

    private boolean deleteFileFromQueuedDir(File queuedFile) {
        int numTries = 0;
        int MAXTRIES = 3;
        while (queuedFile.exists() && numTries < MAXTRIES) {
            try {
                Thread.sleep(500L);
            } catch (InterruptedException e) {
                log.warn(e);
            }
            fileDeleter.deleteFile(queuedFile);
            numTries++;
        }

        return !queuedFile.exists();
    }
}
