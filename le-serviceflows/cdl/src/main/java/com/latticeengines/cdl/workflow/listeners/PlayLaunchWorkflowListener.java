package com.latticeengines.cdl.workflow.listeners;

import java.util.ArrayList;
import java.util.List;

import javax.inject.Inject;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.JobExecution;
import org.springframework.stereotype.Component;

import com.latticeengines.common.exposed.util.HdfsUtils;
import com.latticeengines.domain.exposed.serviceflows.cdl.play.PlayLaunchWorkflowConfiguration;
import com.latticeengines.workflow.listener.LEJobListener;

@Component("playLaunchWorkflowListener")
public class PlayLaunchWorkflowListener extends LEJobListener {

    private static final Logger log = LoggerFactory.getLogger(PlayLaunchWorkflowListener.class);

    @Inject
    private Configuration yarnConfiguration;

    @Override
    public void beforeJobExecution(JobExecution jobExecution) {

    }

    @Override
    public void afterJobExecution(JobExecution jobExecution) {
        cleanupIntermediateFiles(jobExecution);
    }

    private void cleanupIntermediateFiles(JobExecution jobExecution) {

        List<String> hdfsIntermediateFiles = new ArrayList<>();
        // String avroFile = getStringValueFromContext(jobExecution,
        // PlayLaunchWorkflowConfiguration.RECOMMENDATION_AVRO_HDFS_FILEPATH);
        List<String> s3UploadFiles = getListObjectFromContext(jobExecution,
                PlayLaunchWorkflowConfiguration.RECOMMENDATION_EXPORT_FILES, String.class);

        hdfsIntermediateFiles.add(avroFile);
        if (s3UploadFiles != null) {
            hdfsIntermediateFiles.addAll(s3UploadFiles);
        }

        log.info("Deleting files: " + hdfsIntermediateFiles);
        for (String filePath : hdfsIntermediateFiles) {
            if (StringUtils.isBlank(filePath)) {
                continue;
            }
            try {
                HdfsUtils.rmdir(yarnConfiguration, //
                        filePath.substring(0, filePath.lastIndexOf("/")));
            } catch (Exception ex) {
                log.error("Ignoring error while deleting dir: {}" //
                        + filePath.substring(0, filePath.lastIndexOf("/")), //
                        ex.getMessage());
            }
        }

    }

}
