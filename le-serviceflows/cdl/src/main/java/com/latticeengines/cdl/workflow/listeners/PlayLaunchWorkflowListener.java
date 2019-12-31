package com.latticeengines.cdl.workflow.listeners;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import javax.inject.Inject;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.JobExecution;
import org.springframework.stereotype.Component;

import com.latticeengines.common.exposed.util.HdfsUtils;
import com.latticeengines.domain.exposed.pls.LaunchState;
import com.latticeengines.domain.exposed.serviceflows.cdl.play.PlayLaunchWorkflowConfiguration;
import com.latticeengines.domain.exposed.workflow.WorkflowContextConstants;
import com.latticeengines.domain.exposed.workflow.WorkflowJob;
import com.latticeengines.proxy.exposed.cdl.PlayProxy;
import com.latticeengines.workflow.exposed.entitymanager.WorkflowJobEntityMgr;
import com.latticeengines.workflow.listener.LEJobListener;

@Component("playLaunchWorkflowListener")
public class PlayLaunchWorkflowListener extends LEJobListener {

    private static final Logger log = LoggerFactory.getLogger(PlayLaunchWorkflowListener.class);

    @Inject
    private PlayProxy playProxy;

    @Inject
    private Configuration yarnConfiguration;

    private String customerSpace;

    @Inject
    private WorkflowJobEntityMgr workflowJobEntityMgr;

    @Override
    public void beforeJobExecution(JobExecution jobExecution) {

    }

    @Override
    public void afterJobExecution(JobExecution jobExecution) {
        WorkflowJob job = workflowJobEntityMgr.findByWorkflowId(jobExecution.getId());
        customerSpace = job.getTenant().getId();
        String playName = job.getInputContextValue(WorkflowContextConstants.Inputs.PLAY_NAME);
        String playLaunchId = job.getInputContextValue(WorkflowContextConstants.Inputs.PLAY_LAUNCH_ID);
        try {
            if (jobExecution.getStatus().isUnsuccessful()) {
                log.warn(String.format("CampaignLaunch failed. Update launch %s of Campaign %s for customer %s",
                        playLaunchId, playName, customerSpace));
                playProxy.updatePlayLaunch(customerSpace, playName, playLaunchId, LaunchState.Failed);
            } else {
                log.info(String.format("CampaignLaunch is successful. Update launch %s of Campaign %s for customer %s",
                        playLaunchId, playName, customerSpace));
                playProxy.updatePlayLaunch(customerSpace, playName, playLaunchId, LaunchState.Launched);
            }
        } finally {
            cleanupIntermediateFiles(jobExecution);
        }
    }

    private void cleanupIntermediateFiles(JobExecution jobExecution) {

        List<String> hdfsIntermediateFiles = new ArrayList<>();
        List<String> s3UploadFiles = getListObjectFromContext(jobExecution,
                PlayLaunchWorkflowConfiguration.RECOMMENDATION_EXPORT_FILES, String.class);

        hdfsIntermediateFiles.add(getStringValueFromContext(jobExecution,
                PlayLaunchWorkflowConfiguration.RECOMMENDATION_AVRO_HDFS_FILEPATH));
        hdfsIntermediateFiles.add(getStringValueFromContext(jobExecution,
                PlayLaunchWorkflowConfiguration.RECOMMENDATION_CSV_EXPORT_AVRO_HDFS_FILEPATH));
        if (s3UploadFiles != null) {
            hdfsIntermediateFiles.addAll(s3UploadFiles);
        }

        log.info("Deleting files: " + Arrays.toString(hdfsIntermediateFiles.toArray()));
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
