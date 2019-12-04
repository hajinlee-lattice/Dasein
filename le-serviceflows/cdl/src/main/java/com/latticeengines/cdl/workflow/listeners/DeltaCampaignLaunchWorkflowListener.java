package com.latticeengines.cdl.workflow.listeners;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import javax.inject.Inject;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.JobExecution;
import org.springframework.stereotype.Component;

import com.latticeengines.domain.exposed.pls.LaunchState;
import com.latticeengines.domain.exposed.serviceflows.cdl.DeltaCampaignLaunchWorkflowConfiguration;
import com.latticeengines.domain.exposed.workflow.WorkflowContextConstants;
import com.latticeengines.domain.exposed.workflow.WorkflowJob;
import com.latticeengines.proxy.exposed.cdl.PlayProxy;
import com.latticeengines.workflow.exposed.entitymanager.WorkflowJobEntityMgr;
import com.latticeengines.workflow.listener.LEJobListener;

@Component("deltaCampaignLaunchWorkflowListener")
public class DeltaCampaignLaunchWorkflowListener extends LEJobListener {

    private static final Logger log = LoggerFactory.getLogger(DeltaCampaignLaunchWorkflowListener.class);

    @Inject
    private PlayProxy playProxy;

    private String customerSpace;

    @Inject
    private WorkflowJobEntityMgr workflowJobEntityMgr;

    @Override
    public void beforeJobExecution(JobExecution jobExecution) {

    }

    @Override
    public void afterJobExecution(JobExecution jobExecution) {
        try {
            if (jobExecution.getStatus().isUnsuccessful()) {
                WorkflowJob job = workflowJobEntityMgr.findByWorkflowId(jobExecution.getId());
                customerSpace = job.getTenant().getId();
                String playName = job.getInputContextValue(WorkflowContextConstants.Inputs.PLAY_NAME);
                String playLaunchId = job.getInputContextValue(WorkflowContextConstants.Inputs.PLAY_LAUNCH_ID);
                log.warn(String.format("CampaignLaunch failed. Update launch %s of Campaign %s for customer %s",
                        playLaunchId, playName, customerSpace));
                playProxy.updatePlayLaunch(customerSpace, playName, playLaunchId, LaunchState.Failed);
            }
        } finally {
            cleanupIntermediateFiles(jobExecution);
        }
    }

    private void cleanupIntermediateFiles(JobExecution jobExecution) {
        boolean createRecommendationDataFrame = Boolean.toString(true).equals(getStringValueFromContext(jobExecution,
                DeltaCampaignLaunchWorkflowConfiguration.CREATE_RECOMMENDATION_DATA_FRAME));
        boolean createAddCsvDataFrame = Boolean.toString(true).equals(getStringValueFromContext(jobExecution,
                DeltaCampaignLaunchWorkflowConfiguration.CREATE_ADD_CSV_DATA_FRAME));
        boolean createDeleteCsvDataFrame = Boolean.toString(true).equals(getStringValueFromContext(jobExecution,
                DeltaCampaignLaunchWorkflowConfiguration.CREATE_DELETE_CSV_DATA_FRAME));

        List<String> hdfsIntermediateFiles = new ArrayList<>();
        List<String> s3UploadFiles = getListObjectFromContext(jobExecution,
                DeltaCampaignLaunchWorkflowConfiguration.RECOMMENDATION_S3_EXPORT_FILE_PATHS, String.class);

        if (createRecommendationDataFrame) {
            hdfsIntermediateFiles.add(getStringValueFromContext(jobExecution,
                    DeltaCampaignLaunchWorkflowConfiguration.RECOMMENDATION_AVRO_HDFS_FILEPATH));
        }
        if (createAddCsvDataFrame) {
            hdfsIntermediateFiles.add(getStringValueFromContext(jobExecution,
                    DeltaCampaignLaunchWorkflowConfiguration.ADD_CSV_EXPORT_AVRO_HDFS_FILEPATH));
        }
        if (createDeleteCsvDataFrame) {
            hdfsIntermediateFiles.add(getStringValueFromContext(jobExecution,
                    DeltaCampaignLaunchWorkflowConfiguration.DELETE_CSV_EXPORT_AVRO_HDFS_FILEPATH));
        }
        if (s3UploadFiles != null) {
            hdfsIntermediateFiles.addAll(s3UploadFiles);
        }

        log.info("Deleting files: " + Arrays.toString(hdfsIntermediateFiles.toArray()));
        for (String filePath : hdfsIntermediateFiles) {
            if (StringUtils.isBlank(filePath)) {
                continue;
            }
            try {
                // HdfsUtils.rmdir(yarnConfiguration, //
                // filePath.substring(0, filePath.lastIndexOf("/")));
            } catch (Exception ex) {
                log.error("Ignoring error while deleting dir: {}" //
                        + filePath.substring(0, filePath.lastIndexOf("/")), //
                        ex.getMessage());
            }
        }

    }

}
