package com.latticeengines.propdata.workflow.listeners;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationReport;
import org.apache.hadoop.yarn.client.api.YarnClient;
import org.apache.hadoop.yarn.util.ConverterUtils;
import org.springframework.batch.core.ExitStatus;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.JobExecutionListener;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.latticeengines.common.exposed.util.HdfsUtils;
import com.latticeengines.common.exposed.util.YarnUtils;
import com.latticeengines.domain.exposed.propdata.manage.MatchBlock;
import com.latticeengines.domain.exposed.propdata.manage.MatchCommand;
import com.latticeengines.domain.exposed.propdata.match.MatchStatus;
import com.latticeengines.propdata.core.service.impl.HdfsPathBuilder;
import com.latticeengines.propdata.match.service.MatchCommandService;
import com.latticeengines.propdata.workflow.steps.BulkMatchContextKey;

@Component("updateFailedMatchListener")
public class UpdateFailedMatchListener implements JobExecutionListener {

    private static final Log log = LogFactory.getLog(UpdateFailedMatchListener.class);

    @Autowired
    private MatchCommandService matchCommandService;

    @Autowired
    private HdfsPathBuilder hdfsPathBuilder;

    @Autowired
    private Configuration yarnConfiguration;

    @Override
    public void beforeJob(JobExecution jobExecution) {
    }

    @Override
    public void afterJob(JobExecution jobExecution) {
        ExitStatus exitStatus = jobExecution.getExitStatus();
        log.info("In AfterMatchListener.afterJob. ExitStatus=" + exitStatus);
        if (ExitStatus.FAILED.equals(exitStatus)) {
            failTheWorkflowAndUpdateCommandTable(jobExecution);
        }
    }

    private void failTheWorkflowAndUpdateCommandTable(JobExecution jobExecution) {
        String rootOperationUid = jobExecution.getExecutionContext().getString(BulkMatchContextKey.ROOT_OPERATION_UID);
        String errorMsg = "Unknown error.";
        if (jobExecution.getFailureExceptions() != null && !jobExecution.getFailureExceptions().isEmpty()) {
            errorMsg = jobExecution.getFailureExceptions().get(0).getMessage();
        }

        try {
            String matchErrorFile = hdfsPathBuilder.constructMatchErrorFile(rootOperationUid).toString();
            HdfsUtils.writeToFile(yarnConfiguration, matchErrorFile, errorMsg);
        } catch (Exception e) {
            log.error("Failed to write the error file: " + e.getMessage(), e);
        }

        MatchCommand matchCommand = matchCommandService.getByRootOperationUid(rootOperationUid);
        if (!MatchStatus.FAILED.equals(matchCommand.getMatchStatus())
                && !MatchStatus.ABORTED.equals(matchCommand.getMatchStatus())) {
            matchCommandService.update(rootOperationUid).status(MatchStatus.FAILED).errorMessage(errorMsg).commit();
        }

        killChildrenApplications(jobExecution);
    }

    private void killChildrenApplications(JobExecution jobExecution) {
        List list = (List) jobExecution.getExecutionContext().get(BulkMatchContextKey.APPLICATION_IDS);
        List<ApplicationId> applicationIds = new ArrayList<>();
        for (Object obj : list) {
            if (obj instanceof ApplicationId) {
                applicationIds.add((ApplicationId) obj);
            }
        }
        YarnClient yarnClient = YarnClient.createYarnClient();
        yarnClient.init(yarnConfiguration);
        yarnClient.start();

        String rootUid = jobExecution.getExecutionContext().getString(BulkMatchContextKey.ROOT_OPERATION_UID);
        List<MatchBlock> matchBlocks = matchCommandService.getByRootOperationUid(rootUid).getMatchBlocks();
        for (MatchBlock block: matchBlocks) {
            if (!YarnUtils.TERMINAL_APP_STATE.contains(block.getApplicationState())) {
                ApplicationId appId = ConverterUtils.toApplicationId(block.getApplicationId());
                try {
                    ApplicationReport report = yarnClient.getApplicationReport(appId);
                    if (!YarnUtils.TERMINAL_APP_STATE.contains(report.getYarnApplicationState())) {
                        yarnClient.killApplication(appId);
                    }

                    try {
                        log.info("Wait 10 sec to let applications drain.");
                        Thread.sleep(10000L);
                    } catch (Exception e) {
                        // ignore
                    }

                    String blockId = block.getBlockOperationUid();
                    matchCommandService.updateBlockByApplicationReport(blockId, report);
                } catch (Exception e) {
                    log.error("Error when killing the application " + appId, e);
                }
            }
        }

        try {
            yarnClient.close();
        } catch (IOException e) {
            throw new RuntimeException("Failed to stop yarn client.", e);
        }
    }

}