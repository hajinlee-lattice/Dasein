package com.latticeengines.datacloud.workflow.match.listeners;

import java.io.IOException;
import java.util.List;

import javax.inject.Inject;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationReport;
import org.apache.hadoop.yarn.client.api.YarnClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.ExitStatus;
import org.springframework.batch.core.JobExecution;
import org.springframework.stereotype.Component;

import com.latticeengines.common.exposed.util.HdfsUtils;
import com.latticeengines.common.exposed.util.YarnUtils;
import com.latticeengines.datacloud.core.util.HdfsPathBuilder;
import com.latticeengines.datacloud.match.exposed.service.MatchCommandService;
import com.latticeengines.datacloud.workflow.match.steps.BulkMatchContextKey;
import com.latticeengines.domain.exposed.datacloud.manage.MatchBlock;
import com.latticeengines.domain.exposed.datacloud.manage.MatchCommand;
import com.latticeengines.domain.exposed.datacloud.match.MatchStatus;
import com.latticeengines.domain.exposed.util.ApplicationIdUtils;
import com.latticeengines.workflow.listener.LEJobListener;

@Component("updateFailedMatchListener")
public class UpdateFailedMatchListener extends LEJobListener {

    private static final Logger log = LoggerFactory.getLogger(UpdateFailedMatchListener.class);

    @Inject
    private MatchCommandService matchCommandService;

    @Inject
    private HdfsPathBuilder hdfsPathBuilder;

    @Inject
    private Configuration yarnConfiguration;

    @Override
    public void beforeJobExecution(JobExecution jobExecution) {
    }

    @Override
    public void afterJobExecution(JobExecution jobExecution) {
        ExitStatus exitStatus = jobExecution.getExitStatus();
        log.info("In AfterMatchListener.afterJob. ExitStatus=" + exitStatus);
        if (ExitStatus.FAILED.getExitCode().equals(exitStatus.getExitCode())) {
            failTheWorkflowAndUpdateCommandTable(jobExecution);
        }
    }

    private void failTheWorkflowAndUpdateCommandTable(JobExecution jobExecution) {
        log.info("Failing the workflow ...");
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
        YarnClient yarnClient = YarnClient.createYarnClient();
        yarnClient.init(yarnConfiguration);
        yarnClient.start();

        String rootUid = jobExecution.getExecutionContext().getString(BulkMatchContextKey.ROOT_OPERATION_UID);
        List<MatchBlock> matchBlocks = matchCommandService.getByRootOperationUid(rootUid).getMatchBlocks();
        for (MatchBlock block: matchBlocks) {
            if (!YarnUtils.TERMINAL_APP_STATE.contains(block.getApplicationState())) {
                ApplicationId appId = ApplicationIdUtils.toApplicationIdObj(block.getApplicationId());
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
