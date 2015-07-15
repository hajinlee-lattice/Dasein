package com.latticeengines.scoring.service.impl;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;

import javax.annotation.Resource;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.FinalApplicationStatus;
import org.apache.http.message.BasicNameValuePair;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Scope;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Component;

import com.google.common.annotations.VisibleForTesting;
import com.latticeengines.common.exposed.util.YarnUtils;
import com.latticeengines.dataplatform.exposed.service.JobService;
import com.latticeengines.dataplatform.exposed.service.MetadataService;
import com.latticeengines.domain.exposed.dataplatform.JobStatus;
import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.domain.exposed.scoring.ScoringCommand;
import com.latticeengines.domain.exposed.scoring.ScoringCommandLog;
import com.latticeengines.domain.exposed.scoring.ScoringCommandState;
import com.latticeengines.domain.exposed.scoring.ScoringCommandStatus;
import com.latticeengines.domain.exposed.scoring.ScoringCommandStep;
import com.latticeengines.monitor.exposed.alerts.service.AlertService;
import com.latticeengines.scoring.entitymanager.ScoringCommandEntityMgr;
import com.latticeengines.scoring.entitymanager.ScoringCommandStateEntityMgr;
import com.latticeengines.scoring.service.ScoringCommandLogService;
import com.latticeengines.scoring.service.ScoringStepProcessor;
import com.latticeengines.scoring.service.ScoringStepYarnProcessor;

@Component("scoringProcessor")
@Scope("prototype")
public class ScoringProcessorCallable implements Callable<Long> {

    private static final int SUCCESS = 0;
    private static final int FAIL = -1;

    private ScoringCommand scoringCommand;

    @Autowired
    private ScoringCommandEntityMgr scoringCommandEntityMgr;

    @Autowired
    private ScoringCommandLogService scoringCommandLogService;

    @Autowired
    private ScoringCommandStateEntityMgr scoringCommandStateEntityMgr;

    @Autowired
    private ScoringStepYarnProcessor scoringStepYarnProcessor;

    @Autowired
    private ScoringStepProcessor scoringStepFinishProcessor;

    @Autowired
    private JobService jobService;

    @Resource(name = "modelingAlertService")
    private AlertService alertService;

    @SuppressWarnings("unused")
    private Configuration yarnConfiguration;

    @Value("${dataplatform.yarn.timeline-service.webapp.address}")
    private String appTimeLineWebAppAddress;

    @Autowired
    private MetadataService metadataService;

    @Autowired
    private JdbcTemplate scoringJdbcTemplate;

    private static final Log log = LogFactory.getLog(ScoringProcessorCallable.class);

    public ScoringProcessorCallable() {
    }

    public void setScoringCommand(ScoringCommand scoringCommand) {
        this.scoringCommand = scoringCommand;
    }

    @Override
    public Long call() throws Exception {
        int result = SUCCESS;
        try {
            log.info("Begin scheduled work on " + ScoringCommandLogServiceImpl.SCORINGCOMMAND_ID_LOG_PREFIX + ":"
                    + scoringCommand.getPid()); // Need this line to associate
                                                // scoringCommandId with
                                                // threadId
                                                // in
                                                // log4j output.
            executeWorkflow();
            log.info("End scheduled work on " + ScoringCommandLogServiceImpl.SCORINGCOMMAND_ID_LOG_PREFIX + ":"
                    + scoringCommand.getPid());
        } catch (LedpException e) {
            result = FAIL;
            scoringCommandLogService.logLedpException(scoringCommand, e);
        } catch (Exception e) {
            result = FAIL;
            scoringCommandLogService.logException(scoringCommand, e);
        } finally {
            if (result == FAIL) {
                handleJobFailed();
            }
        }
        return scoringCommand.getPid();
    }

    private void executeWorkflow() {
        ScoringCommandState scoringCommandState = scoringCommandStateEntityMgr
                .findLastStateByScoringCommand(scoringCommand);
        if (scoringCommandState == null) {
            scoringCommandLogService.log(scoringCommand, "Total: " + scoringCommand.getTotal());
            long total = metadataService.getRowCount(scoringJdbcTemplate, scoringCommand.getTableName());
            if (total != scoringCommand.getTotal()) {
                throw new LedpException(LedpCode.LEDP_200016);
            }if(total < 1)
                throw new LedpException(LedpCode.LEDP_200017);
            executeYarnStep(ScoringCommandStep.LOAD_DATA);
        } else { // scoringCommand IN_PROGRESS
            String yarnApplicationId;
            if ((yarnApplicationId = scoringCommandState.getYarnApplicationId()) == null)
                return;
            JobStatus jobStatus = jobService.getJobStatus(yarnApplicationId);
            saveScoringCommandStateFromJobStatus(scoringCommandState, jobStatus);
            if (jobStatus.getStatus().equals(FinalApplicationStatus.SUCCEEDED)) {
                handleAllJobsSucceeded();
            } else if (jobStatus.getStatus().equals(FinalApplicationStatus.UNDEFINED)
                    || YarnUtils.isPrempted(jobStatus.getDiagnostics())) {
                // Job in progress.
            } else if (jobStatus.getStatus().equals(FinalApplicationStatus.KILLED)
                    || jobStatus.getStatus().equals(FinalApplicationStatus.FAILED)) {
                handleJobFailed(yarnApplicationId);
            }
        }
    }

    private void executeStep(ScoringStepProcessor scoringCommandProcessor, ScoringCommandStep scoringCommandStep) {
        long start = System.currentTimeMillis();
        ScoringCommandState scoringCommandState = initExecutionStep(scoringCommandStep);

        scoringCommandProcessor.executeStep(scoringCommand);

        scoringCommandState.setElapsedTimeInMillis(System.currentTimeMillis() - start);
        scoringCommandState.setStatus(FinalApplicationStatus.SUCCEEDED);
        scoringCommandStateEntityMgr.update(scoringCommandState);
        scoringCommandLogService.logCompleteStep(scoringCommand, scoringCommandStep, ScoringCommandStatus.SUCCESS);
    }

    private void executeYarnStep(ScoringCommandStep scoringCommandStep) {
        ScoringCommandState scoringCommandState = initExecutionStep(scoringCommandStep);

        ApplicationId appId = scoringStepYarnProcessor.executeYarnStep(scoringCommand, scoringCommandStep);

        String appIdString = appId.toString();
        scoringCommandLogService.logYarnAppId(scoringCommand, appIdString, scoringCommandStep);
        JobStatus jobStatus = jobService.getJobStatus(appIdString);
        saveScoringCommandStateFromJobStatus(scoringCommandStateEntityMgr.findByKey(scoringCommandState), jobStatus);

    }

    private ScoringCommandState initExecutionStep(ScoringCommandStep scoringCommandStep) {
        scoringCommandLogService.logBeginStep(scoringCommand, scoringCommandStep);
        ScoringCommandState scoringCommandState = new ScoringCommandState(scoringCommand, scoringCommandStep);
        scoringCommandState.setStatus(FinalApplicationStatus.UNDEFINED);
        scoringCommandStateEntityMgr.create(scoringCommandState);
        return scoringCommandState;
    }

    private void saveScoringCommandStateFromJobStatus(ScoringCommandState scoringCommandState, JobStatus jobStatus) {
        scoringCommandState.setYarnApplicationId(jobStatus.getId());
        scoringCommandState.setStatus(jobStatus.getStatus());
        scoringCommandState.setProgress(jobStatus.getProgress());
        scoringCommandState.setDiagnostics(jobStatus.getDiagnostics());
        scoringCommandState.setTrackingUrl(jobStatus.getTrackingUrl());
        scoringCommandState.setElapsedTimeInMillis(System.currentTimeMillis() - jobStatus.getStartTime());
        scoringCommandStateEntityMgr.update(scoringCommandState);
    }

    private void handleAllJobsSucceeded() {
        ScoringCommandState scoringCommandState = scoringCommandStateEntityMgr
                .findLastStateByScoringCommand(scoringCommand);
        scoringCommandLogService.logCompleteStep(scoringCommand, scoringCommandState.getScoringCommandStep(),
                ScoringCommandStatus.SUCCESS);
        ScoringCommandStep nextScoringCommandStep = scoringCommandState.getScoringCommandStep().getNextStep();
        if (nextScoringCommandStep.equals(ScoringCommandStep.FINISH)) {
            executeStep(scoringStepFinishProcessor, ScoringCommandStep.FINISH);
        } else {
            executeYarnStep(nextScoringCommandStep);
        }
    }

    @VisibleForTesting
    String handleJobFailed() {
        return handleJobFailed(null);
    }

    @VisibleForTesting
    String handleJobFailed(String failedYarnApplicationId) {
        ScoringCommandState scoringCommandState = scoringCommandStateEntityMgr
                .findLastStateByScoringCommand(scoringCommand);
        setJobFailed(scoringCommandState);
        if (scoringCommandState != null) {
            StringBuilder clientUrl = new StringBuilder(appTimeLineWebAppAddress);
            if (failedYarnApplicationId != null) {
                clientUrl.append("/app/").append(failedYarnApplicationId);
                scoringCommandLogService.log(scoringCommand, "Failed job link: " + clientUrl.toString());
            }

            List<BasicNameValuePair> details = new ArrayList<>();
            details.add(new BasicNameValuePair("scoringCommandId", scoringCommand.getPid().toString()));
            details.add(new BasicNameValuePair("yarnAppId", failedYarnApplicationId == null ? "None" : ""));
            details.add(new BasicNameValuePair("deploymentExternalId", scoringCommand.getId()));
            details.add(new BasicNameValuePair("failedStep", scoringCommandState.getScoringCommandStep()
                    .getDescription()));
            List<ScoringCommandLog> logs = scoringCommandLogService.findByScoringCommand(scoringCommand);
            if (!logs.isEmpty()) {
                for (ScoringCommandLog scoringCommandLog : logs) {
                    details.add(new BasicNameValuePair("scoringCommandLogId" + scoringCommandLog.getPid(), scoringCommandLog
                            .getMessage()));
                }
            }

            return alertService.triggerCriticalEvent(LedpCode.LEDP_20000.getMessage(), clientUrl.toString(), details);
        } else
            return "";
    }

    private void setJobFailed(ScoringCommandState scoringCommandState) {
        if (scoringCommandState != null) {
            scoringCommandLogService.logCompleteStep(scoringCommand, scoringCommandState.getScoringCommandStep(),
                    ScoringCommandStatus.FAIL);
            scoringCommandState.setStatus(FinalApplicationStatus.FAILED);
            scoringCommandStateEntityMgr.update(scoringCommandState);
        }

        scoringCommand.setStatus(ScoringCommandStatus.CONSUMED);
        scoringCommandEntityMgr.update(scoringCommand);
    }
}
