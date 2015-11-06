package com.latticeengines.scoring.orchestration.service.impl;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.FinalApplicationStatus;
import org.apache.http.message.BasicNameValuePair;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import com.google.common.annotations.VisibleForTesting;
import com.latticeengines.common.exposed.util.YarnUtils;
import com.latticeengines.dataplatform.exposed.service.JobService;
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
import com.latticeengines.scoring.orchestration.service.ScoringCommandLogService;
import com.latticeengines.scoring.orchestration.service.ScoringDaemonService;
import com.latticeengines.scoring.orchestration.service.ScoringStepProcessor;
import com.latticeengines.scoring.orchestration.service.ScoringStepYarnProcessor;
import com.latticeengines.scoring.orchestration.service.ScoringValidationService;

@Component("scoringProcessor")
@Scope("prototype")
public class ScoringProcessorCallable implements Callable<Long> {

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
    private ScoringValidationService scoringValidationService;

    @Autowired
    private JobService jobService;

    @Autowired
    private AlertService alertService;

    @SuppressWarnings("unused")
    private Configuration yarnConfiguration;

    @Value("${dataplatform.yarn.timeline-service.webapp.address}")
    private String appTimeLineWebAppAddress;

    private static final Log log = LogFactory.getLog(ScoringProcessorCallable.class);

    public ScoringProcessorCallable() {
    }

    public void setScoringCommand(ScoringCommand scoringCommand) {
        this.scoringCommand = scoringCommand;
    }

    public void setAlertService(AlertService alertService) {
        this.alertService = alertService;
    }

    @Override
    public Long call() throws Exception {
        int result = ScoringDaemonService.SUCCESS;
        try {
            log.info("Begin scheduled work on " + ScoringCommandLogServiceImpl.SCORINGCOMMAND_ID_LOG_PREFIX + ":"
                    + this.scoringCommand.getPid()); // Need this line to
                                                     // associate
                                                     // scoringCommandId with
                                                     // threadId in log4j
                                                     // output.
            this.executeWorkflow();
            log.info("End scheduled work on " + ScoringCommandLogServiceImpl.SCORINGCOMMAND_ID_LOG_PREFIX + ":"
                    + this.scoringCommand.getPid());
        } catch (LedpException e) {
            result = ScoringDaemonService.FAIL;
            this.scoringCommandLogService.logLedpException(this.scoringCommand, e);
        } catch (Exception e) {
            result = ScoringDaemonService.FAIL;
            this.scoringCommandLogService.logException(this.scoringCommand, e);
        } finally {
            if (result == ScoringDaemonService.FAIL) {
                this.handleJobFailed();
            }
        }
        return this.scoringCommand.getPid();
    }

    private void executeWorkflow() {
        ScoringCommandState scoringCommandState = this.scoringCommandStateEntityMgr
                .findLastStateByScoringCommand(this.scoringCommand);
        if (scoringCommandState == null) {
            this.scoringCommandLogService.log(this.scoringCommand, "Total: " + this.scoringCommand.getTotal());
            scoringValidationService.validateBeforeProcessing(scoringCommand);
            this.executeYarnStep(ScoringCommandStep.LOAD_DATA);
        } else { // scoringCommand IN_PROGRESS
            String yarnApplicationId;
            if ((yarnApplicationId = scoringCommandState.getYarnApplicationId()) == null) {
                return;
            }
            JobStatus jobStatus = this.jobService.getJobStatus(yarnApplicationId);
            this.saveScoringCommandStateFromJobStatus(scoringCommandState, jobStatus);
            if (jobStatus.getStatus().equals(FinalApplicationStatus.SUCCEEDED)) {
                this.handleAllJobsSucceeded();
            } else if (jobStatus.getStatus().equals(FinalApplicationStatus.UNDEFINED)
                    || YarnUtils.isPrempted(jobStatus.getDiagnostics())) {
                // Job in progress.
            } else if (jobStatus.getStatus().equals(FinalApplicationStatus.KILLED)
                    || jobStatus.getStatus().equals(FinalApplicationStatus.FAILED)) {
                this.handleJobFailed(yarnApplicationId);
            }
        }
    }

    private void executeStep(ScoringStepProcessor scoringCommandProcessor, ScoringCommandStep scoringCommandStep) {
        long start = System.currentTimeMillis();
        ScoringCommandState scoringCommandState = this.initExecutionStep(scoringCommandStep);

        scoringCommandProcessor.executeStep(this.scoringCommand);

        scoringCommandState.setElapsedTimeInMillis(System.currentTimeMillis() - start);
        scoringCommandState.setStatus(FinalApplicationStatus.SUCCEEDED);
        this.scoringCommandStateEntityMgr.update(scoringCommandState);
        this.scoringCommandLogService.logCompleteStep(this.scoringCommand, scoringCommandStep,
                ScoringCommandStatus.SUCCESS);
    }

    private void executeYarnStep(ScoringCommandStep scoringCommandStep) {
        ScoringCommandState scoringCommandState = this.initExecutionStep(scoringCommandStep);

        ApplicationId appId = this.scoringStepYarnProcessor.executeYarnStep(this.scoringCommand, scoringCommandStep);

        String appIdString = appId.toString();
        this.scoringCommandLogService.logYarnAppId(this.scoringCommand, appIdString, scoringCommandStep);
        JobStatus jobStatus = this.jobService.getJobStatus(appIdString);
        this.saveScoringCommandStateFromJobStatus(this.scoringCommandStateEntityMgr.findByKey(scoringCommandState),
                jobStatus);

    }

    private ScoringCommandState initExecutionStep(ScoringCommandStep scoringCommandStep) {
        this.scoringCommandLogService.logBeginStep(this.scoringCommand, scoringCommandStep);
        ScoringCommandState scoringCommandState = new ScoringCommandState(this.scoringCommand, scoringCommandStep);
        scoringCommandState.setStatus(FinalApplicationStatus.UNDEFINED);
        this.scoringCommandStateEntityMgr.create(scoringCommandState);
        return scoringCommandState;
    }

    private void saveScoringCommandStateFromJobStatus(ScoringCommandState scoringCommandState, JobStatus jobStatus) {
        scoringCommandState.setYarnApplicationId(jobStatus.getId());
        scoringCommandState.setStatus(jobStatus.getStatus());
        scoringCommandState.setProgress(jobStatus.getProgress());
        scoringCommandState.setDiagnostics(jobStatus.getDiagnostics());
        scoringCommandState.setTrackingUrl(jobStatus.getTrackingUrl());
        scoringCommandState.setElapsedTimeInMillis(System.currentTimeMillis() - jobStatus.getStartTime());
        this.scoringCommandStateEntityMgr.update(scoringCommandState);
    }

    private void handleAllJobsSucceeded() {
        ScoringCommandState scoringCommandState = this.scoringCommandStateEntityMgr
                .findLastStateByScoringCommand(this.scoringCommand);
        this.scoringCommandLogService.logCompleteStep(this.scoringCommand, scoringCommandState.getScoringCommandStep(),
                ScoringCommandStatus.SUCCESS);
        ScoringCommandStep nextScoringCommandStep = scoringCommandState.getScoringCommandStep().getNextStep();
        if (nextScoringCommandStep.equals(ScoringCommandStep.FINISH)) {
            this.executeStep(this.scoringStepFinishProcessor, ScoringCommandStep.FINISH);
        } else {
            this.executeYarnStep(nextScoringCommandStep);
        }
    }

    @VisibleForTesting
    String handleJobFailed() {
        return this.handleJobFailed(null);
    }

    @VisibleForTesting
    String handleJobFailed(String failedYarnApplicationId) {
        ScoringCommandState scoringCommandState = this.scoringCommandStateEntityMgr
                .findLastStateByScoringCommand(this.scoringCommand);
        this.setJobFailed(scoringCommandState);
        if (scoringCommandState != null) {
            StringBuilder clientUrl = new StringBuilder(this.appTimeLineWebAppAddress);
            if (failedYarnApplicationId != null) {
                clientUrl.append("/app/").append(failedYarnApplicationId);
                this.scoringCommandLogService.log(this.scoringCommand, "Failed job link: " + clientUrl.toString());
            }

            List<BasicNameValuePair> details = new ArrayList<>();
            details.add(new BasicNameValuePair("scoringCommandId", this.scoringCommand.getPid().toString()));
            details.add(new BasicNameValuePair("yarnAppId", failedYarnApplicationId == null ? "None" : ""));
            details.add(new BasicNameValuePair("deploymentExternalId", this.scoringCommand.getId()));
            details.add(new BasicNameValuePair("failedStep", scoringCommandState.getScoringCommandStep()
                    .getDescription()));
            List<ScoringCommandLog> logs = this.scoringCommandLogService.findByScoringCommand(this.scoringCommand);
            if (!logs.isEmpty()) {
                for (ScoringCommandLog scoringCommandLog : logs) {
                    details.add(new BasicNameValuePair("scoringCommandLogId" + scoringCommandLog.getPid(),
                            scoringCommandLog.getMessage()));
                }
            }

            return this.alertService.triggerCriticalEvent(LedpCode.LEDP_20000.getMessage(), clientUrl.toString(),
                    details);
        } else {
            return "";
        }
    }

    private void setJobFailed(ScoringCommandState scoringCommandState) {
        if (scoringCommandState != null) {
            this.scoringCommandLogService.logCompleteStep(this.scoringCommand,
                    scoringCommandState.getScoringCommandStep(), ScoringCommandStatus.FAIL);
            scoringCommandState.setStatus(FinalApplicationStatus.FAILED);
            this.scoringCommandStateEntityMgr.update(scoringCommandState);
        }

        this.scoringCommand.setStatus(ScoringCommandStatus.CONSUMED);
        this.scoringCommandEntityMgr.update(this.scoringCommand);
    }
}
