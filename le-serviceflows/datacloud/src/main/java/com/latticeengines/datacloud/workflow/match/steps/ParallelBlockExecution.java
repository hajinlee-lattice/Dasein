package com.latticeengines.datacloud.workflow.match.steps;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.TimeUnit;

import org.apache.avro.Schema;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang.time.DurationFormatUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationReport;
import org.apache.hadoop.yarn.api.records.FinalApplicationStatus;
import org.apache.hadoop.yarn.api.records.YarnApplicationState;
import org.apache.hadoop.yarn.client.api.YarnClient;
import org.apache.hadoop.yarn.util.ConverterUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.latticeengines.common.exposed.util.AvroUtils;
import com.latticeengines.common.exposed.util.HdfsUtils;
import com.latticeengines.common.exposed.util.YarnUtils;
import com.latticeengines.datacloud.core.service.ZkConfigurationService;
import com.latticeengines.datacloud.core.util.HdfsPathBuilder;
import com.latticeengines.datacloud.core.util.HdfsPodContext;
import com.latticeengines.datacloud.match.exposed.service.MatchCommandService;
import com.latticeengines.datacloud.match.exposed.util.MatchUtils;
import com.latticeengines.domain.exposed.datacloud.DataCloudJobConfiguration;
import com.latticeengines.domain.exposed.datacloud.manage.MatchCommand;
import com.latticeengines.domain.exposed.datacloud.match.MatchOutput;
import com.latticeengines.domain.exposed.datacloud.match.MatchStatus;
import com.latticeengines.domain.exposed.serviceflows.datacloud.match.steps.ParallelBlockExecutionConfiguration;
import com.latticeengines.proxy.exposed.matchapi.MatchInternalProxy;
import com.latticeengines.serviceflows.workflow.core.BaseWorkflowStep;

@Component("parallelBlockExecution")
@Scope("prototype")
public class ParallelBlockExecution extends BaseWorkflowStep<ParallelBlockExecutionConfiguration> {

    private static Log log = LogFactory.getLog(ParallelBlockExecution.class);
    private static final int MAX_ERRORS = 100;
    private static final Long MATCH_TIMEOUT = TimeUnit.DAYS.toMillis(3);
    private static final String MATCHOUTPUT_BUFFER_FILE = "matchoutput.json";

    @Autowired
    private MatchInternalProxy matchInternalProxy;

    @Autowired
    private MatchCommandService matchCommandService;

    @Autowired
    private ZkConfigurationService zkConfigurationService;

    @Autowired
    private HdfsPathBuilder hdfsPathBuilder;

    private YarnClient yarnClient;
    private String rootOperationUid;
    private List<ApplicationId> applicationIds = new ArrayList<>();
    private Map<String, String> blockUuidMap = new HashMap<>();
    private int numErrors = 0;
    private float progress;
    private Long progressUpdated;
    private MatchOutput matchOutput;
    private Random random = new Random(System.currentTimeMillis());
    private List<DataCloudJobConfiguration> jobConfigurations = new ArrayList<>();

    @Override
    public void execute() {
        try {
            log.info("Inside ParallelBlockExecution execute()");
            initializeYarnClient();

            Object listObj = executionContext.get(BulkMatchContextKey.YARN_JOB_CONFIGS);
            if (listObj instanceof List) {
                @SuppressWarnings("rawtypes")
                List list = (List) listObj;
                for (Object configObj : list) {
                    if (configObj instanceof DataCloudJobConfiguration) {
                        jobConfigurations.add((DataCloudJobConfiguration) configObj);
                    }
                }
            }
            HdfsPodContext.changeHdfsPodId(getConfiguration().getPodId());

            rootOperationUid = getStringValueFromContext(BulkMatchContextKey.ROOT_OPERATION_UID);
            submitMatchBlocks(jobConfigurations);
            waitForMatchBlocks();
            finalizeMatch();
        } catch (Exception e) {
            failTheWorkflowWithErrorMessage(e.getMessage(), e);
        }
    }

    @Override
    public void onExecutionCompleted() {
        putObjectInContext(MATCH_COMMAND, matchCommandService.getByRootOperationUid(rootOperationUid));
    }

    private void submitMatchBlocks(List<DataCloudJobConfiguration> jobConfigurations) {
        applicationIds = new ArrayList<>();
        putObjectInContext(BulkMatchContextKey.APPLICATION_IDS, applicationIds);
        for (DataCloudJobConfiguration jobConfiguration : jobConfigurations) {
            ApplicationId appId = ConverterUtils.toApplicationId(matchInternalProxy.submitYarnJob(jobConfiguration)
                    .getApplicationIds().get(0));
            blockUuidMap.put(appId.toString(), jobConfiguration.getBlockOperationUid());
            applicationIds.add(appId);

            MatchCommand matchCommand = matchCommandService.getByRootOperationUid(rootOperationUid);
            matchCommandService.startBlock(matchCommand, appId, jobConfiguration.getBlockOperationUid(),
                    jobConfiguration.getBlockSize());
            log.info("Submit a match block to application id " + appId);
        }
    }

    private void waitForMatchBlocks() {
        progressUpdated = System.currentTimeMillis();
        try {
            while (!applicationIds.isEmpty()) {
                List<ApplicationReport> reports = gatherApplicationReports();
                if (reports != null) {
                    updateProgress(reports);
                    checkTerminatedApplications(reports);
                }
                try {
                    Thread.sleep(10000L);
                } catch (InterruptedException e) {
                    // ignore
                }
            }
        } finally {
            yarnClient.stop();
        }
    }

    private void finalizeMatch() {
        try {
            matchCommandService.update(rootOperationUid).status(MatchStatus.FINISHING).progress(0.98f)
                    .commit();

            Long startTime = matchOutput.getReceivedAt().getTime();
            matchOutput.getStatistics().setTimeElapsedInMsec(System.currentTimeMillis() - startTime);

            try {
                String outputFile = hdfsPathBuilder.constructMatchOutputFile(rootOperationUid).toString();
                ObjectMapper mapper = new ObjectMapper();
                mapper.writeValue(new File(MATCHOUTPUT_BUFFER_FILE), matchOutput);
                HdfsUtils.copyFromLocalToHdfs(yarnConfiguration, MATCHOUTPUT_BUFFER_FILE, outputFile);
                FileUtils.deleteQuietly(new File(MATCHOUTPUT_BUFFER_FILE));
            } catch (Exception e) {
                log.error("Failed to save matchoutput json.", e);
            }

            String avroDir = hdfsPathBuilder.constructMatchOutputDir(rootOperationUid).toString();

            if (!StringUtils.isEmpty(configuration.getResultLocation())) {
                avroDir = configuration.getResultLocation();
            }
            Long count = AvroUtils.count(yarnConfiguration, MatchUtils.toAvroGlobs(avroDir));
            log.info("Generated " + count + " results in " + MatchUtils.toAvroGlobs(avroDir));
            matchCommandService.update(rootOperationUid) //
                    .resultLocation(avroDir) //
                    .dnbCommands() //
                    .rowsMatched(count.intValue()) //
                    .status(MatchStatus.FINISHED) //
                    .progress(1f) //
                    .commit();
        } catch (Exception e) {
            String errorMessage = "Failed to finalize the match: " + e.getMessage();
            throw new RuntimeException(errorMessage, e);
        }
    }

    private List<ApplicationReport> gatherApplicationReports() {
        List<ApplicationReport> reports = new ArrayList<>();
        for (ApplicationId appId : applicationIds) {
            try {
                ApplicationReport report = yarnClient.getApplicationReport(appId);
                reports.add(report);
            } catch (Exception e) {
                numErrors++;
                log.error("Failed to read status of application " + appId, e);
                if (numErrors > MAX_ERRORS) {
                    throw new RuntimeException("Exceeded maximum number of errors " + MAX_ERRORS);
                } else {
                    return null;
                }
            }
        }
        return reports;
    }

    private void updateProgress(List<ApplicationReport> reports) {
        Float totalProgress = 0f;
        for (ApplicationReport report : reports) {
            YarnApplicationState state = report.getYarnApplicationState();
            ApplicationId appId = report.getApplicationId();
            Float appProgress = report.getProgress();
            String logMessage = String.format("Application [%s] is at state [%s]", appId, state);
            if (YarnApplicationState.RUNNING.equals(state)) {
                logMessage += String.format(": %.2f ", appProgress * 100) + "%";
                if (hitThirtyPercentChance()) {
                    String blockId = blockUuidMap.get(appId.toString());
                    matchCommandService.updateBlock(blockId).status(state).progress(appProgress).commit();
                }
            }
            log.info(logMessage);

            if (YarnUtils.TERMINAL_APP_STATE.contains(state)) {
                FinalApplicationStatus status = report.getFinalApplicationStatus();
                log.info("Application [" + appId + "] is at the terminal state " + state + " with final status "
                        + status + ". Taking it out of the running matcher list.");
                applicationIds.remove(appId);
                log.info("Remaining matches are " + applicationIds);
            }

            totalProgress += appProgress;
        }
        // add full progress of finished applications
        totalProgress += 1.0f * (blockUuidMap.size() - reports.size());

        Float overallProgress = 0.9f * (totalProgress / blockUuidMap.size()) + 0.05f;
        if (overallProgress != this.progress) {
            this.progress = overallProgress;
            Long currentTimestamp = System.currentTimeMillis();
            if (currentTimestamp - progressUpdated > MATCH_TIMEOUT) {
                String errorMsg = "The match has been hanging for "
                        + DurationFormatUtils.formatDurationWords(currentTimestamp - progressUpdated, true, false)
                        + ".";
                throw new RuntimeException(errorMsg);
            } else {
                this.progressUpdated = currentTimestamp;
            }
        }
        if (hitThirtyPercentChance()) {
            matchCommandService.update(rootOperationUid).progress(this.progress).commit();
        }
        log.info(String.format("Overall progress is %.2f ", this.progress * 100) + "%.");
    }

    private void checkTerminatedApplications(List<ApplicationReport> reports) {
        for (ApplicationReport report : reports) {
            YarnApplicationState state = report.getYarnApplicationState();
            ApplicationId appId = report.getApplicationId();
            String blockUid = blockUuidMap.get(appId.toString());
            if (YarnUtils.TERMINAL_APP_STATE.contains(state)) {
                FinalApplicationStatus status = report.getFinalApplicationStatus();
                log.info("Application [" + appId + "] is at the terminal state " + state + " with final status "
                        + status);
                applicationIds.remove(appId);
                log.info("Took it out of the running matcher list. Remaining matches are " + applicationIds);

                if (FinalApplicationStatus.FAILED.equals(status) || FinalApplicationStatus.KILLED.equals(status)) {
                    handleAbnormallyTerminatedBlock(report);
                } else if (FinalApplicationStatus.SUCCEEDED.equals(status)) {
                    matchCommandService.updateBlock(blockUid).status(state).progress(1f).commit();
                    mergeBlockResult(appId);
                } else {
                    log.error("Unknown teminal status " + status + " for Application [" + appId
                            + "]. Treat it as FAILED.");
                    failTheWorkflowByFailedBlock(MatchStatus.FAILED, report);
                }

            }
        }
    }

    private void handleAbnormallyTerminatedBlock(ApplicationReport report) {
        ApplicationId appId = report.getApplicationId();
        FinalApplicationStatus status = report.getFinalApplicationStatus();
        String errorMsg = "Application [" + appId + "] ended abnormally with the final status " + status;

        String blockUid = blockUuidMap.get(appId.toString());
        if (doRetry(blockUid)) {
            log.info(errorMsg + ". Retry the block.");
            for (DataCloudJobConfiguration jobConfiguration : jobConfigurations) {
                if (jobConfiguration.getBlockOperationUid().equals(blockUid)) {
                    ApplicationId newAppId = ConverterUtils.toApplicationId(matchInternalProxy
                            .submitYarnJob(jobConfiguration).getApplicationIds().get(0));
                    blockUuidMap.remove(appId.toString());
                    blockUuidMap.put(newAppId.toString(), jobConfiguration.getBlockOperationUid());
                    applicationIds.add(newAppId);
                    matchCommandService.retryBlock(blockUid, newAppId);
                    log.info("Submit a match block to application id " + newAppId);
                }
            }
        } else {
            log.warn(errorMsg + ". Killing the whole match");
            MatchStatus terminalStatus = FinalApplicationStatus.FAILED.equals(status) ? MatchStatus.FAILED
                    : MatchStatus.ABORTED;
            failTheWorkflowByFailedBlock(terminalStatus, report);
        }
    }

    private Boolean doRetry(String blockUid) {
        boolean useRemoteDnB = false;
        DataCloudJobConfiguration jobConfiguration = jobConfigurations.get(0);
        if (jobConfiguration.getMatchInput().getUseRemoteDnB() != null) {
            useRemoteDnB = jobConfiguration.getMatchInput().getUseRemoteDnB();
        } else {
            useRemoteDnB = zkConfigurationService.fuzzyMatchEnabled(jobConfiguration.getCustomerSpace());
        }
        useRemoteDnB = useRemoteDnB
                && MatchUtils.isValidForAccountMasterBasedMatch(jobConfiguration.getMatchInput().getDataCloudVersion());
        if (useRemoteDnB) {
            return false;
        }
        return matchCommandService.blockIsRetriable(blockUid);
    }

    private void failTheWorkflowByFailedBlock(MatchStatus terminalStatus, ApplicationReport failedReport) {
        ApplicationId failedAppId = failedReport.getApplicationId();
        String blockOperationUid = blockUuidMap.get(failedAppId.toString());
        String blockErrorFile = hdfsPathBuilder.constructMatchBlockErrorFile(rootOperationUid, blockOperationUid)
                .toString();
        String errorMsg = "Match container [" + failedAppId + "] failed.";
        try {
            String blockError = HdfsUtils.getHdfsFileContents(yarnConfiguration, blockErrorFile);
            String matchErrorFile = hdfsPathBuilder.constructMatchErrorFile(rootOperationUid).toString();
            HdfsUtils.writeToFile(yarnConfiguration, matchErrorFile, errorMsg + "\n" + blockError);
            errorMsg += blockError.split("\n")[0];
        } catch (Exception e) {
            log.error("Failed to read the error for matcher " + blockOperationUid + " in application " + failedAppId
                    + " : " + e.getMessage());
        }
        matchCommandService.update(rootOperationUid) //
                .status(terminalStatus) //
                .errorMessage(errorMsg) //
                .commit();
        throw new RuntimeException("Match failed. " + errorMsg);
    }

    private void failTheWorkflowWithErrorMessage(String errorMsg, Exception ex) {
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

        if (ex != null) {
            throw new RuntimeException("Match failed: " + errorMsg, ex);
        } else {
            throw new RuntimeException("Match failed: " + errorMsg);
        }
    }

    private void mergeBlockResult(ApplicationId appId) {
        String blockOperationUid = blockUuidMap.get(appId.toString());
        String blockJsonPath = hdfsPathBuilder.constructMatchBlockOutputFile(rootOperationUid, blockOperationUid)
                .toString();

        try {
            ObjectMapper mapper = new ObjectMapper();
            String content = HdfsUtils.getHdfsFileContents(yarnConfiguration, blockJsonPath);
            MatchOutput blockMatchOutput = mapper.readValue(content, MatchOutput.class);
            if (blockMatchOutput == null) {
                throw new IOException("MatchOutput is null for the block in application " + appId);
            }
            matchOutput = MatchUtils.mergeOutputs(matchOutput, blockMatchOutput);
        } catch (Exception e) {
            String errorMsg = "Failed to read match output file for block in application " + appId + " : "
                    + e.getMessage();
            throw new RuntimeException(errorMsg, e);
        }

        try {
            String matchAvsc = hdfsPathBuilder.constructMatchSchemaFile(rootOperationUid).toString();
            if (!HdfsUtils.fileExists(yarnConfiguration, matchAvsc)) {
                String blockAvroGlob = hdfsPathBuilder.constructMatchBlockAvroGlob(rootOperationUid, blockOperationUid);
                Schema schema = AvroUtils.getSchemaFromGlob(yarnConfiguration, blockAvroGlob);
                HdfsUtils.writeToFile(yarnConfiguration, matchAvsc, schema.toString());
            }
        } catch (Exception e) {
            throw new RuntimeException("Failed to write schema file: " + e.getMessage(), e);
        }

        try {
            String blockAvroGlob = hdfsPathBuilder.constructMatchBlockAvroGlob(rootOperationUid, blockOperationUid);
            String matchOutputDir = hdfsPathBuilder.constructMatchOutputDir(rootOperationUid).toString();

            if (!StringUtils.isEmpty(configuration.getResultLocation())) {
                matchOutputDir = configuration.getResultLocation();
            }

            if (!HdfsUtils.fileExists(yarnConfiguration, matchOutputDir)) {
                HdfsUtils.mkdir(yarnConfiguration, matchOutputDir);
            }
            HdfsUtils.copyGlobToDir(yarnConfiguration, blockAvroGlob, matchOutputDir);
        } catch (Exception e) {
            throw new RuntimeException("Failed to copy block avro generated by application " + appId
                    + " to match output dir: " + e.getMessage(), e);
        }

    }

    private Boolean hitThirtyPercentChance() {
        return random.nextInt(100) < 30;
    }

    private void initializeYarnClient() {
        yarnClient = YarnClient.createYarnClient();
        yarnClient.init(yarnConfiguration);
        yarnClient.start();
    }

}
