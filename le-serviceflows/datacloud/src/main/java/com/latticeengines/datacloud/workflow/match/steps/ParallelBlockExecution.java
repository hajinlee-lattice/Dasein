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
import org.apache.commons.collections4.MapUtils;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.ObjectUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.time.DurationFormatUtils;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationReport;
import org.apache.hadoop.yarn.api.records.FinalApplicationStatus;
import org.apache.hadoop.yarn.api.records.YarnApplicationState;
import org.apache.hadoop.yarn.client.api.YarnClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.latticeengines.common.exposed.util.AvroUtils;
import com.latticeengines.common.exposed.util.HdfsUtils;
import com.latticeengines.common.exposed.util.YarnUtils;
import com.latticeengines.datacloud.core.util.HdfsPathBuilder;
import com.latticeengines.datacloud.core.util.HdfsPodContext;
import com.latticeengines.datacloud.match.exposed.service.MatchCommandService;
import com.latticeengines.datacloud.match.exposed.util.MatchUtils;
import com.latticeengines.datacloud.match.util.EntityMatchUtils;
import com.latticeengines.domain.exposed.datacloud.DataCloudJobConfiguration;
import com.latticeengines.domain.exposed.datacloud.manage.MatchBlock;
import com.latticeengines.domain.exposed.datacloud.manage.MatchCommand;
import com.latticeengines.domain.exposed.datacloud.match.EntityMatchResult;
import com.latticeengines.domain.exposed.datacloud.match.MatchInput;
import com.latticeengines.domain.exposed.datacloud.match.MatchOutput;
import com.latticeengines.domain.exposed.datacloud.match.MatchStatus;
import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.serviceflows.datacloud.match.steps.ParallelBlockExecutionConfiguration;
import com.latticeengines.domain.exposed.util.ApplicationIdUtils;
import com.latticeengines.domain.exposed.util.MetaDataTableUtils;
import com.latticeengines.domain.exposed.workflow.WorkflowContextConstants;
import com.latticeengines.proxy.exposed.matchapi.MatchInternalProxy;
import com.latticeengines.proxy.exposed.metadata.MetadataProxy;
import com.latticeengines.workflow.exposed.build.BaseWorkflowStep;

@Component("parallelBlockExecution")
@Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
public class ParallelBlockExecution extends BaseWorkflowStep<ParallelBlockExecutionConfiguration> {

    private static Logger log = LoggerFactory.getLogger(ParallelBlockExecution.class);
    private static final int MAX_ERRORS = 100;
    private static final Long MATCH_TIMEOUT = TimeUnit.DAYS.toMillis(3);
    private static final String MATCHOUTPUT_BUFFER_FILE = "matchoutput.json";

    @Autowired
    private MatchInternalProxy matchInternalProxy;

    @Autowired
    private MatchCommandService matchCommandService;

    @Value("${datacloud.match.block.interval.sec}")
    private int blockRampingRate;

    @Autowired
    private HdfsPathBuilder hdfsPathBuilder;

    @Autowired
    private MetadataProxy metadataProxy;

    private YarnClient yarnClient;
    private String rootOperationUid;
    private List<ApplicationId> applicationIds = new ArrayList<>();
    // Match Block ApplicationId -> Match Block UUID
    private Map<String, String> blockUuidMap = new HashMap<>();
    private int numErrors = 0;
    private float progress;
    private Long progressUpdated;
    private MatchOutput matchOutput;
    private Random random = new Random(System.currentTimeMillis());
    private List<DataCloudJobConfiguration> jobConfigurations;
    private List<DataCloudJobConfiguration> remainingJobs;
    private int totalRetries = 0;
    private String matchErrorDir;
    // directory to store all newly allocated entity list of this match
    private String matchNewEntityDir;

    @Override
    public void execute() {
        try {
            log.info("Inside ParallelBlockExecution execute()");
            initializeYarnClient();

            jobConfigurations = new ArrayList<>();
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
            matchErrorDir = hdfsPathBuilder.constructMatchErrorDir(rootOperationUid).toString();
            matchNewEntityDir = hdfsPathBuilder.constructMatchNewEntityDir(rootOperationUid).toString();
            remainingJobs = new ArrayList<DataCloudJobConfiguration>(jobConfigurations);
            while ((remainingJobs.size() != 0) || (applicationIds.size() != 0)) {
                submitMatchBlocks();
                waitForMatchBlocks();
                try {
                    Thread.sleep(10000L);
                } catch (InterruptedException e) {
                    // ignore
                }
            }
            finalizeMatch();
        } catch (Exception e) {
            failTheWorkflowWithErrorMessage(e.getMessage(), e);
        } finally {
            yarnClient.stop();
        }

    }

    @Override
    public void onExecutionCompleted() {
        putObjectInContext(MATCH_COMMAND, matchCommandService.getByRootOperationUid(rootOperationUid));
    }

    private void submitMatchBlocks() {
        Integer maxConcurrentBlocks = (Integer) executionContext.get(BulkMatchContextKey.MAX_CONCURRENT_BLOCKS);
        if (ObjectUtils.defaultIfNull(maxConcurrentBlocks, 0) == 0) {
            throw new RuntimeException("Invalid maximum concurrent number of blocks: " + maxConcurrentBlocks);
        }
        while ((remainingJobs.size() > 0) && (applicationIds.size() < maxConcurrentBlocks)) {
            DataCloudJobConfiguration jobConfiguration = remainingJobs.remove(0);
            ApplicationId appId = ApplicationIdUtils //
                    .toApplicationIdObj(matchInternalProxy.submitYarnJob(jobConfiguration).getApplicationIds().get(0));
            blockUuidMap.put(appId.toString(), jobConfiguration.getBlockOperationUid());
            applicationIds.add(appId);

            MatchCommand matchCommand = matchCommandService.getByRootOperationUid(rootOperationUid);
            matchCommandService.startBlock(matchCommand, appId, jobConfiguration.getBlockOperationUid(),
                    jobConfiguration.getBlockSize());
            log.info("Submit a match block to application id " + appId);
            try {
                log.info("Sleep for " + blockRampingRate + " seconds before submitting next block.");
                Thread.sleep(blockRampingRate * 1000);
            } catch (InterruptedException e) {
                log.warn("Waiting between block submissions was interrupted.", e);
            }
        }
    }

    private void waitForMatchBlocks() {
        progressUpdated = System.currentTimeMillis();
        try {
            List<ApplicationReport> reports = gatherApplicationReports();
            if (reports != null) {
                updateProgress(reports);
                checkTerminatedApplications(reports);
            }
        } catch (LedpException ex) {
            log.error("Match failed!", ex);
            throw ex;
        } catch (Exception ex) {
            log.warn("waitForMatchBlocks got exception!", ex);
        }
    }

    private void finalizeMatch() {
        try {
            matchCommandService.update(rootOperationUid).status(MatchStatus.FINISHING).progress(0.98f).commit();

            long startTime = matchOutput.getReceivedAt().getTime();
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
            Long count = 0L;
            List<MatchBlock> blocks = matchCommandService.getBlocks(rootOperationUid);

            long orphanedNoMatchCount = 0L;
            long orphanedUnmatchedAccountIdCount = 0L;
            long matchedByMatchKeyCount = 0L;
            long matchedByAccountIdCount = 0L;
            if (blocks != null && !blocks.isEmpty()) {
                for (MatchBlock block : blocks) {
                    count += block.getMatchedRows() != null ? block.getMatchedRows() : 0L;
                    if (MapUtils.isNotEmpty(block.getMatchResults())) {
                        Map<EntityMatchResult, Long> map = block.getMatchResults();
                        orphanedNoMatchCount += //
                                map.getOrDefault(EntityMatchResult.ORPHANED_NO_MATCH, 0L);
                        orphanedUnmatchedAccountIdCount += //
                                map.getOrDefault(EntityMatchResult.ORPHANED_UNMATCHED_ACCOUNTID, 0L);
                        matchedByMatchKeyCount += //
                                map.getOrDefault(EntityMatchResult.MATCHED_BY_MATCHKEY, 0L);
                        matchedByAccountIdCount += //
                                map.getOrDefault(EntityMatchResult.MATCHED_BY_ACCOUNTID, 0L);
                    }
                }
            }


            log.info("Match Command Statistics:");
            log.info("   Rows Matched: " + count);
            Map<EntityMatchResult, Long> entityMatchResultMap = new HashMap<>();
            if (orphanedNoMatchCount != 0L || orphanedUnmatchedAccountIdCount != 0L || matchedByMatchKeyCount != 0L
                    || matchedByAccountIdCount != 0L) {
                entityMatchResultMap.put(EntityMatchResult.ORPHANED_NO_MATCH, orphanedNoMatchCount);
                entityMatchResultMap.put(EntityMatchResult.ORPHANED_UNMATCHED_ACCOUNTID,
                        orphanedUnmatchedAccountIdCount);
                entityMatchResultMap.put(EntityMatchResult.MATCHED_BY_MATCHKEY, matchedByMatchKeyCount);
                entityMatchResultMap.put(EntityMatchResult.MATCHED_BY_ACCOUNTID, matchedByAccountIdCount);

                log.info("   MatchBlock Orphaned No Match: " + orphanedNoMatchCount);
                log.info("   MatchBlock Orphaned Unmatched Account ID: " +
                        orphanedUnmatchedAccountIdCount);
                log.info("   MatchBlock Matched By MatchKey: " + matchedByMatchKeyCount);
                log.info("   MatchBlock Matched By Account ID: " + matchedByAccountIdCount);
            }

            log.info("Aggregated statistics will be stored in " + MatchUtils.toAvroGlobs(avroDir));
            matchCommandService.update(rootOperationUid) //
                    .resultLocation(avroDir) //
                    .dnbCommands() //
                    .rowsMatched(count.intValue()) //
                    .matchResults(entityMatchResultMap) //
                    .status(MatchStatus.FINISHED) //
                    .progress(1f) //
                    .commit();
            setupErrorExport();
        } catch (Exception e) {
            String errorMessage = "Failed to finalize the match: " + e.getMessage();
            throw new RuntimeException(errorMessage, e);
        }
    }

    @SuppressWarnings("deprecation")
    private void setupErrorExport() {
        try {
            if (!AvroUtils.iterator(yarnConfiguration, matchErrorDir + "/*.avro").hasNext()) {
                putStringValueInContext(SKIP_EXPORT_DATA, "true");
                return;
            }
        } catch (Exception ex) {
            log.warn("Can not get error records' count! error=" + ex.getMessage());
            putStringValueInContext(SKIP_EXPORT_DATA, "true");
            return;
        }
        Table errorTable = MetaDataTableUtils.createTable(yarnConfiguration, "MatchError" + rootOperationUid,
                matchErrorDir);
        errorTable.getExtracts().get(0).setExtractionTimestamp(System.currentTimeMillis());
        metadataProxy.updateTable(configuration.getCustomerSpace().toString(), errorTable.getName(), errorTable);
        putStringValueInContext(EXPORT_TABLE_NAME, errorTable.getName());

        putStringValueInContext(EXPORT_INPUT_PATH, matchErrorDir);
        putStringValueInContext(EXPORT_OUTPUT_PATH, matchErrorDir + "CSV/" + errorTable.getName());
        putStringValueInContext(EXPORT_MERGE_FILE_PATH, matchErrorDir + "CSV");
        putStringValueInContext(EXPORT_MERGE_FILE_NAME, "matcherror.csv");
        saveOutputValue(WorkflowContextConstants.Outputs.POST_MATCH_ERROR_EXPORT_PATH,
                matchErrorDir + "CSV/matcherror.csv");

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

        Float overallProgress = 0.9f * (totalProgress / jobConfigurations.size()) + 0.05f;
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
                log.info("Took it out of the running matcher list. Remaining running matches are " + applicationIds);
                log.info(remainingJobs.size() + " blocks to be matched");

                if (FinalApplicationStatus.FAILED.equals(status) || FinalApplicationStatus.KILLED.equals(status)) {
                    handleAbnormallyTerminatedBlock(report);
                } else if (FinalApplicationStatus.SUCCEEDED.equals(status)) {
                    matchCommandService.updateBlock(blockUid).status(state).progress(1f).commit();
                    mergeBlockResult(appId);
                    mergeBlockErrorResult(appId);
                    mergeBlockNewEntityResult(appId);
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
                    ApplicationId newAppId = ApplicationIdUtils
                            .toApplicationIdObj(matchInternalProxy.submitYarnJob(jobConfiguration).getApplicationIds().get(0));
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

    private boolean doRetry(String blockUid) {
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
        throw new LedpException(LedpCode.LEDP_00008, new String[] { errorMsg });
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

            HdfsUtils.moveGlobToDir(yarnConfiguration, blockAvroGlob, matchOutputDir);
        } catch (Exception e) {
            throw new RuntimeException("Failed to move block avro generated by application " + appId
                    + " to match output dir: " + e.getMessage(), e);
        }

    }

    private void mergeBlockErrorResult(ApplicationId appId) {
        if (!EntityMatchUtils.isAllocateIdModeEntityMatch(jobConfigurations.get(0).getMatchInput())) {
            return;
        }
        String blockOperationUid = blockUuidMap.get(appId.toString());
        String blockAvroGlob = hdfsPathBuilder.constructMatchBlockErrorAvroGlob(rootOperationUid, blockOperationUid);
        try {
            moveBlockAvro(blockAvroGlob, matchErrorDir);
        } catch (Exception e) {
            throw new RuntimeException("Failed to move block error avro generated by application " + appId
                    + " to match output dir: " + e.getMessage(), e);
        }
    }

    /*
     * Move all new entity avro files from target block directory to directory of
     * the entire match
     */
    private void mergeBlockNewEntityResult(ApplicationId appId) {
        if (appId == null || StringUtils.isBlank(matchNewEntityDir)) {
            return;
        }
        MatchInput input = jobConfigurations.get(0).getMatchInput();
        if (!EntityMatchUtils.shouldOutputNewEntities(input)) {
            log.info(
                    "Should not ouput new entities. Skip merging block new entity result. "
                            + "MatchInput(OperationalMode={},isAllocateId={},outputNewEntities={})",
                    input.getOperationalMode(), input.isAllocateId(), input.isOutputNewEntities());
            return;
        }

        String blockOperationUid = blockUuidMap.get(appId.toString());
        String blockAvroGlob = hdfsPathBuilder.constructMatchBlockNewEntityAvroGlob(rootOperationUid,
                blockOperationUid);
        try {
            moveBlockAvro(blockAvroGlob, matchNewEntityDir);
        } catch (Exception e) {
            String msg = String.format(
                    "Failed to move block avro for newly allocated entities to match dir %s. ApplicationId=%s, error=%s",
                    matchNewEntityDir, appId, e.getMessage());
            throw new RuntimeException(msg, e);
        }
    }

    /*
     * Move all block avro files (that satisfy blockAvroGlob) to destination
     * directory
     */
    private void moveBlockAvro(String blockAvroGlob, String destDir) throws Exception {
        if (HdfsUtils.getFilesByGlob(yarnConfiguration, blockAvroGlob).size() <= 0) {
            log.info("No files satisfy glob={}. Skip copying.", blockAvroGlob);
            return;
        }
        if (!HdfsUtils.fileExists(yarnConfiguration, destDir)) {
            log.info("Creating destination directory = {}", destDir);
            HdfsUtils.mkdir(yarnConfiguration, destDir);
        }

        log.info("Moving avro files (glob={}) to directory = {}", blockAvroGlob, destDir);
        HdfsUtils.moveGlobToDir(yarnConfiguration, blockAvroGlob, destDir);
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
