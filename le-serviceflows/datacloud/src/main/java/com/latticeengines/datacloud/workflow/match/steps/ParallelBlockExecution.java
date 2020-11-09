package com.latticeengines.datacloud.workflow.match.steps;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.TimeUnit;

import javax.inject.Inject;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;
import org.apache.commons.collections4.MapUtils;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.ObjectUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.time.DurationFormatUtils;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationReport;
import org.apache.hadoop.yarn.api.records.FinalApplicationStatus;
import org.apache.hadoop.yarn.api.records.YarnApplicationState;
import org.apache.hadoop.yarn.client.api.YarnClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.latticeengines.common.exposed.util.AvroUtils;
import com.latticeengines.common.exposed.util.HdfsUtils;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.common.exposed.util.SleepUtils;
import com.latticeengines.common.exposed.util.YarnUtils;
import com.latticeengines.datacloud.core.util.HdfsPathBuilder;
import com.latticeengines.datacloud.core.util.HdfsPodContext;
import com.latticeengines.datacloud.match.exposed.service.MatchCommandService;
import com.latticeengines.datacloud.match.exposed.util.MatchUtils;
import com.latticeengines.datacloud.match.util.EntityMatchUtils;
import com.latticeengines.domain.exposed.datacloud.DataCloudJobConfiguration;
import com.latticeengines.domain.exposed.datacloud.MatchCoreErrorConstants;
import com.latticeengines.domain.exposed.datacloud.manage.MatchBlock;
import com.latticeengines.domain.exposed.datacloud.manage.MatchCommand;
import com.latticeengines.domain.exposed.datacloud.match.EntityMatchResult;
import com.latticeengines.domain.exposed.datacloud.match.MatchBlockErrorData;
import com.latticeengines.domain.exposed.datacloud.match.MatchConstants;
import com.latticeengines.domain.exposed.datacloud.match.MatchInput;
import com.latticeengines.domain.exposed.datacloud.match.MatchOutput;
import com.latticeengines.domain.exposed.datacloud.match.MatchStatus;
import com.latticeengines.domain.exposed.datacloud.match.OperationalMode;
import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.domain.exposed.serviceflows.datacloud.match.steps.ParallelBlockExecutionConfiguration;
import com.latticeengines.domain.exposed.util.ApplicationIdUtils;
import com.latticeengines.domain.exposed.util.MetaDataTableUtils;
import com.latticeengines.domain.exposed.workflow.WorkflowContextConstants;
import com.latticeengines.proxy.exposed.matchapi.MatchInternalProxy;
import com.latticeengines.proxy.exposed.metadata.MetadataProxy;
import com.latticeengines.workflow.exposed.build.BaseWorkflowStep;

/**
 * Bulk match workflow step
 * Splits match requests into blocks which are sent in parallel to Yarn containers
 */
@Component("parallelBlockExecution")
@Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
public class ParallelBlockExecution extends BaseWorkflowStep<ParallelBlockExecutionConfiguration> {

    private static final Logger log = LoggerFactory.getLogger(ParallelBlockExecution.class);

    private static final int MAX_ERRORS = 100;
    private static final Long MATCH_TIMEOUT = TimeUnit.DAYS.toMillis(3);
    private static final String MATCHOUTPUT_BUFFER_FILE = "matchoutput.json";

    @Inject
    private MatchInternalProxy matchInternalProxy;

    @Inject
    private MatchCommandService matchCommandService;

    @Value("${datacloud.match.block.interval.sec}")
    private int blockRampingRate;

    @Inject
    private HdfsPathBuilder hdfsPathBuilder;

    @Inject
    private MetadataProxy metadataProxy;

    private YarnClient yarnClient;
    private String rootOperationUid;
    private List<ApplicationId> applicationIds = new ArrayList<>();
    // Match Block ApplicationId -> Match Block UUID
    private Map<String, String> blockUuidMap = new HashMap<>();
    // Match Block UUID -> Job Configuration
    private Map<String, DataCloudJobConfiguration> configMap = new HashMap<>();

    private int numErrors = 0;
    private float progress;
    private Long progressUpdated;
    private MatchOutput matchOutput;
    private List<DataCloudJobConfiguration> jobConfigurations;
    private List<DataCloudJobConfiguration> remainingJobs;
    private int totalRetries = 0;
    private String matchErrorDir;
    // directory to store all newly allocated entity list of this match
    private String matchNewEntityDir;
    private String matchCandidateDir;
    private String matchUsageDir;
    private Map<String, Long> newEntityCountsFromFailedBlocks = new HashMap<>();

    private Random random = new Random(System.currentTimeMillis());

    // Continuously AND blocks: start with true in both
    private boolean allSuccess = true;
    private boolean allFailures = true;

    private List<String> failedApps = new ArrayList<>();

    @Override
    public void execute() {
        try {
            log.info("Inside ParallelBlockExecution execute()");
            initializeYarnClient();

            // Generate jobConfigurations
            Map<String, String> tracingContext = getTracingContext();
            jobConfigurations = new ArrayList<>();
            Object listObj = executionContext.get(BulkMatchContextKey.YARN_JOB_CONFIGS);
            if (listObj instanceof List) {
                @SuppressWarnings("rawtypes")
                List list = (List) listObj;
                for (Object configObj : list) {
                    if (configObj instanceof DataCloudJobConfiguration) {
                        DataCloudJobConfiguration config = (DataCloudJobConfiguration) configObj;
                        if (tracingContext != null) {
                            config.setTracingContext(tracingContext);
                        }
                        jobConfigurations.add(config);
                        configMap.put(config.getBlockOperationUid(), config);
                    }
                }
            }

            HdfsPodContext.changeHdfsPodId(getConfiguration().getPodId());
            rootOperationUid = getStringValueFromContext(BulkMatchContextKey.ROOT_OPERATION_UID);

            // Output directories
            matchErrorDir = hdfsPathBuilder.constructMatchErrorDir(rootOperationUid).toString();
            matchNewEntityDir = hdfsPathBuilder.constructMatchNewEntityDir(rootOperationUid).toString();
            matchCandidateDir = hdfsPathBuilder.constructMatchCandidateDir(rootOperationUid).toString();
            matchUsageDir = hdfsPathBuilder.constructMatchUsageDir(rootOperationUid).toString();

            // Setup complete
            remainingJobs = new ArrayList<>(jobConfigurations);
            // TODO trace block submission
            while ((remainingJobs.size() != 0) || (applicationIds.size() != 0)) {
                submitMatchBlocks();
                waitForMatchBlocks();
                SleepUtils.sleep(10000L);
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

    private Map<String, String> getTracingContext() {
        if (!hasKeyInContext(TRACING_CONTEXT)) {
            return null;
        }

        return getMapObjectFromContext(TRACING_CONTEXT, String.class, String.class);
    }

    private void submitMatchBlocks() {
        Integer maxConcurrentBlocks = (Integer) executionContext.get(BulkMatchContextKey.MAX_CONCURRENT_BLOCKS);
        if (ObjectUtils.defaultIfNull(maxConcurrentBlocks, 0) == 0) {
            throw new RuntimeException("Invalid maximum concurrent number of blocks: " + maxConcurrentBlocks);
        }
        while ((remainingJobs.size() > 0) && (applicationIds.size() < maxConcurrentBlocks)) {
            DataCloudJobConfiguration jobConfiguration = remainingJobs.remove(0);
            while(!jobConfiguration.isReadyToProcess()) {
                log.info("Match block {} is not ready for processing.", jobConfiguration.getBlockOperationUid());
                SleepUtils.sleep(60000);
            }

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
        if (!allSuccess && allFailures) {
            throw new LedpException(LedpCode.LEDP_00008, new String[] { failedApps.toString() });
        }

        try {
            matchCommandService.update(rootOperationUid)
                    .status(MatchStatus.FINISHING)
                    .progress(0.98f)
                    .commit();

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

            // For D&B Connect, mark failed as batch errors
            if (!allSuccess) {
                String schemaFile = HdfsUtils.getHdfsFileContents(yarnConfiguration, hdfsPathBuilder.constructMatchSchemaFile(rootOperationUid).toString());
                Schema outputSchema = new Schema.Parser().parse(schemaFile);

                String outputDir = hdfsPathBuilder.constructMatchOutputDir(rootOperationUid).toString();

                if (!outputDir.endsWith("/")) {
                    outputDir += '/';
                }

                for (String appId : failedApps) {
                    String blockOperationUid = blockUuidMap.get(appId);
                    DataCloudJobConfiguration config = configMap.get(blockOperationUid);

                    if (config.getMatchInput().getTargetEntity().equals(BusinessEntity.PrimeAccount.toString())) {
                        try {
                            String blockErrorFile = hdfsPathBuilder.constructMatchBlockErrorFile(rootOperationUid, blockOperationUid)
                                    .toString();
                            String blockError = HdfsUtils.getHdfsFileContents(yarnConfiguration, blockErrorFile);

                            List<GenericRecord> failedInputRecords = AvroUtils.getData(yarnConfiguration, new Path(config.getAvroPath()));
                            List<GenericRecord> outputRecords = new ArrayList<>();

                            for (GenericRecord record : failedInputRecords) {
                                GenericRecordBuilder errorBuilder = new GenericRecordBuilder(outputSchema);

                                for (Schema.Field inputField : outputSchema.getFields()) {
                                    errorBuilder.set(inputField, record.get(inputField.name()));
                                }
                                errorBuilder.set(MatchConstants.MATCH_ERROR_TYPE, MatchCoreErrorConstants.ErrorType.BATCH_FAILURE.name());
                                int endOfFirstLine = blockError.indexOf('\n');
                                errorBuilder.set(MatchConstants.MATCH_ERROR_INFO, endOfFirstLine == -1 ? blockError : blockError.substring(0, endOfFirstLine));
                                outputRecords.add(errorBuilder.build());
                            }
                            String outputFile = hdfsPathBuilder.constructMatchBlockSplitAvro(rootOperationUid, blockOperationUid, 0).toString();
                            outputFile = outputDir + outputFile.substring(outputFile.lastIndexOf('/') + 1);
                            AvroUtils.writeToHdfsFile(yarnConfiguration, outputSchema, outputFile, outputRecords);
                        } catch (Exception e) {
                            log.error("Failed to save errors for matcher " + blockOperationUid + " in application " + appId
                                    + " : ", e);
                        }
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

            Map<String, Long> outputNewEntityCnts = new HashMap<>(
                    MapUtils.emptyIfNull(matchOutput.getStatistics().getNewEntityCount()));
            MatchUtils.mergeEntityCnt(outputNewEntityCnts, newEntityCountsFromFailedBlocks);

            log.info("Aggregated statistics will be stored in " + MatchUtils.toAvroGlobs(avroDir));
            matchCommandService.update(rootOperationUid) //
                    .resultLocation(avroDir) //
                    .dnbCommands() //
                    .rowsMatched(count.intValue()) //
                    .newEntityCounts(outputNewEntityCnts) //
                    .matchResults(entityMatchResultMap) //
                    .status(allSuccess ? MatchStatus.FINISHED : MatchStatus.PARTIAL_SUCCESS) //
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
                    mergeBlockResult(appId);
                    mergeBlockErrorResult(appId);
                    mergeBlockNewEntityResult(appId);
                    mergeBlockCandidateResult(appId);
                    mergeBlockUsageResult(appId);
                    matchCommandService.updateBlock(blockUid).status(state).progress(1f).commit();
                    allFailures = false;
                } else {
                    log.error("Unknown terminal status " + status + " for Application [" + appId
                            + "]. Treat it as FAILED.");
                    logFailedBlock(MatchStatus.FAILED, report);
                }

            }
        }
    }

    private void handleAbnormallyTerminatedBlock(ApplicationReport report) {
        ApplicationId appId = report.getApplicationId();
        FinalApplicationStatus status = report.getFinalApplicationStatus();
        String errorMsg = "Application [" + appId + "] ended abnormally with the final status " + status;

        String blockUid = blockUuidMap.get(appId.toString());
        updateNewEntityCounts(blockUid);
        if (doRetry(blockUid)) {
            log.info(errorMsg + ". Retry the block.");
            for (DataCloudJobConfiguration jobConfiguration : jobConfigurations) {
                if (jobConfiguration.getBlockOperationUid().equals(blockUid)) {
                    ApplicationId newAppId = ApplicationIdUtils
                            .toApplicationIdObj(matchInternalProxy.submitYarnJob(jobConfiguration).getApplicationIds().get(0));
                    blockUuidMap.remove(appId.toString());
                    blockUuidMap.put(newAppId.toString(), jobConfiguration.getBlockOperationUid());
                    applicationIds.add(newAppId);
                    if (report.getDiagnostics().contains("lost node")) {
                        log.info("Failed due to lost node. Re-run block without incrementing the retry count.");
                        matchCommandService.rerunBlock(blockUid, newAppId);
                    } else {
                        matchCommandService.retryBlock(blockUid, newAppId);
                    }
                    log.info("Submit a match block to application id " + newAppId);
                }
            }
        } else {
            log.warn(errorMsg + ". Failing the block");
            MatchStatus terminalStatus = FinalApplicationStatus.FAILED.equals(status) ? MatchStatus.FAILED
                    : MatchStatus.ABORTED;
            logFailedBlock(terminalStatus, report);
            allSuccess = false;
        }
    }

    private void updateNewEntityCounts(String blockUid) {
        try {
            MatchBlock block = matchCommandService.getBlock(blockUid);
            if (block != null && block.getNewEntityCounts() != null) {
                MatchUtils.mergeEntityCnt(newEntityCountsFromFailedBlocks, block.getNewEntityCounts());
                log.info("Copy new entity counts from failed block {}", block.getNewEntityCounts());
            } else if (block == null) {
                log.warn("Cannot find block object for match block {}", blockUid);
            } else {
                log.info("No new entity allocated for failed block {}", blockUid);
            }
        } catch (Exception e) {
            log.error("Failed to update new entity counts", e);
        }
    }

    private boolean doRetry(String blockUid) {
        return matchCommandService.blockIsRetriable(blockUid);
    }

    private void logFailedBlock(MatchStatus terminalStatus, ApplicationReport failedReport) {
        ApplicationId failedAppId = failedReport.getApplicationId();
        String blockOperationUid = blockUuidMap.get(failedAppId.toString());
        String blockErrorFile = hdfsPathBuilder.constructMatchBlockErrorFile(rootOperationUid, blockOperationUid)
                .toString();
        String errorMsg = "Match container [" + failedAppId + "] failed.";

        String avroDir = hdfsPathBuilder.constructMatchOutputDir(rootOperationUid).toString();
        try {
            String blockError = HdfsUtils.getHdfsFileContents(yarnConfiguration, blockErrorFile);

            // Each batch is given its own file, to avoid overwrites (especially by failTheWorkflowWithErrorMessage)
            String matchErrorFile = hdfsPathBuilder.constructErrorFileForBatchError(rootOperationUid, blockOperationUid).toString();

            DataCloudJobConfiguration config = configMap.get(blockOperationUid);

            // Record info necessary to determine error and input fields
            MatchBlockErrorData fileContents = new MatchBlockErrorData();
            fileContents.setContainerPath(failedAppId.toString());
            fileContents.setErrorFilePath(blockErrorFile);
            fileContents.setInputAvro(config.getAvroPath());
            HdfsUtils.writeToFile(yarnConfiguration, matchErrorFile, JsonUtils.serialize(fileContents));

            errorMsg += blockError.split("\n")[0];
        } catch (Exception e) {
            log.error("Failed to read the error for matcher " + blockOperationUid + " in application " + failedAppId
                    + " : " + e.getMessage());
        }

        if (terminalStatus == MatchStatus.ABORTED) {
            matchCommandService.update(rootOperationUid) //
                    .resultLocation(avroDir)
                    .status(terminalStatus) //
                    .errorMessage(errorMsg) //
                    .commit();

            throw new LedpException(LedpCode.LEDP_00008, new String[] { errorMsg });
        }

        failedApps.add(failedAppId.toString());
    }

    private void failTheWorkflowWithErrorMessage(String errorMsg, Exception ex) {
        try {
            String matchErrorFile = hdfsPathBuilder.constructMatchErrorFile(rootOperationUid).toString();
            log.info("Match Error File: " + matchErrorFile);
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
                    "Should not output new entities. Skip merging block new entity result. "
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
     * Move all match candidate avro files from target block directory to directory of
     * the entire match
     */
    private void mergeBlockCandidateResult(ApplicationId appId) {
        if (appId == null || StringUtils.isBlank(matchCandidateDir)) {
            return;
        }
        MatchInput input = jobConfigurations.get(0).getMatchInput();
        if (OperationalMode.MULTI_CANDIDATES.equals(input.getOperationalMode())) {
            String blockOperationUid = blockUuidMap.get(appId.toString());
            String blockAvroGlob = hdfsPathBuilder.constructMatchBlockCandidateAvroGlob(rootOperationUid,
                    blockOperationUid);
            try {
                moveBlockAvro(blockAvroGlob, matchCandidateDir);
            } catch (Exception e) {
                String msg = String.format(
                        "Failed to move block avro for match candidates to match dir %s. ApplicationId=%s, error=%s",
                        matchCandidateDir, appId, e.getMessage());
                throw new RuntimeException(msg, e);
            }
        }
    }

    /*
     * Move all match usage avro files from target block directory to directory of
     * the entire match
     */
    private void mergeBlockUsageResult(ApplicationId appId) {
        if (appId == null || StringUtils.isBlank(matchUsageDir)) {
            return;
        }
        MatchInput input = jobConfigurations.get(0).getMatchInput();
        if (input.getDplusUsageReportConfig() != null && input.getDplusUsageReportConfig().isEnabled()) {
            String blockOperationUid = blockUuidMap.get(appId.toString());
            String blockAvroGlob = hdfsPathBuilder.constructMatchBlockUsageAvroGlob(rootOperationUid,
                    blockOperationUid);
            try {
                moveBlockAvro(blockAvroGlob, matchUsageDir);
            } catch (Exception e) {
                String msg = String.format(
                        "Failed to move block avro for usage events to match dir %s. ApplicationId=%s, error=%s",
                        matchUsageDir, appId, e.getMessage());
                throw new RuntimeException(msg, e);
            }
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
