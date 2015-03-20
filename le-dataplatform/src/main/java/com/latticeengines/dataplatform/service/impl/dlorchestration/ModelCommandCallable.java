package com.latticeengines.dataplatform.service.impl.dlorchestration;

import java.sql.SQLException;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.concurrent.Callable;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.FinalApplicationStatus;
import org.apache.http.message.BasicNameValuePair;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.springframework.jdbc.core.JdbcTemplate;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Joiner;
import com.latticeengines.common.exposed.util.HdfsUtils;
import com.latticeengines.common.exposed.util.YarnUtils;
import com.latticeengines.dataplatform.entitymanager.ModelCommandEntityMgr;
import com.latticeengines.dataplatform.entitymanager.ModelCommandResultEntityMgr;
import com.latticeengines.dataplatform.entitymanager.ModelCommandStateEntityMgr;
import com.latticeengines.dataplatform.exposed.service.AlertService;
import com.latticeengines.dataplatform.service.MetadataService;
import com.latticeengines.dataplatform.service.dlorchestration.ModelCommandLogService;
import com.latticeengines.dataplatform.service.dlorchestration.ModelStepProcessor;
import com.latticeengines.dataplatform.service.dlorchestration.ModelStepYarnProcessor;
import com.latticeengines.dataplatform.service.modeling.ModelingJobService;
import com.latticeengines.domain.exposed.dataplatform.JobStatus;
import com.latticeengines.domain.exposed.dataplatform.dlorchestration.ModelCommand;
import com.latticeengines.domain.exposed.dataplatform.dlorchestration.ModelCommandLog;
import com.latticeengines.domain.exposed.dataplatform.dlorchestration.ModelCommandResult;
import com.latticeengines.domain.exposed.dataplatform.dlorchestration.ModelCommandState;
import com.latticeengines.domain.exposed.dataplatform.dlorchestration.ModelCommandStatus;
import com.latticeengines.domain.exposed.dataplatform.dlorchestration.ModelCommandStep;
import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;

public class ModelCommandCallable implements Callable<Long> {

    private static final Log log = LogFactory.getLog(ModelCommandCallable.class);

    private static final Joiner commaJoiner = Joiner.on(", ").skipNulls();
    private static final int SUCCESS = 0;
    private static final int FAIL = -1;
    private static final String HTTPFS_SUFFIX = "?op=OPEN&user.name=yarn";

    private Configuration yarnConfiguration;

    private ModelingJobService modelingJobService;

    private ModelCommandEntityMgr modelCommandEntityMgr;

    private ModelCommandStateEntityMgr modelCommandStateEntityMgr;

    private ModelStepYarnProcessor modelStepYarnProcessor;

    private ModelCommandLogService modelCommandLogService;

    private ModelCommandResultEntityMgr modelCommandResultEntityMgr;

    private ModelStepProcessor modelStepFinishProcessor;

    private ModelStepProcessor modelStepOutputResultsProcessor;

    private ModelStepProcessor modelStepRetrieveMetadataProcessor;

    private DebugProcessorImpl debugProcessorImpl;

    private AlertService alertService;

    private ModelCommand modelCommand;

    private String httpFsPrefix;

    private String resourceManagerWebAppAddress;

    private String appTimeLineWebAppAddress;

    private int rowFailThreshold;

    private int rowWarnThreshold;

    private int positiveEventFailThreshold;

    private int positiveEventWarnThreshold;

    private MetadataService metadataService;

    public ModelCommandCallable() {
    }

    public ModelCommandCallable(ModelCommand modelCommand, Configuration yarnConfiguration,
            ModelingJobService modelingJobService, ModelCommandEntityMgr modelCommandEntityMgr,
            ModelCommandStateEntityMgr modelCommandStateEntityMgr, ModelStepYarnProcessor modelStepYarnProcessor,
            ModelCommandLogService modelCommandLogService, ModelCommandResultEntityMgr modelCommandResultEntityMgr,
            ModelStepProcessor modelStepFinishProcessor, ModelStepProcessor modelStepOutputResultsProcessor,
            ModelStepProcessor modelStepRetrieveMetadataProcessor, DebugProcessorImpl debugProcessorImpl,
            AlertService alertService, String httpFsPrefix, String resourceManagerWebAppAddress,
            String appTimeLineWebAppAddress, int rowFailThreshold, int rowWarnThreshold,
            int positiveEventFailThreshold, int positiveEventWarnThreshold, MetadataService metadataService) {
        this.modelCommand = modelCommand;
        this.yarnConfiguration = yarnConfiguration;
        this.modelingJobService = modelingJobService;
        this.modelCommandEntityMgr = modelCommandEntityMgr;
        this.modelCommandStateEntityMgr = modelCommandStateEntityMgr;
        this.modelStepYarnProcessor = modelStepYarnProcessor;
        this.modelCommandLogService = modelCommandLogService;
        this.modelCommandResultEntityMgr = modelCommandResultEntityMgr;
        this.modelStepOutputResultsProcessor = modelStepOutputResultsProcessor;
        this.modelStepFinishProcessor = modelStepFinishProcessor;
        this.modelStepRetrieveMetadataProcessor = modelStepRetrieveMetadataProcessor;
        this.debugProcessorImpl = debugProcessorImpl;
        this.alertService = alertService;
        this.httpFsPrefix = httpFsPrefix;
        this.resourceManagerWebAppAddress = resourceManagerWebAppAddress;
        this.appTimeLineWebAppAddress = appTimeLineWebAppAddress;
        this.rowFailThreshold = rowFailThreshold;
        this.rowWarnThreshold = rowWarnThreshold;
        this.positiveEventFailThreshold = positiveEventFailThreshold;
        this.positiveEventWarnThreshold = positiveEventWarnThreshold;
        this.metadataService = metadataService;
    }

    @Override
    public Long call() throws Exception {
        int result = SUCCESS;
        try {
            log.info("Begin scheduled work on " + ModelCommandLogServiceImpl.MODELCOMMAND_ID_LOG_PREFIX + ":"
                    + modelCommand.getPid()); // Need this line to associate
                                              // modelCommandId with threadId in
                                              // log4j output.
            executeWorkflow();
            log.info("End scheduled work on " + ModelCommandLogServiceImpl.MODELCOMMAND_ID_LOG_PREFIX + ":"
                    + modelCommand.getPid());
        } catch (LedpException e) {
            result = FAIL;
            modelCommandLogService.logLedpException(modelCommand, e);
        } catch (Exception e) {
            result = FAIL;
            modelCommandLogService.logException(modelCommand, e);
        } finally {
            if (result == FAIL) {
                handleJobFailed();
            }
        }
        return modelCommand.getPid();
    }

    private static String readableFileSize(long size) {
        if (size <= 0) {
            return "0";
        }
        final String[] units = new String[] { "B", "kB", "MB", "GB", "TB" };
        int digitGroups = (int) (Math.log10(size) / Math.log10(1024));
        return new DecimalFormat("#,##0.#").format(size / Math.pow(1024, digitGroups)) + " " + units[digitGroups];
    }

    private void executeWorkflow() throws Exception {
        if (modelCommand.isNew()) {
            Date now = new Date();
            modelCommandResultEntityMgr.create(new ModelCommandResult(modelCommand, now, now,
                    ModelCommandStatus.IN_PROGRESS));

            modelCommand.setModelCommandStep(ModelCommandStep.RETRIEVE_METADATA);
            modelCommand.setCommandStatus(ModelCommandStatus.IN_PROGRESS);
            modelCommandEntityMgr.update(modelCommand);

            ModelCommandParameters commandParameters = new ModelCommandParameters(modelCommand.getCommandParameters());

            if (commandParameters.isDebug()) {
                debugProcessorImpl.execute(modelCommand, commandParameters);
            }

            // Validation is turned off during tests
            if (commandParameters.isValidate()) {
                boolean validationFailed = validateDataSize(commandParameters);
                if (validationFailed) {
                    handleJobFailed();
                    return;
                }
            }

            executeStep(modelStepRetrieveMetadataProcessor, ModelCommandStep.RETRIEVE_METADATA, commandParameters);
            executeYarnStep(ModelCommandStep.LOAD_DATA, commandParameters);

            JdbcTemplate dlOrchestrationJdbcTemplate = debugProcessorImpl.getDlOrchestrationJdbcTemplate();
            Long rowSize = metadataService.getRowCount(dlOrchestrationJdbcTemplate, modelCommand.getEventTable());
            Long dataSize = metadataService.getDataSize(dlOrchestrationJdbcTemplate, modelCommand.getEventTable());
            Integer columnSize = metadataService.getColumnCount(dlOrchestrationJdbcTemplate,
                    modelCommand.getEventTable());
            modelCommandLogService.log(modelCommand, "Data Size: " + readableFileSize(dataSize) + " Row count: "
                    + rowSize + " Column count: " + columnSize);
        } else { // modelCommand IN_PROGRESS
            List<ModelCommandState> commandStates = modelCommandStateEntityMgr.findByModelCommandAndStep(modelCommand,
                    modelCommand.getModelCommandStep());
            int successCount = 0;
            boolean jobFailed = false;
            List<String> failedYarnApplicationIds = new ArrayList<>();

            for (ModelCommandState commandState : commandStates) {
                JobStatus jobStatus = modelingJobService.getJobStatus(commandState.getYarnApplicationId());
                saveModelCommandStateFromJobStatus(commandState, jobStatus);
                if (jobStatus.getStatus().equals(FinalApplicationStatus.SUCCEEDED)) {
                    if (commandState.getModelCommandStep().equals(ModelCommandStep.PROFILE_DATA)) {
                        generateDataDiagnostics(commandState, jobStatus);
                    }
                    successCount++;
                } else if (jobStatus.getStatus().equals(FinalApplicationStatus.UNDEFINED)
                        || YarnUtils.isPrempted(jobStatus.getDiagnostics())) {
                    // Job in progress.
                } else if (jobStatus.getStatus().equals(FinalApplicationStatus.KILLED)
                        || jobStatus.getStatus().equals(FinalApplicationStatus.FAILED)) {
                    jobFailed = true;
                    failedYarnApplicationIds.add(commandState.getYarnApplicationId());
                }
            }

            if (successCount == commandStates.size()) { // All jobs succeeded,
                                                        // move on to next step
                handleAllJobsSucceeded();
            } else if (jobFailed) {
                handleJobFailed(failedYarnApplicationIds);
            } else {
                // Do nothing; job(s) in progress.
            }
        }
    }

    private boolean validateDataSize(ModelCommandParameters commandParameters) throws SQLException {
        JdbcTemplate dlOrchestrationJdbcTemplate = debugProcessorImpl.getDlOrchestrationJdbcTemplate();
        Long rowCount = metadataService.getRowCount(dlOrchestrationJdbcTemplate, modelCommand.getEventTable());
        Long positiveEventCount = metadataService.getPositiveEventCount(dlOrchestrationJdbcTemplate,
                modelCommand.getEventTable(), commandParameters.getEventColumnName());

        if (rowCount < rowFailThreshold && positiveEventCount < positiveEventFailThreshold) {
            modelCommandLogService.log(modelCommand,
                    "Failing modeling job due to insufficient rows or positive events. " + "Row count: " + rowCount
                            + " Positive event count: " + positiveEventCount);
            return true;
        } else if (rowCount < rowWarnThreshold || positiveEventCount < positiveEventWarnThreshold) {
            modelCommandLogService.log(modelCommand,
                    "Model quality may be low due to insufficient rows or positive events. " + "Row count: " + rowCount
                            + " Positive event count: " + positiveEventCount);
        }

        return false;
    }

    private void handleAllJobsSucceeded() {
        modelCommandLogService.logCompleteStep(modelCommand, modelCommand.getModelCommandStep(),
                ModelCommandStatus.SUCCESS);

        ModelCommandStep nextStep = modelCommand.getModelCommandStep().getNextStep();
        modelCommand.setModelCommandStep(nextStep);

        ModelCommandParameters commandParameters = new ModelCommandParameters(modelCommand.getCommandParameters());
        if (nextStep.equals(ModelCommandStep.OUTPUT_COMMAND_RESULTS)) {
            executeStep(modelStepOutputResultsProcessor, ModelCommandStep.OUTPUT_COMMAND_RESULTS, commandParameters);
            executeStep(modelStepFinishProcessor, ModelCommandStep.FINISH, commandParameters);
        } else {
            executeYarnStep(nextStep, commandParameters);
        }
    }

    @VisibleForTesting
    String handleJobFailed() {
        return handleJobFailed(Collections.<String> emptyList());
    }

    @VisibleForTesting
    String handleJobFailed(List<String> failedYarnApplicationIds) {
        modelCommandLogService.logCompleteStep(modelCommand, modelCommand.getModelCommandStep(),
                ModelCommandStatus.FAIL);
        ModelCommandResult result = modelCommandResultEntityMgr.findByModelCommand(modelCommand);
        result.setEndTime(new Date());
        result.setProcessStatus(ModelCommandStatus.FAIL);
        modelCommandResultEntityMgr.update(result);

        modelCommand.setCommandStatus(ModelCommandStatus.FAIL);
        modelCommandEntityMgr.update(modelCommand);

        String appIds = "";
        StringBuilder clientUrl = new StringBuilder(resourceManagerWebAppAddress).append("/cluster/");
        if (!failedYarnApplicationIds.isEmpty()) {
            appIds = commaJoiner.join(failedYarnApplicationIds);
            // Currently each step only generates one yarn job anyways so first
            // failed appId works
            clientUrl.append("app/").append(failedYarnApplicationIds.get(0));
            modelCommandLogService.log(modelCommand, "Failed job link: " + appTimeLineWebAppAddress + "/app/"
                    + failedYarnApplicationIds.get(0));
        }

        List<BasicNameValuePair> details = new ArrayList<>();
        details.add(new BasicNameValuePair("commandId", modelCommand.getPid().toString()));
        details.add(new BasicNameValuePair("yarnAppIds", failedYarnApplicationIds.isEmpty() ? "None" : appIds));
        details.add(new BasicNameValuePair("deploymentExternalId", modelCommand.getDeploymentExternalId()));
        details.add(new BasicNameValuePair("failedStep", modelCommand.getModelCommandStep().getDescription()));
        List<ModelCommandLog> logs = modelCommandLogService.findByModelCommand(modelCommand);
        if (!logs.isEmpty()) {
            for (ModelCommandLog modelCommandLog : logs) {
                details.add(new BasicNameValuePair("commandLogId" + modelCommandLog.getPid(), modelCommandLog
                        .getMessage()));
            }
        }

        return alertService.triggerCriticalEvent(LedpCode.LEDP_16007.getMessage(), clientUrl.toString(), details);
    }

    private void executeYarnStep(ModelCommandStep step, ModelCommandParameters commandParameters) {
        modelCommand.setModelCommandStep(step);
        modelCommandEntityMgr.update(modelCommand);
        modelCommandLogService.logBeginStep(modelCommand, step);

        List<ApplicationId> appIds = modelStepYarnProcessor.executeYarnStep(modelCommand.getDeploymentExternalId(),
                step, modelCommand, commandParameters);
        for (ApplicationId appId : appIds) {
            String appIdString = appId.toString();
            modelCommandLogService.logYarnAppId(modelCommand, appIdString, step);
            JobStatus jobStatus = modelingJobService.getJobStatus(appIdString);

            ModelCommandState commandState = new ModelCommandState(modelCommand, step);
            commandState.setYarnApplicationId(appIdString);
            saveModelCommandStateFromJobStatus(commandState, jobStatus);
        }
    }

    private void executeStep(ModelStepProcessor processor, ModelCommandStep step,
            ModelCommandParameters commandParameters) {
        long start = System.currentTimeMillis();
        modelCommand.setModelCommandStep(step);
        modelCommandEntityMgr.update(modelCommand);
        modelCommandLogService.logBeginStep(modelCommand, step);
        ModelCommandState commandState = new ModelCommandState(modelCommand, step);
        commandState.setStatus(FinalApplicationStatus.UNDEFINED);
        modelCommandStateEntityMgr.create(commandState);

        processor.executeStep(modelCommand, commandParameters);

        commandState.setElapsedTimeInMillis(System.currentTimeMillis() - start);
        commandState.setStatus(FinalApplicationStatus.SUCCEEDED);
        modelCommandStateEntityMgr.update(commandState);
        modelCommandLogService.logCompleteStep(modelCommand, step, ModelCommandStatus.SUCCESS);
    }

    private void saveModelCommandStateFromJobStatus(ModelCommandState commandState, JobStatus jobStatus) {
        commandState.setStatus(jobStatus.getStatus());
        commandState.setProgress(jobStatus.getProgress());
        commandState.setDiagnostics(jobStatus.getDiagnostics());
        commandState.setTrackingUrl(jobStatus.getTrackingUrl());
        commandState.setElapsedTimeInMillis(System.currentTimeMillis() - jobStatus.getStartTime());
        modelCommandStateEntityMgr.createOrUpdate(commandState);
    }

    void generateDataDiagnostics(ModelCommandState commandState, JobStatus jobStatus) throws Exception {
        String diagnosticsPath = jobStatus.getDataDiagnosticsPath();

        if (!HdfsUtils.fileExists(yarnConfiguration, diagnosticsPath)) {
            modelCommandLogService.log(modelCommand, "No data diagnostics generated.");
            log.warn("No data diagnostics generated for command " + modelCommand.getPid() + "with application id "
                    + commandState.getYarnApplicationId());
            return;
        }

        // Parse diagnostics file
        String warnings = "";
        String content = HdfsUtils.getHdfsFileContents(yarnConfiguration, diagnosticsPath);
        JSONParser jsonParser = new JSONParser();
        JSONObject jsonObject = (JSONObject) jsonParser.parse(content);

        // Check positive event rate between arbitrary range
        double[] positiveEventRateThresh = { 1, 50 }; // 1% to 50%
        double positiveEventRate = (double) ((JSONObject) jsonObject.get("Summary")).get("PositiveEventRate");
        positiveEventRate *= 100; // Convert to percentage

        if (positiveEventRate < positiveEventRateThresh[0] || positiveEventRate > positiveEventRateThresh[1]) {
            warnings += "Detected abnormal positive event rate " + positiveEventRate + "% from event table (below "
                    + positiveEventRateThresh[0] + "% or above " + positiveEventRateThresh[1] + "%).\n";
        }

        // check if there's skipped rows
        long numOfSkippedRows = (long) ((JSONObject) jsonObject.get("Summary")).get("NumberOfSkippedRows");
        if (numOfSkippedRows > 0) {
            warnings += "The number of skipped rows=" + numOfSkippedRows + "\n";
        }

        // check if there's high UC columns
        String highUCColumns = (String) ((JSONObject) jsonObject.get("Summary")).get("HighUCColumns");
        if (highUCColumns != null) {
            warnings += "Columns with high Uncertainty Coefficient=" + highUCColumns + "\n";
        }

        // Check any invalid column bucketing metadata
        JSONObject metadataDiagnostics = (JSONObject) jsonObject.get("MetadataDiagnostics");
        List<String> columns = new ArrayList<String>();
        for (Object key : metadataDiagnostics.keySet()) {
            columns.add((String) key);
        }
        if (!columns.isEmpty()) {
            warnings += "Detected invalid bucketing metadata for columns: " + columns.toString() + "\n";
        }

        // Generate warnings
        if (!warnings.isEmpty()) {
            modelCommandLogService.log(modelCommand, "Data diagnostics:\n" + warnings);
        }

        // Provide link to full diagnostics file
        modelCommandLogService.log(modelCommand, "Data diagnostics json file download link: " + httpFsPrefix
                + diagnosticsPath + HTTPFS_SUFFIX);

    }

}
