package com.latticeengines.dataplatform.service.impl.dlorchestration;

import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.concurrent.Callable;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.FinalApplicationStatus;
import org.apache.http.message.BasicNameValuePair;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.type.TypeReference;

import com.google.common.annotations.VisibleForTesting;
import com.latticeengines.common.exposed.util.HdfsUtils;
import com.latticeengines.common.exposed.util.HdfsUtils.HdfsFilenameFilter;
import com.latticeengines.common.exposed.util.StringTokenUtils;
import com.latticeengines.common.exposed.util.YarnUtils;
import com.latticeengines.dataplatform.entitymanager.ModelCommandEntityMgr;
import com.latticeengines.dataplatform.entitymanager.ModelCommandResultEntityMgr;
import com.latticeengines.dataplatform.entitymanager.ModelCommandStateEntityMgr;
import com.latticeengines.dataplatform.exposed.exception.LedpCode;
import com.latticeengines.dataplatform.exposed.exception.LedpException;
import com.latticeengines.dataplatform.exposed.service.AlertService;
import com.latticeengines.dataplatform.service.dlorchestration.ModelCommandLogService;
import com.latticeengines.dataplatform.service.dlorchestration.ModelStepProcessor;
import com.latticeengines.dataplatform.service.dlorchestration.ModelStepYarnProcessor;
import com.latticeengines.dataplatform.service.modeling.ModelingJobService;
import com.latticeengines.domain.exposed.dataplatform.JobStatus;
import com.latticeengines.domain.exposed.dataplatform.dlorchestration.ModelCommand;
import com.latticeengines.domain.exposed.dataplatform.dlorchestration.ModelCommandResult;
import com.latticeengines.domain.exposed.dataplatform.dlorchestration.ModelCommandState;
import com.latticeengines.domain.exposed.dataplatform.dlorchestration.ModelCommandStatus;
import com.latticeengines.domain.exposed.dataplatform.dlorchestration.ModelCommandStep;

public class ModelCommandCallable implements Callable<Long> {

    private static final Log log = LogFactory.getLog(ModelCommandCallable.class);

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

    public ModelCommandCallable() {
    }

    public ModelCommandCallable(ModelCommand modelCommand, Configuration yarnConfiguration,
            ModelingJobService modelingJobService, ModelCommandEntityMgr modelCommandEntityMgr,
            ModelCommandStateEntityMgr modelCommandStateEntityMgr, ModelStepYarnProcessor modelStepYarnProcessor,
            ModelCommandLogService modelCommandLogService, ModelCommandResultEntityMgr modelCommandResultEntityMgr,
            ModelStepProcessor modelStepFinishProcessor, ModelStepProcessor modelStepOutputResultsProcessor,
            ModelStepProcessor modelStepRetrieveMetadataProcessor, DebugProcessorImpl debugProcessorImpl,
            AlertService alertService, String httpFsPrefix) {
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

            executeStep(modelStepRetrieveMetadataProcessor, ModelCommandStep.RETRIEVE_METADATA, commandParameters);
            executeYarnStep(ModelCommandStep.LOAD_DATA, commandParameters);
        } else { // modelCommand IN_PROGRESS
            List<ModelCommandState> commandStates = modelCommandStateEntityMgr.findByModelCommandAndStep(modelCommand,
                    modelCommand.getModelCommandStep());
            int successCount = 0;
            boolean jobFailed = false;

            for (ModelCommandState commandState : commandStates) {
                JobStatus jobStatus = modelingJobService.getJobStatus(commandState.getYarnApplicationId());
                saveModelCommandStateFromJobStatus(commandState, jobStatus);

                if (jobStatus.getStatus().equals(FinalApplicationStatus.SUCCEEDED)) {
                    if (commandState.getModelCommandStep().equals(ModelCommandStep.LOAD_DATA)) {
                        ModelCommandParameters commandParameters = new ModelCommandParameters(
                                modelCommand.getCommandParameters());
                        String customer = modelCommand.getDeploymentExternalId();
                        String filePath = modelStepRetrieveMetadataProcessor.getCustomerBaseDir() + "/" + customer
                                + "/data/" + commandParameters.getEventTable();
                        List<String> files = HdfsUtils.getFilesForDir(
                                modelStepRetrieveMetadataProcessor.getConfiguration(), filePath,
                                new HdfsFilenameFilter() {

                                    @Override
                                    public boolean accept(String filename) {
                                        return filename.endsWith(".avro");
                                    }

                                });
                        log.info("_____Job is " + jobStatus.getState() + "," + jobStatus.getStatus() + " file status "
                                + filePath + " is : \n");
                        log.info(files);
                    } else if (commandState.getModelCommandStep().equals(ModelCommandStep.PROFILE_DATA)) {
                        generateDataDiagnostics(commandState, jobStatus);
                    }
                    successCount++;
                } else if (jobStatus.getStatus().equals(FinalApplicationStatus.UNDEFINED)
                        || YarnUtils.isPrempted(jobStatus.getDiagnostics())) {
                    // Job in progress.
                } else if (jobStatus.getStatus().equals(FinalApplicationStatus.KILLED)
                        || jobStatus.getStatus().equals(FinalApplicationStatus.FAILED)) {
                    jobFailed = true;
                }
            }

            if (successCount == commandStates.size()) { // All jobs succeeded,
                                                        // move on to next step
                handleAllJobsSucceeded();
            } else if (jobFailed) {
                handleJobFailed();
            } else {
                // Do nothing; job(s) in progress.
            }
        }
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
    void handleJobFailed() {
        modelCommandLogService.logCompleteStep(modelCommand, modelCommand.getModelCommandStep(),
                ModelCommandStatus.FAIL);

        ModelCommandResult result = modelCommandResultEntityMgr.findByModelCommand(modelCommand);
        result.setEndTime(new Date());
        result.setProcessStatus(ModelCommandStatus.FAIL);
        modelCommandResultEntityMgr.update(result);

        modelCommand.setCommandStatus(ModelCommandStatus.FAIL);
        modelCommandEntityMgr.update(modelCommand);

        alertService.triggerCriticalEvent(LedpCode.LEDP_16007.getMessage(), new BasicNameValuePair("commandId",
                modelCommand.getPid().toString()),
                new BasicNameValuePair("deploymentExternalId", modelCommand.getDeploymentExternalId()),
                new BasicNameValuePair("failedStep", modelCommand.getModelCommandStep().getDescription()));
    }

    private void executeYarnStep(ModelCommandStep step, ModelCommandParameters commandParameters) {
        modelCommand.setModelCommandStep(step);
        modelCommandEntityMgr.update(modelCommand);
        modelCommandLogService.logBeginStep(modelCommand, step);

        List<ApplicationId> appIds = modelStepYarnProcessor.executeYarnStep(modelCommand.getDeploymentExternalId(),
                step, commandParameters);
        for (ApplicationId appId : appIds) {
            String s = appId.toString();
            JobStatus jobStatus = modelingJobService.getJobStatus(s);

            ModelCommandState commandState = new ModelCommandState(modelCommand, step);
            commandState.setYarnApplicationId(appId.toString());
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

    private void generateDataDiagnostics(ModelCommandState commandState, JobStatus jobStatus) throws Exception {
        String diagnosticsPath = "";
        try {
            for (String filePath : HdfsUtils.getFilesForDir(yarnConfiguration, jobStatus.getDataDiagnosticsDirectory())) {
                if (filePath.endsWith("diagnostics.json")) {
                    diagnosticsPath = filePath;
                    break;
                }
            }
        } catch (Exception e) {
            throw new LedpException(LedpCode.LEDP_16002, e, new String[] { String.valueOf(modelCommand.getPid()),
                    commandState.getYarnApplicationId() });
        }

        if (diagnosticsPath.isEmpty()) {
            modelCommandLogService.log(modelCommand, "No data diagnostics generated.");
            log.warn("No data diagnostics generated for command " + modelCommand.getPid() + "with application id "
                    + commandState.getYarnApplicationId());
            return;
        }

        // Parse file
        String warnings = "";
        diagnosticsPath = jobStatus.getDataDiagnosticsDirectory() + "/" + StringTokenUtils.stripPath(diagnosticsPath); // full
                                                                                                                       // hdfs
                                                                                                                       // path
        String content = HdfsUtils.getHdfsFileContents(yarnConfiguration, diagnosticsPath);
        TypeReference<HashMap<String, Object>> typeRef = new TypeReference<HashMap<String, Object>>() {
        };
        HashMap<String, Object> diagnostics = new ObjectMapper().readValue(content.getBytes(), typeRef);

        // Check positive event rate between arbitrary range
        double[] positiveEventRateThresh = { 1, 50 }; // 1% to 50%
        @SuppressWarnings("unchecked")
        double positiveEventRate = (double) ((LinkedHashMap<String, Object>) diagnostics.get("Summary"))
                .get("PositiveEventRate");
        positiveEventRate *= 100;
        if (positiveEventRate < positiveEventRateThresh[0] || positiveEventRate > positiveEventRateThresh[1]) {
            warnings += "Detected abnormal positive event rate " + positiveEventRate + "% from event table (below "
                    + positiveEventRateThresh[0] + "% or above " + positiveEventRateThresh[1] + "%).\n";
        }
        // Check any invalid column bucketing metadata
        @SuppressWarnings("unchecked")
        LinkedHashMap<String, Object> metadataDiagnostics = ((LinkedHashMap<String, Object>) diagnostics
                .get("MetadataDiagnostics"));
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
