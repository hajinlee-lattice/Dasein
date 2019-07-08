package com.latticeengines.datacloud.etl.transformation.service.impl;

import static com.latticeengines.datacloud.etl.transformation.transformer.IterativeStep.CTX_ITERATION;
import static com.latticeengines.datacloud.etl.transformation.transformer.IterativeStep.CTX_LAST_COUNT;
import static com.latticeengines.datacloud.etl.transformation.transformer.IterativeStep.MAX_ITERATION;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;

import javax.inject.Inject;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.time.DurationFormatUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.datacloud.core.service.DataCloudNotificationService;
import com.latticeengines.datacloud.core.source.Source;
import com.latticeengines.datacloud.core.source.impl.IngestionSource;
import com.latticeengines.datacloud.core.source.impl.PipelineSource;
import com.latticeengines.datacloud.core.source.impl.TableSource;
import com.latticeengines.datacloud.core.util.HdfsPodContext;
import com.latticeengines.datacloud.core.util.LoggingUtils;
import com.latticeengines.datacloud.core.util.RequestContext;
import com.latticeengines.datacloud.etl.service.DataCloudEngineService;
import com.latticeengines.datacloud.etl.service.SourceService;
import com.latticeengines.datacloud.etl.transformation.entitymgr.PipelineTransformationReportEntityMgr;
import com.latticeengines.datacloud.etl.transformation.service.TransformationService;
import com.latticeengines.datacloud.etl.transformation.service.TransformerService;
import com.latticeengines.datacloud.etl.transformation.transformer.IterativeStep;
import com.latticeengines.datacloud.etl.transformation.transformer.TransformStep;
import com.latticeengines.datacloud.etl.transformation.transformer.Transformer;
import com.latticeengines.domain.exposed.datacloud.DataCloudConstants;
import com.latticeengines.domain.exposed.datacloud.manage.PipelineTransformationReportByStep;
import com.latticeengines.domain.exposed.datacloud.manage.ProgressStatus;
import com.latticeengines.domain.exposed.datacloud.manage.TransformationProgress;
import com.latticeengines.domain.exposed.datacloud.orchestration.DataCloudEngine;
import com.latticeengines.domain.exposed.datacloud.orchestration.DataCloudEngineStage;
import com.latticeengines.domain.exposed.datacloud.transformation.PipelineTransformationReport;
import com.latticeengines.domain.exposed.datacloud.transformation.PipelineTransformationRequest;
import com.latticeengines.domain.exposed.datacloud.transformation.TransformationStepReport;
import com.latticeengines.domain.exposed.datacloud.transformation.config.impl.PipelineTransformationConfiguration;
import com.latticeengines.domain.exposed.datacloud.transformation.step.IterativeStepConfig;
import com.latticeengines.domain.exposed.datacloud.transformation.step.SourceIngestion;
import com.latticeengines.domain.exposed.datacloud.transformation.step.SourceTable;
import com.latticeengines.domain.exposed.datacloud.transformation.step.TargetTable;
import com.latticeengines.domain.exposed.datacloud.transformation.step.TransformationStepConfig;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.monitor.SlackSettings;
import com.latticeengines.proxy.exposed.metadata.MetadataProxy;

/**
 * This transformation service allows parameterizing the source and target,
 * pipelining the transformation process.
 */
@Component("pipelineTransformationService")
public class PipelineTransformationService extends AbstractTransformationService<PipelineTransformationConfiguration>
        implements TransformationService<PipelineTransformationConfiguration>, DataCloudEngineService {

    private static final Logger log = LoggerFactory.getLogger(PipelineTransformationService.class);
    private static final String SLACK_BOT = "PipelineTransformer";

    @Inject
    private PipelineSource pipelineSource;

    @Inject
    private SourceService sourceService;

    @Inject
    private TransformerService transformerService;

    @Inject
    private PipelineTransformationReportEntityMgr reportEntityMgr;

    @Inject
    private MetadataProxy metadataProxy;

    @Inject
    private DataCloudNotificationService notificationService;

    private final String PIPELINE = DataCloudConstants.PIPELINE_TEMPSRC_PREFIX;
    private final String VERSION = "_version_";
    private final String STEP = "_step_";
    private final String SUFFIX = "_suffix_" + ThreadLocalRandom.current().nextInt(0, 10000);

    @Override
    Logger getLogger() {
        return log;
    }

    public String getServiceBeanName() {
        return "pipelineTransformationService";
    }

    @Override
    public Source getSource() {
        return pipelineSource;
    }

    @Override
    public Class<PipelineTransformationConfiguration> getConfigurationClass() {
        return PipelineTransformationConfiguration.class;
    }

    @Override
    public boolean isManualTriggerred() {
        return true;
    }

    @Override
    public TransformationProgress startNewProgress(PipelineTransformationConfiguration config, String creator) {
        checkTransformationConfigurationValidity(config);
        TransformationProgress progress;
        try {
            progress = progressEntityMgr.findEarliestFailureUnderMaxRetry(getSource(), config.getVersion());

            if (progress == null) {
                progress = progressEntityMgr.insertNewProgress(config.getName(), getSource(), config.getVersion(),
                        creator);
            } else {
                log.info("Retrying " + progress.getRootOperationUID());
                progress.setNumRetries(progress.getNumRetries() + 1);
                progress = progressEntityMgr.updateStatus(progress, ProgressStatus.NEW);
            }
            writeTransConfOutsideWorkflow(config, progress);
        } catch (Exception e) {
            throw new RuntimeException("Failed to start a new progress for " + getSource(), e);
        }
        LoggingUtils.logInfo(getLogger(), progress, "Started a new progress with version=" + config.getVersion());
        return progress;
    }

    @Override
    protected TransformationProgress transformHook(TransformationProgress progress,
            PipelineTransformationConfiguration transConf) {

        progress.setPipelineName(transConf.getName());
        progressEntityMgr.updateProgress(progress);

        boolean succeeded = false;
        String workflowDir = initialDataFlowDirInHdfs(progress);

        cleanupWorkflowDir(progress, workflowDir);

        TransformStep[] steps = initiateTransformSteps(progress, transConf);
        if (steps == null) {
            updateStatusToFailed(progress, "Failed to initiate transform steps", null);
        } else {
            succeeded = executeTransformSteps(progress, steps, workflowDir, transConf);
            if (succeeded || !transConf.getKeepTemp()) {
                cleanupTempSources(steps);
            }
        }
        if (doPostProcessing(progress, workflowDir, false) && succeeded) {
            return progress;
        } else {
            return null;
        }
    }

    @Override
    public List<String> findUnprocessedBaseVersions() {
        return new ArrayList<>();
    }

    private void cleanupWorkflowDir(TransformationProgress progress, String workflowDir) {
        deleteFSEntry(progress, workflowDir + "/*");
    }

    private void cleanupTempSources(TransformStep[] steps) {

        for (int i = steps.length - 1; i >= 0; i--) {
            log.info("Clean up temp source for step " + i);
            Source source = steps[i].getTarget();
            if (source != null && isTempSource(source)) {
                sourceService.deleteSource(source);
            }
        }
    }

    private String getTempSourceName(String pipelineName, String version, int step, boolean keepTemp) {
        if (keepTemp) {
            return PIPELINE + pipelineName + VERSION + version + STEP + step;
        } else {
            return PIPELINE + pipelineName + VERSION + version + STEP + step + SUFFIX;
        }

    }

    private boolean isTempSource(Source source) {
        return source.getSourceName().startsWith(PIPELINE);
    }

    private TransformStep[] initiateTransformSteps(TransformationProgress progress,
            PipelineTransformationConfiguration transConf) {

        List<TransformationStepConfig> stepConfigs = transConf.getSteps();
        TransformStep[] steps = new TransformStep[stepConfigs.size()];
        Map<Source, String> sourceVersions = new HashMap<>();
        String pipelineVersion = transConf.getVersion();

        for (int stepIdx = 0; stepIdx < steps.length; stepIdx++) {
            TransformationStepConfig config = stepConfigs.get(stepIdx);
            Transformer transformer = transformerService.findTransformerByName(config.getTransformer());

            if (transformer == null) {
                log.error("Failed to find transformer " + config.getTransformer());
                return null;
            }

            List<Integer> inputSteps = config.getInputSteps();

            List<String> baseSourceNames = config.getBaseSources();
            List<String> baseTemplateNames = config.getBaseTemplates();
            List<String> inputBaseVersions = config.getBaseVersions();
            if (baseTemplateNames == null) {
                baseTemplateNames = baseSourceNames;
            }

            int baseSourceCount = 0;
            if (inputSteps != null) {
                baseSourceCount += inputSteps.size();
            }
            if (baseSourceNames != null) {
                baseSourceCount += baseSourceNames.size();
            }

            Source[] baseSources = new Source[baseSourceCount];
            Source[] baseTemplates = new Source[baseSourceCount];
            List<String> baseVersions = new ArrayList<String>();

            int baseSourceIdx = 0;

            if (inputSteps != null) {
                for (Integer inputStep : inputSteps) {
                    log.info("total steps " + steps.length + "stepidx " + stepIdx + " basesourcnt " + baseSourceCount
                            + " input step " + inputStep);
                    baseSources[baseSourceIdx] = steps[inputStep].getTarget();
                    baseTemplates[baseSourceIdx] = steps[inputStep].getTargetTemplate();
                    baseVersions.add(steps[inputStep].getTargetVersion());
                    baseSourceIdx++;
                }
            }

            transformer.initBaseSources(config.getConfiguration(), baseSourceNames);
            Map<String, SourceTable> baseTables = config.getBaseTables();
            if (baseTables == null) {
                baseTables = Collections.emptyMap();
            }
            Map<String, SourceIngestion> baseIngestions = config.getBaseIngestions();
            if (baseIngestions == null) {
                baseIngestions = Collections.emptyMap();
            }

            Map<String, TableSource> involvedTableSources = new HashMap<>();
            Map<String, IngestionSource> involvedIngestionSources = new HashMap<>();

            if ((baseSourceNames != null) && (baseSourceNames.size() != 0)) {
                for (int i = 0; i < baseSourceNames.size(); i++, baseSourceIdx++) {
                    String sourceName = baseSourceNames.get(i);
                    Source source;
                    if (baseTables.containsKey(sourceName)) {
                        SourceTable baseTable = baseTables.get(sourceName);
                        Table table;
                        try {
                            table = metadataProxy.getTable(baseTable.getCustomerSpace().toString(),
                                    baseTable.getTableName());
                        } catch (Exception e) {
                            throw new RuntimeException("Failed to retrieve table from metadata proxy for "
                                    + baseTable.getCustomerSpace() + " : " + baseTable.getTableName());
                        }
                        if (table == null) {
                            throw new RuntimeException("There is no table named " + baseTable.getTableName()
                                    + " for customer " + baseTable.getCustomerSpace());
                        }
                        source = new TableSource(table, baseTable.getCustomerSpace());
                        involvedTableSources.put(sourceName, (TableSource) source);
                    } else if (baseIngestions.containsKey(sourceName)) {
                        SourceIngestion baseIngestion = baseIngestions.get(sourceName);
                        source = new IngestionSource(baseIngestion.getIngestionName());
                        involvedIngestionSources.put(sourceName, (IngestionSource) source);
                    }else {
                        source = sourceService.findBySourceName(sourceName);
                    }
                    if (source == null) {
                        updateStatusToFailed(progress, "Base source " + sourceName + " not found", null);
                        return null;
                    }
                    baseSources[baseSourceIdx] = source;

                    String templateName = baseTemplateNames.get(i);
                    Source template = sourceService.findBySourceName(templateName);
                    if (involvedTableSources.containsKey(templateName)) {
                        template = involvedTableSources.get(templateName);
                    }
                    if (involvedIngestionSources.containsKey(templateName)) {
                        template = involvedIngestionSources.get(templateName);
                    }
                    if (template == null) {
                        updateStatusToFailed(progress, "Base template " + templateName + " not found", null);
                        return null;
                    }
                    baseTemplates[baseSourceIdx] = template;

                    String sourceVersion = null;
                    if (inputBaseVersions == null) {
                        sourceVersion = sourceVersions.get(source);
                        if (sourceVersion == null) {
                            sourceVersion = hdfsSourceEntityMgr.getCurrentVersion(source);
                        }
                    } else {
                        sourceVersion = inputBaseVersions.get(i);
                    }
                    sourceVersions.put(source, sourceVersion);
                    baseVersions.add(sourceVersion);
                }
            }

            String targetVersion = config.getTargetVersion();
            if (targetVersion == null) {
                targetVersion = pipelineVersion;
            }

            Source target;
            Source targetTemplate = null;
            String targetName = config.getTargetSource();
            TargetTable targetTable = config.getTargetTable();
            if (targetTable != null) {
                target = sourceService.createTableSource(targetTable, pipelineVersion);
            } else if (targetName == null) {
                targetName = getTempSourceName(transConf.getName(), pipelineVersion, stepIdx, transConf.getKeepTemp());
                target = sourceService.createSource(targetName);
            } else {
                target = sourceService.findOrCreateSource(targetName);
                targetTemplate = target;
            }

            String targetTemplateName = config.getTargetTemplate();
            if (targetTemplateName != null) {
                targetTemplate = sourceService.findBySourceName(targetTemplateName);
            }
            if (targetTemplate == null) {
                if (baseTemplates.length > 0) {
                    targetTemplate = baseTemplates[0];
                } else {
                    targetTemplate = target;
                }
            }

            sourceVersions.put(target, targetVersion);
            log.info("Step " + stepIdx + " target " + target.getSourceName());

            String confStr = config.getConfiguration();
            TransformStep step;
            if (TransformationStepConfig.ITERATIVE.equalsIgnoreCase(config.getStepType())) {
                step = new IterativeStep(String.valueOf(stepIdx), transformer, baseSources, baseVersions, baseTemplates,
                        target, targetVersion, targetTemplate, confStr);
                log.info("Found a iterative step " + step.getName());
            } else {
                step = new TransformStep(String.valueOf(stepIdx), transformer, baseSources, baseVersions, baseTemplates,
                        target, targetVersion, targetTemplate, confStr);
                log.info("Found a simple step " + step.getName());
            }
            steps[stepIdx] = step;
        }
        return steps;
    }

    private boolean executeTransformSteps(TransformationProgress progress, TransformStep[] steps, String workflowDir,
            PipelineTransformationConfiguration transConf) {
        Long pipelineStarTime = System.currentTimeMillis();

        String name = transConf.getName();
        boolean reportEnabled = true;
        if (StringUtils.isBlank(name)) {
            log.info("Report will not be generated for anonymous pipeline");
            reportEnabled = false;
        }
        String version = transConf.getVersion();
        if (version == null) {
            log.info("Report will not be generated for pipeline execution without version");
            reportEnabled = false;
        }

        // initialize reports
        PipelineTransformationReport report = new PipelineTransformationReport();
        if (reportEnabled) {
            reportEntityMgr.deleteReport(name, version);
            report.setName(name);
        }
        List<TransformationStepReport> stepReports = new ArrayList<>();
        List<TransformationStepConfig> stepConfigs = transConf.getSteps();

        for (int i = 0; i < steps.length; i++) {
            String slackMessage = String.format("Started step %d at %s", i, new Date().toString());
            sendSlack(transConf.getName() + " [" + progress.getYarnAppId() + "]", slackMessage,
                    SlackSettings.Color.NORMAL, transConf);
            TransformStep step = steps[i];
            Transformer transformer = step.getTransformer();
            Long startTime = System.currentTimeMillis();
            boolean succeeded;
            if (step instanceof IterativeStep) {
                succeeded = executeIterativeStep((IterativeStep) step, transformer, progress, workflowDir);
            } else {
                succeeded = executeSimpleStep(step, transformer, progress, workflowDir);
            }
            long stepDuration = System.currentTimeMillis() - startTime;
            step.setElapsedTime(stepDuration / 1000);

            try {
                if (!succeeded) {
                    // failed message
                    slackMessage = String.format("Failed at step %d after %s :sob:", i,
                            DurationFormatUtils.formatDurationHMS(stepDuration));
                    sendSlack(transConf.getName() + " [" + progress.getYarnAppId() + "]", slackMessage,
                            SlackSettings.Color.DANGER, transConf);
                    updateStatusToFailed(progress, "Failed to transform data at step " + i, null);
                    return false;
                }
                // success message
                slackMessage = String.format("Step %d finished after %s :smile:", i,
                        DurationFormatUtils.formatDurationHMS(stepDuration));
                sendSlack(transConf.getName() + " [" + progress.getYarnAppId() + "]", slackMessage,
                        SlackSettings.Color.GOOD, transConf);

                if (i == steps.length - 1) {
                    slackMessage = String.format("All %d steps in the pipeline are finished after %s :clap:",
                            steps.length,
                            DurationFormatUtils.formatDurationHMS(System.currentTimeMillis() - pipelineStarTime));
                    sendSlack(transConf.getName() + " [" + progress.getYarnAppId() + "]", slackMessage,
                            SlackSettings.Color.GOOD, transConf);
                }

                if (reportEnabled) {
                    Source targetSource = step.getTarget();
                    String targetVersion = step.getTargetVersion();
                    if (step.getCount() != null) {
                        log.info("Found count=" + step.getCount() + " from cascading counter. Lucky!");
                    } else {
                        Long targetRecords = hdfsSourceEntityMgr.count(targetSource, targetVersion);
                        step.setCount(targetRecords);
                    }
                    TransformationStepConfig stepConfig = stepConfigs.get(i);
                    TransformationStepReport stepReport = generateStepReport(step, stepConfig);
                    stepReports.add(stepReport);
                    report.setSteps(stepReports);
                    hdfsSourceEntityMgr.saveReport(pipelineSource, name, version, JsonUtils.serialize(report));

                    reportStepToDB(name, version, step, stepConfig);
                }
            } catch (Exception e) {
                log.error("Failed to generate report of step " + step.getName(), e);
            }
        }
        return true;
    }

    private boolean executeSimpleStep(TransformStep step, Transformer transformer, TransformationProgress progress,
            String workflowDir) {
        try {
            log.info("Transforming step " + step.getName());
            if (hdfsSourceEntityMgr.checkSourceExist(step.getTarget(), step.getTargetVersion())) {
                step.setElapsedTime(0);
                log.info("Skip executed step " + step.getName());
                return true;
            } else {
                boolean succeeded = transformer.transform(progress, workflowDir, step);
                if (step.getTarget() != null) {
                    String workflowOutputDir = getWorkflowOutputDir(step, workflowDir);
                    saveSourceVersion(progress, step.getTargetSchema(), step.getTarget(), step.getTargetVersion(),
                            workflowOutputDir, step.getCount());
                }
                cleanupWorkflowDir(progress, workflowDir);
                return succeeded;
            }
        } catch (Exception e) {
            updateStatusToFailed(progress, "Failed to transform data at step " + step.getName(), e);
        }
        return false;
    }

    private boolean executeIterativeStep(IterativeStep step, Transformer transformer, TransformationProgress progress,
            String workflowDir) {
        try {
            log.info("Iterative transforming step " + step.getName());
            Map<String, Object> iterationContext = initializeIterationContext(step);
            boolean converged;
            boolean succeeded;
            do {
                int iteration = (Integer) iterationContext.get(CTX_ITERATION);
                log.info("Starting " + iteration + "-th iteration ...");
                succeeded = transformer.transform(progress, workflowDir, step);
                String workflowOutputDir = getWorkflowOutputDir(step, workflowDir);
                saveSourceVersion(progress, null, step.getTarget(), step.getTargetVersion(), workflowOutputDir,
                        step.getCount());
                cleanupWorkflowDir(progress, workflowDir);
                converged = updateIterationContext(step, iterationContext);
                if (!converged) {
                    updateStepForNextIteration(step);
                }
            } while (!converged && ((Integer) iterationContext.get(CTX_ITERATION)) < MAX_ITERATION);
            if (converged && succeeded) {
                log.info("Iterative step " + step.getName() + " converged, final count = "
                        + String.valueOf(step.getCount()));
                // Disable hive table creation
                // createSourceHiveTable(step.getTarget(),
                // step.getTargetVersion());
                return true;
            } else {
                if (!converged) {
                    log.error("Iterative step " + step.getName() + " failed to converge.");
                } else {
                    log.error("Iterative step " + step.getName() + " converged, but not successful.");
                }
                return false;
            }
        } catch (Exception e) {
            updateStatusToFailed(progress, "Failed to transform data at step " + step.getName(), e);
        }
        return false;
    }

    private Map<String, Object> initializeIterationContext(IterativeStep step) {
        Map<String, Object> iterationContext = new HashMap<>();
        if (step.getStrategy() instanceof IterativeStepConfig.ConvergeOnCount) {
            iterationContext.put(CTX_LAST_COUNT, -1L);
            iterationContext.put(CTX_ITERATION, 0);
        }
        log.info("Initialized iteration context: \n" + JsonUtils.pprint(iterationContext));
        return iterationContext;
    }

    private boolean updateIterationContext(IterativeStep step, Map<String, Object> iterationContext) {
        boolean converged = false;
        if (step.getStrategy() instanceof IterativeStepConfig.ConvergeOnCount) {
            IterativeStepConfig.ConvergeOnCount strategy = (IterativeStepConfig.ConvergeOnCount) step.getStrategy();
            long lastCount = (Long) iterationContext.get(CTX_LAST_COUNT);
            long newCount = step.getCount();
            int countDiff = strategy.getCountDiff();
            iterationContext.put(CTX_LAST_COUNT, step.getCount());
            if (Math.abs(newCount - lastCount) <= countDiff) {
                // converged
                converged = true;
            }
            int iteration = (Integer) iterationContext.get(CTX_ITERATION);
            iterationContext.put(CTX_ITERATION, iteration + 1);
        }
        log.info("Updated iteration context to: \n" + JsonUtils.pprint(iterationContext));
        return converged;
    }

    private void updateStepForNextIteration(IterativeStep step) {
        // refresh input
        Source newTargetSource = step.getTarget();
        Source[] updatedBaseSources = new Source[step.getBaseSources().length];
        List<String> updatedBaseSourceNames = new ArrayList<>();
        for (int i = 0; i < updatedBaseSources.length; i++) {
            Source original = step.getBaseSources()[i];
            if (original.getSourceName().equals(step.getStrategy().getIteratingSource())) {
                updatedBaseSources[i] = newTargetSource;
                String targetSourceVersion = hdfsSourceEntityMgr.getCurrentVersion(newTargetSource);
                step.getBaseVersions().set(i, targetSourceVersion);
            } else {
                updatedBaseSources[i] = step.getBaseSources()[i];
            }
            updatedBaseSourceNames.add(updatedBaseSources[i].getSourceName());
        }
        step.setBaseSources(updatedBaseSources);
        log.info("Updated the base sources of iterative step " + step.getName() + " to " + updatedBaseSourceNames);
    }

    private TransformationStepReport generateStepReport(TransformStep step, TransformationStepConfig stepConfig) {
        log.info("Generating report of step " + step.getName());
        TransformationStepReport stepReport = new TransformationStepReport();
        Source[] baseSources = step.getBaseSources();
        List<String> baseVersions = step.getBaseVersions();

        stepReport.setTransformer(stepConfig.getTransformer());
        Source targetSource = step.getTarget();
        String targetVersion = step.getTargetVersion();
        for (int j = 0; j < baseSources.length; j++) {
            Source baseSource = baseSources[j];
            stepReport.addBaseSource(baseSource.getSourceName(), baseVersions.get(j));
        }
        stepReport.setElapsedTime(step.getElapsedTime());
        if (targetSource != null && hdfsSourceEntityMgr.checkSourceExist(targetSource, targetVersion)) {
            stepReport.setExecuted(true);
            Long targetRecords = step.getCount();
            stepReport.setTargetSource(targetSource.getSourceName(), targetVersion, targetRecords);
        }
        return stepReport;
    }

    private void reportStepToDB(String pipelineName, String pipelineVersion, TransformStep step,
            TransformationStepConfig stepConfig) {
        PipelineTransformationReportByStep stepReport = new PipelineTransformationReportByStep();

        stepReport.setPipeline(pipelineName);
        stepReport.setVersion(pipelineVersion);
        stepReport.setStepName(step.getName());
        Source[] baseSources = step.getBaseSources();
        List<String> baseVersions = step.getBaseVersions();
        stepReport.setTransformer(stepConfig.getTransformer());

        String baseSourceNames = "";
        String baseSourceVersions = "";
        for (int j = 0; j < baseSources.length; j++) {
            Source baseSource = baseSources[j];
            baseSourceNames = baseSourceNames + baseSource.getSourceName();
            baseSourceVersions = baseSourceVersions + baseVersions.get(j);
            if (baseSourceNames.length() > 1000 || baseSourceVersions.length() > 1000) {
                break;
            }
        }
        stepReport.setBaseSources(baseSourceNames);
        stepReport.setBaseVersions(baseSourceVersions);

        Source targetSource = step.getTarget();
        String targetVersion = step.getTargetVersion();
        if (targetSource != null && hdfsSourceEntityMgr.checkSourceExist(targetSource, targetVersion)) {
            stepReport.setExecuted(true);
            Long targetRecords = step.getCount();
            stepReport.setTargetSource(targetSource.getSourceName(), targetVersion, targetRecords);
            stepReport.setTempTarget(isTempSource(targetSource));
            stepReport.setElapsedTime(step.getElapsedTime());
        }
        stepReport.setHdfsPod(HdfsPodContext.getHdfsPodId());
        reportEntityMgr.insertReportByStep(stepReport);
    }

    @Override
    public PipelineTransformationConfiguration createTransformationConfiguration(List<String> baseVersions,
            String targetVersion) {
        return null;
    }

    public PipelineTransformationConfiguration createTransformationConfiguration(
            PipelineTransformationRequest inputRequest) {

        String error = null;
        PipelineTransformationRequest request = inputRequest;

        String pipelineName = request.getName();
        String version = request.getVersion();
        if (version == null) {
            version = createNewVersionStringFromNow();
        }

        log.info("Creating Pipeline configuration");

        List<TransformationStepConfig> steps = request.getSteps();

        if (((steps == null) || (steps.size() == 0)) && (pipelineName != null)) {
            log.info("Building pipeline " + pipelineName + " from templates in hdfs");
            try {
                String requestJson = hdfsSourceEntityMgr.getRequest(pipelineSource, pipelineName);
                if (requestJson != null) {
                    request = JsonUtils.deserialize(requestJson, PipelineTransformationRequest.class);
                    steps = request.getSteps();
                }
            } catch (Exception e) {
                error = String.format("Failed to load pipeline template %s from hdfs: %s", pipelineName,
                        e.getMessage());
                log.error(error, e);
                RequestContext.logError(error);
                steps = null;
            }
        }

        if ((steps == null) || (steps.size() == 0)) {
            log.info("Invalid pipeline " + pipelineName + " without steps");
            return null;
        }

        //log.info("start checking transformation step configs:");
        int currentStep = 0;
        for (TransformationStepConfig step : steps) {
            //log.info(JsonUtils.serialize(step));
            Transformer transformer = transformerService.findTransformerByName(step.getTransformer());
            if (transformer == null) {
                error = String.format("Transformer %s does not exist", step.getTransformer());
                log.error(error);
                RequestContext.logError(error);
                return null;
            }

            List<Integer> inputSteps = step.getInputSteps();
            if (inputSteps != null) {
                for (Integer inputStep : inputSteps) {
                    if (inputStep < 0) {
                        error = String.format("Input Step %s uses invalid step %s", currentStep, inputStep);
                        log.error(error);
                        RequestContext.logError(error);
                        return null;
                    } else if (inputStep >= currentStep) {
                        error = String.format("Step %s uses future step %s result", currentStep, inputStep);
                        log.error(error);
                        RequestContext.logError(error);
                        return null;
                    }
                }
            }

            List<String> baseSourceNames = step.getBaseSources();
            if (!step.getNoInput() && ((baseSourceNames == null) || (baseSourceNames.size() == 0))
                    && ((inputSteps == null) || (inputSteps.size() == 0))) {
                error = String.format("Step %s does not have any input source specified", currentStep);
                log.error(error);
                RequestContext.logError(error);
                return null;
            }

            List<String> baseVersions = step.getBaseVersions();
            if ((baseVersions != null) && (baseVersions.size() != baseSourceNames.size())) {
                error = String.format("Base versions(%d) does match base sources(%d)", baseVersions.size(),
                        baseSourceNames.size());
                log.error(error);
                RequestContext.logError(error);
                return null;
            }

            List<String> baseTemplates = step.getBaseTemplates();
            if (baseTemplates != null) {
                if (baseTemplates.size() != baseSourceNames.size()) {
                    error = String.format("Base templates(%d) does match base sources(%d)", baseTemplates.size(),
                            baseSourceNames.size());
                    log.error(error);
                    RequestContext.logError(error);
                    return null;
                } else {
                    for (String sourceName : baseTemplates) {
                        Source source = sourceService.findBySourceName(sourceName);
                        if (source == null) {
                            error = String.format("Base template %s does not exist", sourceName);
                            log.error(error);
                            RequestContext.logError(error);
                            return null;
                        }
                    }
                }
            }

            String targetTemplate = step.getTargetTemplate();
            if (targetTemplate != null) {
                Source source = sourceService.findBySourceName(targetTemplate);
                if (source == null) {
                    error = String.format("targetTemplate source %s does not exist", targetTemplate);
                    log.error(error);
                    RequestContext.logError(error);
                    return null;
                }
            }

            String config = step.getConfiguration();
            List<String> sourceNames;
            if (!step.getNoInput()) {
                if (inputSteps == null) {
                    sourceNames = baseSourceNames;
                } else {
                    sourceNames = new ArrayList<>();
                    for (int i = 0; i < inputSteps.size(); i++) {
                        sourceNames.add(getTempSourceName(pipelineName, version, i, request.getKeepTemp()));
                    }
                    if (baseSourceNames != null) {
                        sourceNames.addAll(baseSourceNames);
                    }
                }
                if (!transformer.validateConfig(config, sourceNames)) {
                    error = String.format("Invalid configuration for step %s", currentStep);
                    log.error(error);
                    RequestContext.logError(error);
                    return null;
                }
            }
            currentStep++;
        }

        PipelineTransformationConfiguration configuration = new PipelineTransformationConfiguration();
        configuration.setName(pipelineName);
        configuration.setVersion(version);
        configuration.setServiceBeanName(getServiceBeanName());
        configuration.setKeepTemp(inputRequest.getKeepTemp());
        configuration.setSteps(steps);
        configuration.setEnableSlack(request.isEnableSlack());
        configuration.setContainerMemMB(inputRequest.getContainerMemMB());

        return configuration;
    }

    @Override
    protected Date checkTransformationConfigurationValidity(PipelineTransformationConfiguration conf) {

        // Do we really validate configuration again here?
        return new Date();

    }

    @Override
    protected PipelineTransformationConfiguration parseTransConfJsonInsideWorkflow(String confStr) throws IOException {
        return JsonUtils.deserialize(confStr, getConfigurationClass());
    }

    private void sendSlack(String title, String text, SlackSettings.Color color,
            PipelineTransformationConfiguration transConf) {
        if (transConf.isEnableSlack()) {
            notificationService.sendSlack(title, text, SLACK_BOT, color);
        }
    }

    @Override
    public DataCloudEngineStage findProgressAtVersion(DataCloudEngineStage stage) {
        stage.setEngine(DataCloudEngine.TRANSFORMATION);
        TransformationProgress progress = progressEntityMgr.findPipelineProgressAtVersion(stage.getEngineName(),
                stage.getVersion());
        if (progress == null) {
            stage.setStatus(ProgressStatus.NOTSTARTED);
        } else {
            stage.setStatus(progress.getStatus());
            stage.setMessage(progress.getErrorMessage());
        }
        return stage;
    }

    @Override
    public DataCloudEngine getEngine() {
        return DataCloudEngine.TRANSFORMATION;
    }

    @Override
    public String findCurrentVersion(String pipelineName) {
        return null;
    }

    private String getWorkflowOutputDir(TransformStep step, String workflowDir) {
        String subDir = step.getTransformer().outputSubDir();
        if (StringUtils.isNotBlank(subDir)) {
            return workflowDir + "/" + subDir;
        } else {
            return workflowDir;
        }
    }

}
