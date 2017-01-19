package com.latticeengines.datacloud.etl.transformation.service.impl;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.datacloud.core.source.Source;
import com.latticeengines.datacloud.core.source.impl.PipelineSource;
import com.latticeengines.datacloud.etl.service.SourceService;
import com.latticeengines.datacloud.etl.transformation.entitymgr.PipelineTransformationReportEntityMgr;
import com.latticeengines.datacloud.etl.transformation.service.TransformationService;
import com.latticeengines.datacloud.etl.transformation.service.TransformerService;
import com.latticeengines.datacloud.etl.transformation.transformer.Transformer;
import com.latticeengines.domain.exposed.datacloud.manage.PipelineTransformationReportByStep;
import com.latticeengines.domain.exposed.datacloud.manage.TransformationProgress;
import com.latticeengines.domain.exposed.datacloud.transformation.PipelineTransformationReport;
import com.latticeengines.domain.exposed.datacloud.transformation.PipelineTransformationRequest;
import com.latticeengines.domain.exposed.datacloud.transformation.TransformationStepConfig;
import com.latticeengines.domain.exposed.datacloud.transformation.TransformationStepReport;
import com.latticeengines.domain.exposed.datacloud.transformation.configuration.impl.PipelineTransformationConfiguration;

/**
 * This transformation service allows parameterizing the source and target, pipelining the tranformation process.
 */
@Component("pipelineTransformationService")
public class PipelineTransformationService
        extends AbstractTransformationService<PipelineTransformationConfiguration>
        implements TransformationService<PipelineTransformationConfiguration> {

    private static final Log log = LogFactory.getLog(PipelineTransformationService.class);

    @Autowired
    private PipelineSource pipelineSource;

    @Autowired
    private SourceService sourceService;

    @Autowired
    private TransformerService transformerService;

    @Autowired
    private PipelineTransformationReportEntityMgr reportEntityMgr;

    private final String PIPELINE = "Pipeline_";
    private final String VERSION = "_version_";
    private final String STEP = "_step_";

    @Override
    Log getLogger() {
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
    protected TransformationProgress transformHook(TransformationProgress progress, PipelineTransformationConfiguration transConf) {

        boolean succeeded = false;
        String workflowDir = initialDataFlowDirInHdfs(progress);

        cleanupWorkflowDir(progress, workflowDir);

        TransformStep[] steps = initiateTransformSteps(progress, transConf);
        if (steps == null) {
            updateStatusToFailed(progress, "Failed to initiate transfom steps", null);
        } else {
            succeeded = executeTransformSteps(progress, steps, workflowDir);
            log.info("Report pipe " + transConf.getName() + transConf.getVersion());
            reportPipelineExec(progress, transConf, steps);
            reportPipelineExecToDB(progress, transConf, steps);
            if (!transConf.getKeepTemp()) {
                cleanupTempSources(progress, steps);
            }
        }
        if (doPostProcessing(progress, workflowDir, false) & succeeded) {
            return progress;
        } else {
            return null;
        }
    }

    @Override
    public List<String> findUnprocessedBaseVersions() {
        return new ArrayList<String>();
    }


    private void cleanupWorkflowDir(TransformationProgress progress, String workflowDir) {
        deleteFSEntry(progress, workflowDir + "/*");
    }

    private void cleanupTempSources(TransformationProgress progress, TransformStep[] steps) {

        for (int i = steps.length - 1; i >= 0; i--) {
            log.info("Clean up temp source for step " + i);
            Source source = steps[i].getTarget();
            if (isTempSource(source)) {
                sourceService.deleteSource(source);
            }
        }
    }

    private String getTempSourceName(String pipelineName, String version, int step) {
        return PIPELINE + pipelineName + VERSION + version + STEP + step;
    }

    private boolean isTempSource(Source source) {
        return source.getSourceName().startsWith(PIPELINE);
    }

    private TransformStep[] initiateTransformSteps(TransformationProgress progress,
                                           PipelineTransformationConfiguration transConf) {

        List<TransformationStepConfig> stepConfigs = transConf.getSteps();
        TransformStep[] steps = new TransformStep[stepConfigs.size()];
        Map<Source, String> sourceVersions = new HashMap<Source, String>();
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
                    baseSources[baseSourceIdx] = steps[inputStep].getTarget();
                    baseTemplates[baseSourceIdx++] = steps[inputStep].getTargetTemplate();
                    baseVersions.add(steps[inputStep].getTargetVersion());
                }
            }

            if ((baseSourceNames != null) && (baseSourceNames.size() != 0)) {
                for (int i = 0; i < baseSourceNames.size(); i++, baseSourceIdx++) {
                    String sourceName = baseSourceNames.get(i);
                    Source source = sourceService.findBySourceName(sourceName);
                    if (source == null) {
                        updateStatusToFailed(progress, "Base source " + sourceName + " not found", null);
                        return null;
                    }
                    baseSources[baseSourceIdx] = source;

                    String templateName = baseTemplateNames.get(i);
                    Source template = sourceService.findBySourceName(templateName);
                    if (template == null) {
                        updateStatusToFailed(progress, "Base source " + templateName + " not found", null);
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

            Source target = null;
            Source targetTemplate = null;
            String targetName = config.getTargetSource();
            if (targetName == null) {
                targetName = getTempSourceName(transConf.getName(), pipelineVersion, stepIdx);
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
                targetTemplate = baseTemplates[0];
            }

            String targetVersion = config.getTargetVersion();
            if (targetVersion == null) {
                targetVersion = pipelineVersion;
            }

            sourceVersions.put(target, targetVersion);
            log.info("step " + stepIdx + " target " + target.getSourceName() + " template " + targetTemplate.getSourceName());

            String confStr = config.getConfiguration();
            TransformStep step = new TransformStep(transformer, baseSources, baseVersions, baseTemplates,
                                                   target, targetVersion, targetTemplate, confStr);
            steps[stepIdx] = step;
        }
        return steps;
    }

    private boolean executeTransformSteps(TransformationProgress progress, TransformStep[] steps, String workflowDir) {
        for (int i = 0; i < steps.length; i++) {
            TransformStep step = steps[i];
            Transformer transformer = step.getTransformer();
            try {
                log.info("Transforming step " + i);
                if (hdfsSourceEntityMgr.checkSourceExist(step.getTarget(), step.getTargetVersion())) {
                    step.setElapsedTime(0);
                    log.info("Skip executed step " + i);
                } else {
                    long startTime = System.currentTimeMillis();

                    boolean succeeded = transformer.transform(progress, workflowDir, step.getBaseSources(), step.getBaseVersions(),
                                                              step.getBaseTemplates(), step.getTargetTemplate(),
                                                              step.getConfig());

                    step.setElapsedTime((System.currentTimeMillis() - startTime) / 1000);
                    if (!succeeded) {
                        updateStatusToFailed(progress, "Failed to transform data at step " + i, null);
                        return false;
                    }

                    saveSourceVersion(progress, step.getTarget(), step.getTargetVersion(), workflowDir);
                    cleanupWorkflowDir(progress, workflowDir);
               }
            } catch (Exception e) {
                updateStatusToFailed(progress, "Failed to transform data at step " + i, e);
                return false;
            }
        }
        return true;
    }

    private void reportPipelineExec(TransformationProgress progress, PipelineTransformationConfiguration transConf, TransformStep[] steps) {
        String name = transConf.getName();

        if (name == null) {
            log.info("Report will not be generated for anonymous pipeline");
            return;
        }

        PipelineTransformationReport report = new PipelineTransformationReport();
        report.setName(name);
        String version = transConf.getVersion();

        List<TransformationStepConfig> stepConfigs = transConf.getSteps();
        List<TransformationStepReport> stepReports = new ArrayList<TransformationStepReport>();
        for (int i = 0; i < steps.length; i++) {
            TransformationStepReport stepReport = new TransformationStepReport();
            stepReports.add(stepReport);
            TransformStep step = steps[i];
            Source[] baseSources = step.getBaseSources();
            List<String> baseVersions = step.getBaseVersions();
            TransformationStepConfig stepConfig = stepConfigs.get(i);
            stepReport.setTransformer(stepConfig.getTransformer());
            Source targetSource = step.getTarget();
            String targetVersion = step.getTargetVersion();
            for (int j = 0; j < baseSources.length; j++) {
                Source baseSource = baseSources[j];
                stepReport.addBaseSource(baseSource.getSourceName(), baseVersions.get(j));
            }

            stepReport.setElapsedTime(step.getElapsedTime());
            if (hdfsSourceEntityMgr.checkSourceExist(targetSource, targetVersion)) {
                stepReport.setExecuted(true);
                Long targetRecords = hdfsSourceEntityMgr.count(targetSource, targetVersion);
                stepReport.setTargetSource(targetSource.getSourceName(), targetVersion, targetRecords);
            }
        }
        report.setSteps(stepReports);
        hdfsSourceEntityMgr.saveReport(pipelineSource, name, version, JsonUtils.serialize(report));
    }


    private void reportPipelineExecToDB(TransformationProgress progress, PipelineTransformationConfiguration transConf, TransformStep[] steps) {
        String name = transConf.getName();
        if (name == null) {
            log.info("Report will not be generated for anonymous pipeline");
            return;
        }

        String version = transConf.getVersion();
        if (version == null) {
            log.info("Report will not be generated for pipeline execution without version");
            return;
        }

        reportEntityMgr.deleteReport(name, version);
        List<TransformationStepConfig> stepConfigs = transConf.getSteps();
        for (int i = 0; i < steps.length; i++) {
            PipelineTransformationReportByStep stepReport = new PipelineTransformationReportByStep();

            stepReport.setPipeline(name);
            stepReport.setVersion(version);
            stepReport.setStep(i);

            TransformStep step = steps[i];
            Source[] baseSources = step.getBaseSources();
            List<String> baseVersions = step.getBaseVersions();
            TransformationStepConfig stepConfig = stepConfigs.get(i);
            stepReport.setTransformer(stepConfig.getTransformer());

            String baseSourceNames = "";
            String baseSourceVersions = "";
            for (int j = 0; j < baseSources.length; j++) {
                Source baseSource = baseSources[j];
                baseSourceNames = baseSourceNames + baseSource.getSourceName();
                baseSourceVersions = baseSourceVersions + baseVersions.get(j);
            }
            stepReport.setBaseSources(baseSourceNames);
            stepReport.setBaseVersions(baseSourceVersions);

            Source targetSource = step.getTarget();
            String targetVersion = step.getTargetVersion();
            if (hdfsSourceEntityMgr.checkSourceExist(targetSource, targetVersion)) {
                stepReport.setExecuted(true);
                Long targetRecords = hdfsSourceEntityMgr.count(targetSource, targetVersion);
                stepReport.setTargetSource(targetSource.getSourceName(), targetVersion, targetRecords);
                stepReport.setTempTarget(isTempSource(targetSource));
                stepReport.setElapsedTime(step.getElapsedTime());
            }
            reportEntityMgr.insertReportByStep(stepReport);
        }
    }

    @Override
    public PipelineTransformationConfiguration createTransformationConfiguration(List<String> baseVersions, String targetVersion) {
        return null;
    }

    public PipelineTransformationConfiguration createTransformationConfiguration(PipelineTransformationRequest inputRequest) {

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
                if  (requestJson != null) {
                     request = JsonUtils.deserialize(requestJson, PipelineTransformationRequest.class);
                     steps = request.getSteps();
                }
            } catch (Exception e) {
                log.error("Failed to load pipeline template " + pipelineName + " from hdfs", e);
                steps = null;
            }
        }

        if ((steps == null) || (steps.size() == 0)) {
            log.info("Invalid pipeline " + pipelineName + " without steps");
            return null;
        }

        int currentStep = 0;
        for (TransformationStepConfig step : steps) {
            Transformer transformer = transformerService.findTransformerByName(step.getTransformer());
            if (transformer == null) {
                log.error("Transformer " + step.getTransformer() + " does not exist");
                return null;
            }

            List<Integer> inputSteps = step.getInputSteps();
            if (inputSteps != null) {
                for (Integer inputStep : inputSteps) {
                    if (inputStep < 0) {
                        log.error("Input Step " + currentStep + " uses invalid step " + inputStep);
                        return null;
                    } else if (inputStep >= currentStep) {
                        log.error("Step " + currentStep + " uses future step " + inputStep + " result");
                        return null;
                    }
                }
            }

            List<String> baseSourceNames = step.getBaseSources();
            if (((baseSourceNames == null) || (baseSourceNames.size() == 0)) &&
                ((inputSteps == null) || (inputSteps.size() == 0))) {
                log.error("Step " + currentStep + " does not have any input source specified");
                return null;
            }

            List<String> baseVersions = step.getBaseVersions();
            if ((baseVersions != null) && (baseVersions.size() != baseSourceNames.size())) {
                log.error("Base versions(" + baseVersions.size() + ") does match base sources(" + baseSourceNames.size() + ")");
                return null;
            }


            List<String> baseTemplates = step.getBaseTemplates();
            if (baseTemplates != null) {
                if (baseTemplates.size() != baseSourceNames.size()) {
                    log.error("Base templates(" + baseTemplates.size() + ") does match base sources(" + baseSourceNames.size() + ")");
                    return null;
                } else {
                    for (String sourceName : baseTemplates) {
                        Source source = sourceService.findBySourceName(sourceName);
                        if (source == null) {
                            log.error("Base template" + sourceName + "does not exist");
                            return null;
                        }
                    }
                }
            }

            String targetTemplate = step.getTargetTemplate();
            if (targetTemplate != null) {
                Source source = sourceService.findBySourceName(targetTemplate);
                if (source == null) {
                    log.error("targetTemplate source " + targetTemplate + "does not exist");
                    return null;
                }
            }

            String config = step.getConfiguration();
            List<String> sourceNames;
            if (inputSteps == null) {
                sourceNames = baseSourceNames;
            } else {
                sourceNames = new ArrayList<String>();
                for (int i = 0; i < inputSteps.size(); i++) {
                    sourceNames.add(getTempSourceName(pipelineName, version, i));
                }
                if (baseSourceNames != null) {
                    for (String sourceName : baseSourceNames) {
                        sourceNames.add(sourceName);
                    }
                }
            }
            if (transformer.validateConfig(config, sourceNames) == false) {
                log.error("Invalid configuration for step " + currentStep);
                return null;
            }
            currentStep++;
        }

        PipelineTransformationConfiguration configuration = new PipelineTransformationConfiguration();
        configuration.setName(pipelineName);
        configuration.setVersion(version);
        configuration.setServiceBeanName(getServiceBeanName());
        configuration.setKeepTemp(inputRequest.getKeepTemp());
        configuration.setSteps(steps);

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

    private class TransformStep {

        private String config;
        private Transformer transformer;
        private Source[] baseSources;
        private List<String> baseVersions;
        private Source[] baseTemplates;
        private Source target;
        private String targetVersion;
        private Source targetTemplate;
        private long elapsedTime;

        public TransformStep(Transformer transformer, Source[] baseSources, List<String> baseVersions,
                             Source[] baseTemplates, Source target, String targetVersion, Source targetTemplate,
                             String config) {
            this.transformer = transformer;
            this.config = config;
            this.baseSources = baseSources;
            this.baseVersions = baseVersions;
            this.baseTemplates = baseTemplates;
            this.target = target;
            this.targetVersion = targetVersion;
            this.targetTemplate = targetTemplate;
        }

        public Transformer getTransformer() {
            return transformer;
        }

        public String getConfig() {
            return config;
        }

        public Source[] getBaseSources() {
            return baseSources;
        }

        public List<String> getBaseVersions() {
            return baseVersions;
        }

        public Source[] getBaseTemplates() {
            return baseTemplates;
        }

        public Source getTarget() {
            return target;
        }

        public Source getTargetTemplate() {
            return targetTemplate;
        }

        public String getTargetVersion() {
            return targetVersion;
        }

        public long getElapsedTime() {
            return elapsedTime;
        }

        public void setElapsedTime(long elapsedTime) {
            this.elapsedTime = elapsedTime;
        }
    }
}
