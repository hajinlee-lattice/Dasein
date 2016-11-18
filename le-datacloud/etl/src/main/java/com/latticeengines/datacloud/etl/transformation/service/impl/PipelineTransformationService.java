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
import com.latticeengines.datacloud.etl.transformation.transformer.Transformer;
import com.latticeengines.datacloud.etl.transformation.service.TransformerService;
import com.latticeengines.datacloud.etl.transformation.service.TransformationService;
import com.latticeengines.domain.exposed.datacloud.manage.TransformationProgress;
import com.latticeengines.domain.exposed.datacloud.transformation.configuration.impl.PipelineTransformationConfiguration;
import com.latticeengines.domain.exposed.datacloud.transformation.PipelineTransformationRequest;
import com.latticeengines.domain.exposed.datacloud.transformation.TransformationStepConfig;

/**
 * This transformation service allows paramterizing the source and target, pipelining the tranformation process.
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

    private final String TEMP_SOURCE = "temp_source_";

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
        String workflowDir = initialDataFlowDirInHdfs(progress);
        if (!cleanupHdfsDir(workflowDir, progress)) {
            updateStatusToFailed(progress, "Failed to cleanup HDFS path " + workflowDir, null);
            return null;
        }

        Map<String, Source> sourceMap = new HashMap<String, Source>();

        Source[] baseSources = null;
        Source[] baseTemplates = null;
        List<String> baseVersions = transConf.getBaseVersions();

        List<String> baseSourceNames = transConf.getBaseSources();
        if (baseSourceNames.size() != 0) {
            baseSources = new Source[baseSourceNames.size()];
            for (int i = 0; i < baseSources.length; i++) {
                String sourceName = baseSourceNames.get(i);
                Source source = sourceService.findBySourceName(sourceName);
                if (source == null) {
                    updateStatusToFailed(progress, "Base source " + sourceName + " not found", null);
                    return null;
                }
                sourceMap.put(source.getSourceName(), source);
                baseSources[i] = source;
            }

            baseTemplates = new Source[baseSourceNames.size()];
            List<String> baseTemplateNames = transConf.getBaseTemplates();
            if (baseTemplateNames == null) {
                baseTemplateNames = baseSourceNames;
            }
            for (int i = 0; i < baseTemplateNames.size(); i++) {
                String sourceName = baseTemplateNames.get(i);
                Source source = sourceService.findBySourceName(sourceName);
                if (source == null) {
                    updateStatusToFailed(progress, "Base source " + sourceName + " not found", null);
                    return null;
                }
                baseTemplates[i] = source;
            }

            if (baseVersions == null) {
                baseVersions = new ArrayList<String>();
                for (int i = 0; i < baseSources.length; i++) {
                    String latestBaseVersion = hdfsSourceEntityMgr.getCurrentVersion(baseSources[i]);
                    baseVersions.add(latestBaseVersion);
                }
            } else if (baseVersions.size() != baseSources.length) {
                updateStatusToFailed(progress, "Number of base versions is different with number of base sources.",
                                     null);
                return null;
            }
        }

        String targetName = transConf.getTargetSource();
        Source target = sourceService.findOrCreateSource(targetName);

        String targetVersion = transConf.getTargetVersion();
        if (targetVersion == null) {
            if (baseSources != null) {
                targetVersion = baseVersions.get(0);
             } else {
                targetVersion = createNewVersionStringFromNow();
             }
        }

        Source targetTemplate = target;
        String targetTemplateName = transConf.getTargetTemplate();
        if (targetTemplateName != null) {
            targetTemplate = sourceService.findBySourceName(targetTemplateName);
        }



        TransformStep[] steps = null;
        steps = initiateTransformSteps(progress, transConf, baseSources, baseVersions, baseTemplates,
                                       target, targetVersion, targetTemplate, sourceMap);
        boolean succeeded = false;
        if (steps == null) {
            updateStatusToFailed(progress, "Failed to initiate transfom steps", null);
        } else {
            succeeded = executeTransformSteps(progress, steps, workflowDir);
            cleanupTempSources(progress, steps);
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

    private String getTempSourceName(TransformationProgress progress, int step) {
        return TEMP_SOURCE + progress.getRootOperationUID() + STEP + step;
    }

    private boolean isTempSource(Source source) {
        return source.getSourceName().startsWith(TEMP_SOURCE);
    }

    private TransformStep[] initiateTransformSteps(TransformationProgress progress,
                                           PipelineTransformationConfiguration transConf,
                                           Source[] baseSources, List<String> baseVersions, Source[] baseTemplates,
                                           Source target, String targetVersion, Source targetTemplate,
                                           Map<String, Source> sourceMap) {

        List<TransformationStepConfig> stepConfigs = transConf.getSteps();
        TransformStep[] steps = new TransformStep[stepConfigs.size()];

        for (int i = stepConfigs.size() - 1; i >= 0; i--) {
            TransformationStepConfig config = stepConfigs.get(i);
            if (config.getTargetSource() == null) {
                config.setTargetSource(target.getSourceName());
                config.setTargetVersion(targetVersion);
                break;
            }
        }

        for (int i = 0; i < steps.length; i++) {
            TransformationStepConfig config = stepConfigs.get(i);
            Transformer transformer = transformerService.findTransformerByName(config.getTransformer());

            if (transformer == null) {
                log.error("Failed to find transformer " + config.getTransformer());
                return null;
            }

            Source stepTargetTemplate = targetTemplate;

            Source[] stepInputs = baseSources;
            List<String> stepInputVersions = baseVersions;
            Source[] stepInputTemplates = baseTemplates;

            Integer inputStep = config.getInputStep();

            if (inputStep != null) {
                stepInputs = new Source[1];
                stepInputs[0] = steps[inputStep].getTarget();
                stepInputVersions = new ArrayList<String>();
                stepInputVersions.add(steps[inputStep].getTargetVersion());
                stepInputTemplates = new Source[1];
                stepInputTemplates[0] = steps[inputStep].getTargetTemplate();
                stepTargetTemplate = steps[inputStep].getTargetTemplate();
            }


            Source stepTarget = null;
            String targetName = config.getTargetSource();
            if (targetName == null) {
                targetName = getTempSourceName(progress, i);
                stepTarget = sourceService.createSource(targetName);
            } else {
                stepTarget = sourceService.findOrCreateSource(targetName);
                sourceMap.put(targetName, stepTarget);
                stepTargetTemplate = stepTarget;
            }

            String targetTemplateName = config.getTargetTemplate();
            if (targetTemplateName != null) {
                stepTargetTemplate = sourceService.findBySourceName(targetTemplateName);
            }

            log.info("step " + i + " target " + stepTarget.getSourceName() + " template " + stepTargetTemplate.getSourceName());

            String stepTargetVersion = config.getTargetVersion();
            if (stepTargetVersion == null) {
                if ((sourceMap.get(targetName) == null) & (stepInputs != null)) {
                    stepTargetVersion = stepInputVersions.get(0);
                } else {
                    stepTargetVersion = createNewVersionStringFromNow();
                }
            }

            String confStr = config.getConfiguration();
            TransformStep step = new TransformStep(transformer, stepInputs, stepInputVersions, stepInputTemplates, stepTarget, stepTargetVersion, stepTargetTemplate, confStr);
            steps[i] = step;
        }
        return steps;
    }

    private boolean executeTransformSteps(TransformationProgress progress, TransformStep[] steps, String workflowDir) {
        for (int i = 0; i < steps.length; i++) {
            TransformStep step = steps[i];
            Transformer transformer = step.getTransformer();
            try {
                log.info("Transforming step " + i);
                boolean succeeded = transformer.transform(progress, workflowDir, step.getBaseSources(), step.getBaseVersions(),
                                                            step.getBaseTemplates(), step.getTargetTemplate(),
                                                            step.getConfig());
                if (!succeeded) {
                    updateStatusToFailed(progress, "Failed to transform data at step " + i, null);
                    return false;
                }
                saveSourceVersion(progress, step.getTarget(), step.getTargetVersion(), workflowDir);
                cleanupWorkflowDir(progress, workflowDir);
            } catch (Exception e) {
                updateStatusToFailed(progress, "Failed to transform data at step " + i, e);
                return false;
            }
        }
        return true;
    }

    @Override
    public PipelineTransformationConfiguration createTransformationConfiguration(List<String> baseVersions, String targetVersion) {
        return null;
    }

    public PipelineTransformationConfiguration createTransformationConfiguration(PipelineTransformationRequest request) {

        log.info("Creating Pipeline configuration");

        List<String> baseSourceNames = request.getBaseSources();
        if (baseSourceNames == null) {
            log.error("Base sources attribute must be specified even in case of 0 base source ");
            return null;
        }

        for (String sourceName : baseSourceNames) {
            Source source = sourceService.findBySourceName(sourceName);
            if (source == null) {
                log.error("Base source " + sourceName + "does not exist");
                return null;
            }
        }
        List<String> baseVersions = request.getBaseVersions();
        if ((baseVersions != null) && (baseVersions.size() != baseSourceNames.size())) {
            log.error("Base versions(" + baseVersions.size() + ") does match base sources(" + baseSourceNames.size() + ")");
            return null;
        }


        List<String> baseTemplates = request.getBaseTemplates();
        if (baseTemplates == null) {
            baseTemplates = baseSourceNames;
        } else if (baseTemplates.size() != baseSourceNames.size()) {
            log.error("Base templates(" + baseTemplates.size() + ") does match base sources(" + baseSourceNames.size() + ")");
            return null;
        } else {
            for (String sourceName : baseTemplates) {
                 Source source = sourceService.findBySourceName(sourceName);
                 if (source == null) {
                     log.error("Base source " + sourceName + "does not exist");
                     return null;
                 }
            }
        }

        String targetSourceName = request.getTargetSource();
        if (targetSourceName == null) {
            log.error("Target source must be specified");
            return null;
        }

        String targetTemplate = request.getTargetTemplate();
        if (targetTemplate == null) {
            targetTemplate = targetSourceName;
        }

        String targetVersion = request.getTargetVersion();
        List<TransformationStepConfig> steps = request.getSteps();
        int i = 1;
        for (TransformationStepConfig step : steps) {
            Transformer transformer = transformerService.findTransformerByName(step.getTransformer());
            if (transformer == null) {
                log.error("Transformer " + step.getTransformer() + " does not exist");
                return null;
            }

            Integer input = step.getInputStep();
            if ((input != null) && (input >= i)) {
                log.error("Step " + i + " uses future step " + input + " result");
                return null;
            }
            String config = step.getConfiguration();
            if (transformer.validateConfig(config, baseSourceNames) == false) {
                log.error("Invalid configuration for step " + i);
                return null;
            }
            i++;
        }

        PipelineTransformationConfiguration configuration = new PipelineTransformationConfiguration();
        configuration.setBaseSources(baseSourceNames);
        configuration.setBaseVersions(baseVersions);

        for (String template : baseTemplates) {
            log.info("Base Template " + template);
        }

        configuration.setBaseTemplates(baseTemplates);
        configuration.setTargetSource(targetSourceName);
        configuration.setTargetVersion(targetVersion);
        configuration.setTargetTemplate(targetTemplate);

        configuration.setServiceBeanName(getServiceBeanName());
        configuration.setVersion(createNewVersionStringFromNow());
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
    }
}
