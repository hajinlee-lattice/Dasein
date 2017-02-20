package com.latticeengines.datacloud.etl.transformation.service.impl;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.latticeengines.datacloud.core.util.HdfsPodContext;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.time.DurationFormatUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;
import org.springframework.web.client.RestTemplate;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.latticeengines.common.exposed.util.HttpClientUtils;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.datacloud.core.source.Source;
import com.latticeengines.datacloud.core.source.impl.IngestionSource;
import com.latticeengines.datacloud.core.source.impl.PipelineSource;
import com.latticeengines.datacloud.etl.service.SourceService;
import com.latticeengines.datacloud.etl.transformation.entitymgr.PipelineTransformationReportEntityMgr;
import com.latticeengines.datacloud.etl.transformation.service.TransformationService;
import com.latticeengines.datacloud.etl.transformation.service.TransformerService;
import com.latticeengines.datacloud.etl.transformation.transformer.TransformStep;
import com.latticeengines.datacloud.etl.transformation.transformer.Transformer;
import com.latticeengines.domain.exposed.datacloud.manage.PipelineTransformationReportByStep;
import com.latticeengines.domain.exposed.datacloud.manage.TransformationProgress;
import com.latticeengines.domain.exposed.datacloud.transformation.PipelineTransformationReport;
import com.latticeengines.domain.exposed.datacloud.transformation.PipelineTransformationRequest;
import com.latticeengines.domain.exposed.datacloud.transformation.TransformationStepConfig;
import com.latticeengines.domain.exposed.datacloud.transformation.TransformationStepReport;
import com.latticeengines.domain.exposed.datacloud.transformation.configuration.impl.IngestedFileToSourceTransformerConfig;
import com.latticeengines.domain.exposed.datacloud.transformation.configuration.impl.PipelineTransformationConfiguration;

/**
 * This transformation service allows parameterizing the source and target,
 * pipelining the tranformation process.
 */
@Component("pipelineTransformationService")
public class PipelineTransformationService extends AbstractTransformationService<PipelineTransformationConfiguration>
        implements TransformationService<PipelineTransformationConfiguration> {

    private static final Log log = LogFactory.getLog(PipelineTransformationService.class);
    private static final String SLACK_BOT = "PipelineTransformer";
    private static final String SLACK_COLOR_GOOD = "good";
    private static final String SLACK_COLOR_DANGER = "danger";
    private static final ObjectMapper OM = new ObjectMapper();

    @Autowired
    private PipelineSource pipelineSource;

    @Autowired
    private SourceService sourceService;

    @Autowired
    private TransformerService transformerService;

    @Autowired
    private PipelineTransformationReportEntityMgr reportEntityMgr;

    @Value("${datacloud.slack.webhook.url}")
    private String slackWebhookUrl;

    @Value("${common.le.environment}")
    private String leEnv;

    @Value("${common.le.stack}")
    private String leStack;

    private RestTemplate slackRestTemplate = HttpClientUtils.newSSLEnforcedRestTemplate();

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
    protected TransformationProgress transformHook(TransformationProgress progress,
            PipelineTransformationConfiguration transConf) {

        boolean succeeded = false;
        String workflowDir = initialDataFlowDirInHdfs(progress);

        cleanupWorkflowDir(progress, workflowDir);

        TransformStep[] steps = initiateTransformSteps(progress, transConf);
        if (steps == null) {
            updateStatusToFailed(progress, "Failed to initiate transform steps", null);
        } else {
            succeeded = executeTransformSteps(progress, steps, workflowDir, transConf);
            if (!transConf.getKeepTemp()) {
                cleanupTempSources(steps);
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
        return new ArrayList<>();
    }

    private void cleanupWorkflowDir(TransformationProgress progress, String workflowDir) {
        deleteFSEntry(progress, workflowDir + "/*");
    }

    private void cleanupTempSources(TransformStep[] steps) {

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
                    if (source instanceof IngestionSource) {
                        try {
                            IngestedFileToSourceTransformerConfig ingestedFileToSourceTransformerConfig = new ObjectMapper()
                                    .readValue(config.getConfiguration(), IngestedFileToSourceTransformerConfig.class);
                            ((IngestionSource) source)
                                    .setIngetionName(ingestedFileToSourceTransformerConfig.getIngetionName());
                        } catch (IOException e) {
                            updateStatusToFailed(progress, "Failed to parse IngestedFileToSourceTransformerConfig "
                                    + config.getConfiguration(), null);
                            return null;
                        }

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
            log.info("Step " + stepIdx + " target " + target.getSourceName() + " template "
                    + targetTemplate.getSourceName());

            String confStr = config.getConfiguration();
            TransformStep step = new TransformStep(String.valueOf(stepIdx), transformer, baseSources, baseVersions, baseTemplates, target,
                    targetVersion, targetTemplate, confStr);
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
            sendSlack(transConf.getName() + " [" + progress.getYarnAppId() + "]", slackMessage, "", transConf);
            TransformStep step = steps[i];
            Transformer transformer = step.getTransformer();
            try {
                log.info("Transforming step " + i);
                if (hdfsSourceEntityMgr.checkSourceExist(step.getTarget(), step.getTargetVersion())) {
                    step.setElapsedTime(0);
                    log.info("Skip executed step " + i);
                } else {
                    long startTime = System.currentTimeMillis();
                    boolean succeeded = transformer.transform(progress, workflowDir, step);
                    long stepDuration = System.currentTimeMillis() - startTime;
                    step.setElapsedTime(stepDuration / 1000);
                    if (!succeeded) {
                        // failed message
                        slackMessage = String.format("Failed at step %d after %s :sob:", i,
                                DurationFormatUtils.formatDurationHMS(stepDuration));
                        sendSlack(transConf.getName() + " [" + progress.getYarnAppId() + "]", slackMessage,
                                SLACK_COLOR_DANGER, transConf);
                        updateStatusToFailed(progress, "Failed to transform data at step " + i, null);
                        return false;
                    }
                    // success message
                    slackMessage = String.format("Step %d finished after %s :smile:", i,
                            DurationFormatUtils.formatDurationHMS(stepDuration));
                    sendSlack(transConf.getName() + " [" + progress.getYarnAppId() + "]", slackMessage, SLACK_COLOR_GOOD,
                            transConf);

                    if (i == steps.length - 1) {
                        slackMessage = String.format("All %d steps in the pipeline are finished after %s :clap:", steps.length,
                                DurationFormatUtils.formatDurationHMS(System.currentTimeMillis() - pipelineStarTime));
                        sendSlack(transConf.getName() + " [" + progress.getYarnAppId() + "]", slackMessage, SLACK_COLOR_GOOD,
                                transConf);
                    }

                    saveSourceVersion(progress, step.getTarget(), step.getTargetVersion(), workflowDir);
                    cleanupWorkflowDir(progress, workflowDir);
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
                updateStatusToFailed(progress, "Failed to transform data at step " + i, e);
                return false;
            }
        }
        return true;
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
        if (hdfsSourceEntityMgr.checkSourceExist(targetSource, targetVersion)) {
            stepReport.setExecuted(true);
            Long targetRecords = step.getCount();
            stepReport.setTargetSource(targetSource.getSourceName(), targetVersion, targetRecords);
        }
        return stepReport;
    }

    private void reportStepToDB(String pipelineName, String pipelineVersion, TransformStep step, TransformationStepConfig stepConfig) {
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
        }
        stepReport.setBaseSources(baseSourceNames);
        stepReport.setBaseVersions(baseSourceVersions);

        Source targetSource = step.getTarget();
        String targetVersion = step.getTargetVersion();
        if (hdfsSourceEntityMgr.checkSourceExist(targetSource, targetVersion)) {
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
            if (((baseSourceNames == null) || (baseSourceNames.size() == 0))
                    && ((inputSteps == null) || (inputSteps.size() == 0))) {
                log.error("Step " + currentStep + " does not have any input source specified");
                return null;
            }

            List<String> baseVersions = step.getBaseVersions();
            if ((baseVersions != null) && (baseVersions.size() != baseSourceNames.size())) {
                log.error("Base versions(" + baseVersions.size() + ") does match base sources(" + baseSourceNames.size()
                        + ")");
                return null;
            }

            List<String> baseTemplates = step.getBaseTemplates();
            if (baseTemplates != null) {
                if (baseTemplates.size() != baseSourceNames.size()) {
                    log.error("Base templates(" + baseTemplates.size() + ") does match base sources("
                            + baseSourceNames.size() + ")");
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
        configuration.setEnableSlack(inputRequest.isEnableSlack());

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

    private void sendSlack(String title, String text, String color, PipelineTransformationConfiguration transConf) {
        if (StringUtils.isNotEmpty(slackWebhookUrl) && transConf.isEnableSlack()) {
            try {
                String payload = slackPayload(title, text, color);
                slackRestTemplate.postForObject(slackWebhookUrl, payload, String.class);
            } catch (Exception e) {
                log.error("Failed to send slack message.", e);
            }
        }
    }

    private String slackPayload(String title, String text, String color) {
        ObjectNode objectNode = OM.createObjectNode();
        objectNode.put("username", SLACK_BOT);
        ArrayNode attachments = OM.createArrayNode();
        ObjectNode attachment = OM.createObjectNode();
        String pretext = "[" + leEnv + "-" + leStack + "]";
        if (SLACK_COLOR_DANGER.equals(color)) {
            pretext = "<!channel> " + pretext;
        }
        attachment.put("pretext", pretext);
        if (StringUtils.isNotEmpty(color)) {
            attachment.put("color", color);
        }
        if (StringUtils.isNotEmpty(title)) {
            attachment.put("title", title);
        }
        attachment.put("text", text);
        attachments.add(attachment);
        objectNode.put("attachments", attachments);
        return JsonUtils.serialize(objectNode);
    }
}
