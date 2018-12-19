package com.latticeengines.modelquality.service.impl;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.io.Resource;
import org.springframework.data.hadoop.fs.HdfsResourceLoader;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.MediaType;
import org.springframework.stereotype.Component;
import org.springframework.util.LinkedMultiValueMap;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.latticeengines.common.exposed.util.HdfsUtils;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.ResponseDocument;
import com.latticeengines.domain.exposed.modeling.factory.AlgorithmFactory;
import com.latticeengines.domain.exposed.modeling.factory.DataFlowFactory;
import com.latticeengines.domain.exposed.modeling.factory.PropDataFactory;
import com.latticeengines.domain.exposed.modelquality.AnalyticPipeline;
import com.latticeengines.domain.exposed.modelquality.DataSet;
import com.latticeengines.domain.exposed.modelquality.ModelRun;
import com.latticeengines.domain.exposed.modelquality.ModelRunEntityNames;
import com.latticeengines.domain.exposed.modelquality.SelectedConfig;
import com.latticeengines.domain.exposed.monitor.metric.MetricDB;
import com.latticeengines.domain.exposed.pls.ModelSummary;
import com.latticeengines.domain.exposed.pls.ModelSummaryMetrics;
import com.latticeengines.domain.exposed.pls.ModelingParameters;
import com.latticeengines.domain.exposed.pls.SchemaInterpretation;
import com.latticeengines.domain.exposed.pls.SourceFile;
import com.latticeengines.domain.exposed.pls.frontend.FieldMapping;
import com.latticeengines.domain.exposed.pls.frontend.FieldMappingDocument;
import com.latticeengines.domain.exposed.workflow.Job;
import com.latticeengines.domain.exposed.workflow.JobStatus;
import com.latticeengines.modelquality.entitymgr.ModelSummaryMetricsEntityMgr;
import com.latticeengines.modelquality.metrics.ModelQualityMetrics;
import com.latticeengines.modelquality.metrics.ModelingMeasurement;
import com.latticeengines.proxy.exposed.workflowapi.WorkflowProxy;

@Component("fileModelRunService")
public class FileModelRunServiceImpl extends AbstractModelRunServiceImpl {

    private static final String FILE_KEY = "file";

    private static final Logger log = LoggerFactory.getLogger(FileModelRunServiceImpl.class);

    @Autowired
    private Configuration distCpConfiguration;

    @Autowired
    private WorkflowProxy workflowProxy;

    @Autowired
    private ModelSummaryMetricsEntityMgr modelSummaryMetricsEntityMgr;

    @Override
    protected void runModel(ModelRun modelRun) {
        AnalyticPipeline analyticPipeline = modelRun.getAnalyticPipeline();
        DataSet dataset = modelRun.getDataSet();

        ModelRunEntityNames modelRunEntityNames = new ModelRunEntityNames(modelRun);

        SelectedConfig selectedConfig = new SelectedConfig();
        selectedConfig.setPipeline(analyticPipeline.getPipeline());
        selectedConfig.setAlgorithm(analyticPipeline.getAlgorithm());
        selectedConfig.setDataSet(dataset);
        selectedConfig.setPropData(analyticPipeline.getPropData());
        selectedConfig.setDataFlow(analyticPipeline.getDataFlow());
        selectedConfig.setSampling(analyticPipeline.getSampling());

        SourceFile sourceFile = uploadFile(dataset);

        ModelingParameters parameters = new ModelingParameters();
        parameters.setName(modelRun.getName());
        parameters.setDisplayName(modelRun.getName());
        parameters.setDescription(modelRun.getDescription());
        parameters.setFilename(sourceFile.getName());
        configModelingParams(selectedConfig, parameters);

        resolveMetadata(parameters, sourceFile);
        createModel(parameters);
        ModelSummary modelSummary = retrieveModelSummary(modelRun.getName());
        log.info(String.format("ModelSummaryID: %s", modelSummary.getId()));
        modelRun.setModelId(modelSummary.getId());
        saveMetricsToReportDB(modelSummary, selectedConfig, modelRunEntityNames);
    }

    public SourceFile uploadFile(DataSet dataSet) {
        SchemaInterpretation schemaInterpretation = dataSet.getSchemaInterpretation();

        if (schemaInterpretation == null) {
            schemaInterpretation = SchemaInterpretation.SalesforceLead;
        }

        Resource resource;
        try (HdfsResourceLoader resourceLoader = new HdfsResourceLoader(
                HdfsUtils.getFileSystem(distCpConfiguration, dataSet.getTrainingSetHdfsPath()))) {
            resource = resourceLoader.getResource(dataSet.getTrainingSetHdfsPath());
        } catch (IOException ex) {
            log.error("Failed to load file!", ex);
            throw new RuntimeException(ex);
        }

        LinkedMultiValueMap<String, Object> map = new LinkedMultiValueMap<>();
        map.add(FILE_KEY, resource);
        HttpHeaders headers = new HttpHeaders();
        headers.setContentType(MediaType.MULTIPART_FORM_DATA);

        String fileName = StringUtils.substringAfterLast(dataSet.getTrainingSetHdfsPath(), "/");
        HttpEntity<LinkedMultiValueMap<String, Object>> requestEntity = new HttpEntity<>(map, headers);
        @SuppressWarnings("rawtypes")
        ResponseDocument response = getRestTemplate().postForObject(String
                .format("%s/pls/models/uploadfile/unnamed?displayName=%s", getDeployedRestAPIHostPort(), fileName),
                requestEntity, ResponseDocument.class);
        SourceFile sourceFile = new ObjectMapper().convertValue(response.getResult(), SourceFile.class);
        sourceFile.setSchemaInterpretation(schemaInterpretation);

        log.info(sourceFile.getName());

        return sourceFile;
    }

    public void resolveMetadata(ModelingParameters parameters, SourceFile sourceFile) {

        @SuppressWarnings("rawtypes")
        ResponseDocument response = getRestTemplate().postForObject(
                String.format("%s/pls/models/uploadfile/%s/fieldmappings?schema=%s", getDeployedRestAPIHostPort(),
                        sourceFile.getName(), sourceFile.getSchemaInterpretation().name()),
                parameters, ResponseDocument.class);
        FieldMappingDocument mappings = new ObjectMapper().convertValue(response.getResult(),
                FieldMappingDocument.class);

        for (FieldMapping mapping : mappings.getFieldMappings()) {
            if (mapping.getMappedField() == null) {
                mapping.setMappedToLatticeField(false);
                mapping.setMappedField(mapping.getUserField().replace(' ', '_'));
            }
        }
        log.info("the fieldmappings are: " + mappings.getFieldMappings());

        getRestTemplate().postForObject(String.format("%s/pls/models/uploadfile/fieldmappings?displayName=%s",
                getDeployedRestAPIHostPort(), sourceFile.getName()), mappings, Void.class);
    }

    private void configModelingParams(SelectedConfig selectedConfig, ModelingParameters parameters) {
        String configJson = JsonUtils.serialize(selectedConfig);
        log.info(String.format("SelectedConfig:\n%s", configJson));
        Map<String, String> runTimeParams = new HashMap<>();
        runTimeParams.put(AlgorithmFactory.MODEL_CONFIG, configJson);
        runTimeParams.put(DataFlowFactory.DATAFLOW_DO_SORT_FOR_ATTR_FLOW, "true");
        parameters.setRunTimeParams(runTimeParams);

        DataFlowFactory.configDataFlow(selectedConfig, parameters);
        PropDataFactory.configPropData(selectedConfig, parameters);
    }

    @SuppressWarnings("rawtypes")
    private void createModel(ModelingParameters parameters) {
        ResponseDocument response;
        response = getRestTemplate().postForObject(
                String.format("%s/pls/models/%s", getDeployedRestAPIHostPort(), parameters.getName()), parameters,
                ResponseDocument.class);
        String modelingWorkflowApplicationId = new ObjectMapper().convertValue(response.getResult(), String.class);

        log.info(String.format("Workflow application id is %s", modelingWorkflowApplicationId));

        JobStatus completedStatus = waitForWorkflowStatus(modelingWorkflowApplicationId, false);
        log.info("Job Status=" + completedStatus.toString());
        if (completedStatus != JobStatus.COMPLETED) {
            throw new RuntimeException("Job was not completed! application id=" + modelingWorkflowApplicationId);
        }
    }

    public ModelSummary retrieveModelSummary(String modelName) {
        try {
            ModelSummary modelSummary = getModelSummary(modelName);
            String modelId = modelSummary.getId();
            log.info("modeling id: " + modelId);
            return modelSummary;
        } catch (Exception ex) {
            throw new RuntimeException(ex);
        }
    }

    private ModelSummary getModelSummary(String modelName) throws InterruptedException {
        ModelSummary found = null;
        // Wait for model downloader
        while (true) {
            @SuppressWarnings("unchecked")
            List<Object> summaries = getRestTemplate().getForObject( //
                    String.format("%s/pls/modelsummaries", getDeployedRestAPIHostPort()), List.class);
            for (Object rawSummary : summaries) {
                ModelSummary summary = new ObjectMapper().convertValue(rawSummary, ModelSummary.class);
                if (summary.getName().contains(modelName)) {
                    found = summary;
                }
            }
            if (found != null) {
                break;
            }
            Thread.sleep(1000);
        }

        Object rawSummary = getRestTemplate().getForObject(
                String.format("%s/pls/modelsummaries/%s", getDeployedRestAPIHostPort(), found.getId()), Object.class);
        return JsonUtils.convertValue(rawSummary, ModelSummary.class);
    }

    private JobStatus waitForWorkflowStatus(String applicationId, boolean running) {

        int retryOnException = 10;
        Job job;
        while (true) {
            try {
                job = workflowProxy.getWorkflowJobFromApplicationId(applicationId);
            } catch (Exception e) {
                log.warn("Workflow job exception: " + e.getMessage());
                job = null;
                if (--retryOnException == 0)
                    throw new RuntimeException(e);
            }
            if ((job != null) && ((running && job.isRunning()) || (!running && !job.isRunning()))) {
                return job.getJobStatus();
            }
            try {
                Thread.sleep(30000L);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }
    }

    private ModelSummaryMetrics writeMetricsToSql(ModelSummary modelSummary, SelectedConfig config) {
        String lookupId = modelSummary.getLookupId();
        ModelSummaryMetrics modelSummaryMetrics = new ModelSummaryMetrics();
        modelSummaryMetrics.setTenantName(lookupId.split("\\|")[0]);
        modelSummaryMetrics.setName(config.getDataSet().getName());
        modelSummaryMetrics.setRocScore(modelSummary.getRocScore());
        modelSummaryMetrics.setTop20PercentLift(modelSummary.getTop20PercentLift());
        modelSummaryMetrics.setDataCloudVersion(modelSummary.getDataCloudVersion());
        modelSummaryMetrics.setLastUpdateTime(modelSummary.getLastUpdateTime());
        modelSummaryMetrics.setConstructionTime(modelSummary.getConstructionTime());
        return modelSummaryMetrics;
    }

    private void saveMetricsToReportDB(ModelSummary modelSummary, SelectedConfig config,
            ModelRunEntityNames modelRunEntityNames) {
        modelSummary.setDetails(null);
        log.info("Model Summary=\n" + modelSummary.toString());
        ModelQualityMetrics metrics = new ModelQualityMetrics(modelSummary, config, modelRunEntityNames);
        ModelingMeasurement measurement = new ModelingMeasurement(metrics);
        metricService.write(MetricDB.MODEL_QUALITY, measurement);
        ModelSummaryMetrics modelSummaryMetrics = writeMetricsToSql(modelSummary, config);
        modelSummaryMetricsEntityMgr.create(modelSummaryMetrics);
    }
}
