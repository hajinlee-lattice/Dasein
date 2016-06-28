package com.latticeengines.modelquality.service.impl;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.joda.time.DateTime;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.io.Resource;
import org.springframework.data.hadoop.fs.HdfsResourceLoader;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.MediaType;
import org.springframework.stereotype.Component;
import org.springframework.util.LinkedMultiValueMap;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.ResponseDocument;
import com.latticeengines.domain.exposed.modeling.factory.AlgorithmFactory;
import com.latticeengines.domain.exposed.modelquality.DataSet;
import com.latticeengines.domain.exposed.modelquality.SelectedConfig;
import com.latticeengines.domain.exposed.pls.ModelSummary;
import com.latticeengines.domain.exposed.pls.ModelingParameters;
import com.latticeengines.domain.exposed.pls.SchemaInterpretation;
import com.latticeengines.domain.exposed.pls.SourceFile;
import com.latticeengines.domain.exposed.pls.frontend.FieldMapping;
import com.latticeengines.domain.exposed.pls.frontend.FieldMappingDocument;
import com.latticeengines.domain.exposed.workflow.Job;
import com.latticeengines.domain.exposed.workflow.JobStatus;
import com.latticeengines.proxy.exposed.workflowapi.WorkflowProxy;

@Component("fileModelRunService")
public class FileModelRunServiceImpl extends AbstractModelRunServiceImpl {

    private static final String FILE_KEY = "file";

    private static final Log log = LogFactory.getLog(FileModelRunServiceImpl.class);

    @Autowired
    private Configuration yarnConfiguration;

    @Autowired
    private WorkflowProxy workflowProxy;

    @Override
    protected void runModel(SelectedConfig config) {

        SourceFile sourceFile = uploadFile(config);
        resolveMetadata(config, sourceFile);
        String modelName = createModel(config, sourceFile);
        ModelSummary modelSummary = retrieveModelSummary(modelName);

        saveMetricsToReportDB(modelSummary);
    }

    private void saveMetricsToReportDB(ModelSummary modelSummary) {
        log.info("Model Summary=" + modelSummary.toString());
    }

    public SourceFile uploadFile(SelectedConfig config) {

        DataSet dataSet = config.getDataSet();
        SchemaInterpretation schemaInterpretation = dataSet.getSchemaInterpretation();

        if (schemaInterpretation == null) {
            schemaInterpretation = SchemaInterpretation.SalesforceLead;
        }

        Resource resource = null;
        try (HdfsResourceLoader resourceLoader = new HdfsResourceLoader(FileSystem.newInstance(yarnConfiguration))) {
            resource = resourceLoader.getResource(dataSet.getTrainingSetHdfsPath());
        } catch (IOException ex) {
            log.error("Faield to load file!", ex);
            throw new RuntimeException(ex);
        }

        LinkedMultiValueMap<String, Object> map = new LinkedMultiValueMap<>();
        map.add(FILE_KEY, resource);
        HttpHeaders headers = new HttpHeaders();
        headers.setContentType(MediaType.MULTIPART_FORM_DATA);

        String fileName = StringUtils.substringAfterLast(dataSet.getTrainingSetHdfsPath(), "/");
        HttpEntity<LinkedMultiValueMap<String, Object>> requestEntity = new HttpEntity<>(map, headers);
        @SuppressWarnings("rawtypes")
        ResponseDocument response = restTemplate.postForObject(String.format(
                "%s/pls/models/uploadfile/unnamed?displayName=%s", getDeployedRestAPIHostPort(), fileName),
                requestEntity, ResponseDocument.class);
        SourceFile sourceFile = new ObjectMapper().convertValue(response.getResult(), SourceFile.class);
        log.info(sourceFile.getName());

        return sourceFile;
    }

    public void resolveMetadata(SelectedConfig config, SourceFile sourceFile) {
        DataSet dataSet = config.getDataSet();
        SchemaInterpretation schemaInterpretation = dataSet.getSchemaInterpretation();
        sourceFile.setSchemaInterpretation(schemaInterpretation);
        @SuppressWarnings("rawtypes")
        ResponseDocument response = restTemplate.getForObject(String.format(
                "%s/pls/models/uploadfile/%s/fieldmappings?schema=%s", getDeployedRestAPIHostPort(),
                sourceFile.getName(), schemaInterpretation.name()), ResponseDocument.class);
        FieldMappingDocument mappings = new ObjectMapper().convertValue(response.getResult(),
                FieldMappingDocument.class);

        for (FieldMapping mapping : mappings.getFieldMappings()) {
            if (mapping.getMappedField() == null) {
                mapping.setMappedToLatticeField(false);
                mapping.setMappedField(mapping.getUserField().replace(' ', '_'));
            }
        }
        log.info("the fieldmappings are: " + mappings.getFieldMappings());

        restTemplate.postForObject(String.format("%s/pls/models/uploadfile/fieldmappings?displayName=%s",
                getDeployedRestAPIHostPort(), sourceFile.getName()), mappings, Void.class);
    }

    private String createModel(SelectedConfig config, SourceFile sourceFile) {
        ModelingParameters parameters = new ModelingParameters();
        String configJson = JsonUtils.serialize(config);
        Map<String, String> runTimeParams = new HashMap<>();
        runTimeParams.put(AlgorithmFactory.MODEL_CONFIG, configJson);
        parameters.setRunTimeParams(runTimeParams);

        parameters.setName("SelfServiceModelingByModelQuality" + DateTime.now().getMillis());
        parameters.setDescription("SelfServiceModelingByModelQuality");
        parameters.setFilename(sourceFile.getName());
        String modelName = parameters.getName();
        model(parameters);
        return modelName;
    }

    @SuppressWarnings("rawtypes")
    private void model(ModelingParameters parameters) {
        ResponseDocument response;
        response = restTemplate.postForObject(
                String.format("%s/pls/models/%s", getDeployedRestAPIHostPort(), parameters.getName()), parameters,
                ResponseDocument.class);
        String modelingWorkflowApplicationId = new ObjectMapper().convertValue(response.getResult(), String.class);

        log.info(String.format("Workflow application id is %s", modelingWorkflowApplicationId));

        JobStatus completedStatus = waitForWorkflowStatus(modelingWorkflowApplicationId, false);
        log.info("Job Status=" + completedStatus.toString());
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
            List<Object> summaries = restTemplate.getForObject( //
                    String.format("%s/pls/modelsummaries", getDeployedRestAPIHostPort()), List.class);
            for (Object rawSummary : summaries) {
                ModelSummary summary = new ObjectMapper().convertValue(rawSummary, ModelSummary.class);
                if (summary.getName().contains(modelName)) {
                    found = summary;
                }
            }
            if (found != null)
                break;
            Thread.sleep(1000);
        }

        Object rawSummary = restTemplate.getForObject(
                String.format("%s/pls/modelsummaries/%s", getDeployedRestAPIHostPort(), found.getId()), Object.class);
        return JsonUtils.convertValue(rawSummary, ModelSummary.class);
    }

    private JobStatus waitForWorkflowStatus(String applicationId, boolean running) {

        int retryOnException = 10;
        Job job = null;
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
}
