package com.latticeengines.cdl.workflow.steps;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.datacloud.manage.TransformationProgress;
import com.latticeengines.domain.exposed.datacloud.transformation.PipelineTransformationRequest;
import com.latticeengines.domain.exposed.datacloud.transformation.step.SourceTable;
import com.latticeengines.domain.exposed.datacloud.transformation.step.TargetTable;
import com.latticeengines.domain.exposed.datacloud.transformation.step.TransformationStepConfig;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.workflow.Job;
import com.latticeengines.domain.exposed.workflow.JobStatus;
import com.latticeengines.proxy.exposed.datacloudapi.TransformationProxy;
import com.latticeengines.proxy.exposed.metadata.MetadataProxy;
import com.latticeengines.proxy.exposed.workflowapi.WorkflowProxy;
import com.latticeengines.serviceflows.workflow.core.BaseWorkflowStep;

@Component("calculateStatsStep")
public class CalculateStatsStep extends BaseWorkflowStep<CalculateStatsStepConfiguration> {

    private static final Log log = LogFactory.getLog(CalculateStatsStep.class);

    @Autowired
    private MetadataProxy metadataProxy;

    @Autowired
    private WorkflowProxy workflowProxy;

    @Autowired
    private TransformationProxy transformationProxy;

    @Override
    public void execute() {
        log.info("Inside CalculateStats execute()");
        String masterTableName = configuration.getMasterTableName();
        log.info(String.format("masterTableName for customer %s is %s", configuration.getCustomerSpace().toString(),
                masterTableName));
        Table table = metadataProxy.getTable(configuration.getCustomerSpace().toString(), masterTableName);
        // TODO hook up with transform pipeline to calculate statsCube
        String targetTableName = "SomeRandomNameForNow";
        putStringValueInContext(CALCULATE_STATS_TARGET_TABLE, targetTableName);

        PipelineTransformationRequest request = generateRequest(configuration.getCustomerSpace(), masterTableName,
                targetTableName);

        TransformationProgress progress = transformationProxy.transform(request, "");
        String version = progress.getVersion();
        log.info(String.format("version for customer %s is %s", configuration.getCustomerSpace().toString(), version));
        putStringValueInContext(CALCULATE_STATS_TRANSFORM_VERSION, version);
        String applicationId = progress.getYarnAppId();
        waitForTransformation(applicationId);
    }

    private PipelineTransformationRequest generateRequest(CustomerSpace customerSpace, String masterTableName,
            String targetTableName) {
        try {
            PipelineTransformationRequest request = new PipelineTransformationRequest();
            request.setName("CalculateStatsStep");
            request.setSubmitter(customerSpace.getTenantId());
            request.setKeepTemp(false);
            request.setEnableSlack(false);
            // -----------
            TransformationStepConfig step1 = new TransformationStepConfig();
            List<String> baseSources = Collections.singletonList("MasterTable");
            step1.setBaseSources(baseSources);

            SourceTable sourceTable = new SourceTable(masterTableName, customerSpace);
            Map<String, SourceTable> baseTables = new HashMap<>();
            baseTables.put("MasterTable", sourceTable);
            step1.setBaseTables(baseTables);

            step1.setTransformer("sourceCopier");
            TargetTable targetTable = new TargetTable();
            targetTable.setCustomerSpace(customerSpace);
            targetTable.setNamePrefix(targetTableName);
            step1.setTargetTable(targetTable);
            step1.setConfiguration("{}");

            // -----------
            List<TransformationStepConfig> steps = Arrays.asList(step1);
            // -----------
            request.setSteps(steps);
            return request;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private void waitForTransformation(String applicationId) {
        Job job = null;
        while (true) {
            try {
                job = workflowProxy.getWorkflowJobFromApplicationId(applicationId);
            } catch (Exception e) {
                System.out.println(String.format("Workflow job exception: %s", e.getMessage()));
                job = null;
            }

            if (job != null && !job.isRunning()) {
                break;
            }
            try {
                log.info(String.format("Waiting for the job of applicationId %s to be finished.", applicationId));
                Thread.sleep(3000L);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }
        if (job.getJobStatus() != JobStatus.COMPLETED) {
            log.warn(String.format("The actual job status is %s", job.getJobStatus()));
        }
    }

}