package com.latticeengines.cdl.workflow.steps;

import static com.latticeengines.domain.exposed.datacloud.DataCloudConstants.ACCOUNT_MASTER;
import static com.latticeengines.domain.exposed.datacloud.DataCloudConstants.TRANSFORMER_AM_ENRICHER;
import static com.latticeengines.domain.exposed.datacloud.DataCloudConstants.TRANSFORMER_BUCKETER;
import static com.latticeengines.domain.exposed.datacloud.DataCloudConstants.TRANSFORMER_PROFILER;
import static com.latticeengines.domain.exposed.datacloud.DataCloudConstants.TRANSFORMER_STATS_CALCULATOR;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.datacloud.manage.TransformationProgress;
import com.latticeengines.domain.exposed.datacloud.transformation.PipelineTransformationRequest;
import com.latticeengines.domain.exposed.datacloud.transformation.configuration.impl.AMAttrEnrichConfig;
import com.latticeengines.domain.exposed.datacloud.transformation.configuration.impl.ProfileConfig;
import com.latticeengines.domain.exposed.datacloud.transformation.step.SourceTable;
import com.latticeengines.domain.exposed.datacloud.transformation.step.TargetTable;
import com.latticeengines.domain.exposed.datacloud.transformation.step.TransformationStepConfig;
import com.latticeengines.domain.exposed.serviceflows.cdl.steps.CalculateStatsStepConfiguration;
import com.latticeengines.domain.exposed.workflow.Job;
import com.latticeengines.domain.exposed.workflow.JobStatus;
import com.latticeengines.proxy.exposed.datacloudapi.TransformationProxy;
import com.latticeengines.proxy.exposed.workflowapi.WorkflowProxy;
import com.latticeengines.serviceflows.workflow.core.BaseWorkflowStep;

@Component("calculateStatsStep")
public class CalculateStatsStep extends BaseWorkflowStep<CalculateStatsStepConfiguration> {

    private static final Log log = LogFactory.getLog(CalculateStatsStep.class);

    private static final String TARGET_TABLE_NAME_PATTERN = "%s_%s";

    private static final int JOIN_STEP = 0;
    private static final int PROFILE_STEP = 1;
    private static final int BUCKET_STEP = 2;

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

        String profileTableNamePrefix = "Profile";
        String statsTableNamePrefix = "Stats";
        PipelineTransformationRequest request = generateRequest(configuration.getCustomerSpace(), masterTableName,
                profileTableNamePrefix, statsTableNamePrefix);
        TransformationProgress progress = transformationProxy.transform(request, "");
        String version = progress.getVersion();
        log.info(String.format("version for customer %s is %s", configuration.getCustomerSpace().toString(), version));
        putStringValueInContext(CALCULATE_STATS_TRANSFORM_VERSION, version);
        putStringValueInContext(CALCULATE_STATS_PROFILE_TABLE, String.format(TARGET_TABLE_NAME_PATTERN, profileTableNamePrefix, version));
        putStringValueInContext(CALCULATE_STATS_TARGET_TABLE, String.format(TARGET_TABLE_NAME_PATTERN, statsTableNamePrefix, version));

        String applicationId = progress.getYarnAppId();
        waitForTransformation(applicationId);
    }

    private PipelineTransformationRequest generateRequest(CustomerSpace customerSpace, String masterTableName, String profileTablePrefix,
            String statsTablePrefix) {
        try {
            PipelineTransformationRequest request = new PipelineTransformationRequest();
            request.setName("CalculateStatsStep");
            request.setSubmitter(customerSpace.getTenantId());
            request.setKeepTemp(false);
            request.setEnableSlack(false);
            // -----------
            TransformationStepConfig enrich = enrichStepConfig(customerSpace, masterTableName);
            TransformationStepConfig profile = profileStepConfig(customerSpace, profileTablePrefix);
            TransformationStepConfig bucket = bucketStepConfig();
            TransformationStepConfig calc = calcStepConfig(customerSpace, statsTablePrefix);
            // -----------
            List<TransformationStepConfig> steps = Arrays.asList(enrich, profile, bucket, calc);
            // -----------
            request.setSteps(steps);
            return request;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private TransformationStepConfig enrichStepConfig(CustomerSpace customerSpace, String sourceTableName) {
        TransformationStepConfig step = new TransformationStepConfig();
        String tableSourceName = "Table_" + sourceTableName;
        SourceTable sourceTable = new SourceTable(sourceTableName, customerSpace);
        List<String> baseSources = new ArrayList<>();
        baseSources.add(tableSourceName);
        baseSources.add(ACCOUNT_MASTER);
        step.setBaseSources(baseSources);
        Map<String, SourceTable> baseTables = new HashMap<>();
        baseTables.put(tableSourceName, sourceTable);
        step.setBaseTables(baseTables);
        step.setTransformer(TRANSFORMER_AM_ENRICHER);
        AMAttrEnrichConfig conf = new AMAttrEnrichConfig();
        //TODO: change to false, after we have a local copy of AM
        conf.setNotJoinAM(true);
        step.setConfiguration(JsonUtils.serialize(conf));
        return step;
    }

    private TransformationStepConfig profileStepConfig(CustomerSpace customerSpace, String profileTablePrefix) {
        TransformationStepConfig step = new TransformationStepConfig();
        List<Integer> inputSteps = new ArrayList<>();
        inputSteps.addAll(Collections.singletonList(JOIN_STEP));
        step.setInputSteps(inputSteps);
        step.setTransformer(TRANSFORMER_PROFILER);

        TargetTable targetTable = new TargetTable();
        targetTable.setCustomerSpace(customerSpace);
        targetTable.setNamePrefix(profileTablePrefix);
        step.setTargetTable(targetTable);

        ProfileConfig conf = new ProfileConfig();
        String confParamStr1 = JsonUtils.serialize(conf);
        step.setConfiguration(confParamStr1);
        return step;
    }

    private TransformationStepConfig bucketStepConfig() {
        TransformationStepConfig step = new TransformationStepConfig();
        List<Integer> inputSteps = new ArrayList<>();
        inputSteps.addAll(Arrays.asList(JOIN_STEP, PROFILE_STEP));
        step.setInputSteps(inputSteps);
        step.setTransformer(TRANSFORMER_BUCKETER);
        step.setConfiguration("{}");
        return step;
    }

    private TransformationStepConfig calcStepConfig(CustomerSpace customerSpace, String statsTablePrefix) {
        TransformationStepConfig step = new TransformationStepConfig();

        List<Integer> inputSteps = new ArrayList<>();
        inputSteps.addAll(Arrays.asList(BUCKET_STEP, PROFILE_STEP));
        step.setInputSteps(inputSteps);
        step.setTransformer(TRANSFORMER_STATS_CALCULATOR);
        step.setConfiguration("{}");

        TargetTable targetTable = new TargetTable();
        targetTable.setCustomerSpace(customerSpace);
        targetTable.setNamePrefix(statsTablePrefix);
        step.setTargetTable(targetTable);

        return step;
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