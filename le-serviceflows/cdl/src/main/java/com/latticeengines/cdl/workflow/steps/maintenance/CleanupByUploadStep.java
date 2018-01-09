package com.latticeengines.cdl.workflow.steps.maintenance;

import java.util.Arrays;
import java.util.List;

import javax.inject.Inject;

import org.springframework.stereotype.Component;

import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.cdl.CleanupByUploadConfiguration;
import com.latticeengines.domain.exposed.datacloud.transformation.PipelineTransformationRequest;
import com.latticeengines.domain.exposed.datacloud.transformation.configuration.impl.CleanupConfig;
import com.latticeengines.domain.exposed.datacloud.transformation.step.TargetTable;
import com.latticeengines.domain.exposed.datacloud.transformation.step.TransformationStepConfig;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.serviceflows.cdl.steps.maintenance.CleanupByUploadWrapperConfiguration;
import com.latticeengines.domain.exposed.serviceflows.datacloud.etl.TransformationWorkflowConfiguration;
import com.latticeengines.domain.exposed.util.TableUtils;
import com.latticeengines.proxy.exposed.metadata.MetadataProxy;
import com.latticeengines.serviceflows.workflow.etl.BaseTransformWrapperStep;

@Component("cleanupByUploadStep")
public class CleanupByUploadStep extends BaseTransformWrapperStep<CleanupByUploadWrapperConfiguration> {


    private static int cleanupStep;

    private static final String CLEANUP_TABLE_PREFIX = "DeleteByFile";

    @Inject
    protected MetadataProxy metadataProxy;

    private CleanupByUploadConfiguration cleanupByUploadConfiguration;

    @Override
    protected TransformationWorkflowConfiguration executePreTransformation() {
        intializeConfiguration();
        PipelineTransformationRequest request = generateRequest(configuration.getCustomerSpace());
        return transformationProxy.getWorkflowConf(request, configuration.getPodId());
    }

    @Override
    protected void onPostTransformationCompleted() {
        //update table??
        String cleanupTableName = TableUtils.getFullTableName(CLEANUP_TABLE_PREFIX, pipelineVersion);
        Table cleanupTable = metadataProxy.getTable(configuration.getCustomerSpace().toString(), cleanupTableName);
    }

    private void intializeConfiguration() {
        if (configuration.getMaintenanceOperationConfiguration() == null) {
            throw new RuntimeException("Cleanup by upload configuration is NULL!");
        }
        if (configuration.getMaintenanceOperationConfiguration() instanceof CleanupByUploadConfiguration) {
            cleanupByUploadConfiguration = (CleanupByUploadConfiguration) configuration
                    .getMaintenanceOperationConfiguration();
        } else {
            throw new RuntimeException("Cleanup by upload configuration is not expected!");
        }
    }

    private PipelineTransformationRequest generateRequest(CustomerSpace customerSpace) {
        try {
            PipelineTransformationRequest request = new PipelineTransformationRequest();
            request.setName("CleanupByUploadStep");
            request.setSubmitter(customerSpace.getTenantId());
            request.setKeepTemp(false);
            request.setEnableSlack(false);
            cleanupStep = 0;

            TransformationStepConfig cleanup = cleanup(customerSpace);

            List<TransformationStepConfig> steps = Arrays.asList(cleanup);

            request.setSteps(steps);
            return request;

        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private TransformationStepConfig cleanup(CustomerSpace customerSpace) {
        TransformationStepConfig step = new TransformationStepConfig();
        // master table & delete file table?
        step.setBaseSources(null);
        step.setBaseTables(null);

        step.setTransformer("CleanupTransformer");

        CleanupConfig config = new CleanupConfig();
        String configStr = appendEngineConf(config, lightEngineConfig());
        step.setConfiguration(configStr);
        TargetTable targetTable = new TargetTable();
        targetTable.setCustomerSpace(customerSpace);
        targetTable.setNamePrefix(CLEANUP_TABLE_PREFIX);
        step.setTargetTable(targetTable);

        return step;
    }
}
