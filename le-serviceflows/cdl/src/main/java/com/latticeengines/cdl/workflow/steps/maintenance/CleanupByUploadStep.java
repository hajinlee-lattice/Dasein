package com.latticeengines.cdl.workflow.steps.maintenance;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.inject.Inject;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.cdl.CleanupByUploadConfiguration;
import com.latticeengines.domain.exposed.datacloud.transformation.PipelineTransformationRequest;
import com.latticeengines.domain.exposed.datacloud.transformation.configuration.impl.CleanupConfig;
import com.latticeengines.domain.exposed.datacloud.transformation.step.SourceTable;
import com.latticeengines.domain.exposed.datacloud.transformation.step.TargetTable;
import com.latticeengines.domain.exposed.datacloud.transformation.step.TransformationStepConfig;
import com.latticeengines.domain.exposed.metadata.DataCollection;
import com.latticeengines.domain.exposed.metadata.InterfaceName;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.domain.exposed.serviceflows.cdl.steps.maintenance.CleanupByUploadWrapperConfiguration;
import com.latticeengines.domain.exposed.serviceflows.datacloud.etl.TransformationWorkflowConfiguration;
import com.latticeengines.domain.exposed.util.TableUtils;
import com.latticeengines.proxy.exposed.metadata.DataCollectionProxy;
import com.latticeengines.proxy.exposed.metadata.MetadataProxy;
import com.latticeengines.serviceflows.workflow.etl.BaseTransformWrapperStep;

@Component("cleanupByUploadStep")
public class CleanupByUploadStep extends BaseTransformWrapperStep<CleanupByUploadWrapperConfiguration> {


    private static Logger log = LoggerFactory.getLogger(CleanupByUploadStep.class);
    private static int cleanupStep;

    private static final String CLEANUP_TABLE_PREFIX = "DeleteByFile";

    private static final String TRANSFORMER = "CleanupTransformer";

    @Inject
    protected MetadataProxy metadataProxy;

    @Inject
    private DataCollectionProxy dataCollectionProxy;
    private CleanupByUploadConfiguration cleanupByUploadConfiguration;

    @Override
    protected TransformationWorkflowConfiguration executePreTransformation() {
        intializeConfiguration();
        PipelineTransformationRequest request = generateRequest(configuration.getCustomerSpace());
        return transformationProxy.getWorkflowConf(request, configuration.getPodId());
    }

    @Override
    protected void onPostTransformationCompleted() {
        String cleanupTableName = TableUtils.getFullTableName(CLEANUP_TABLE_PREFIX, pipelineVersion);
        String customerSpace = configuration.getCustomerSpace().toString();
        Table cleanupTable = metadataProxy.getTable(customerSpace, cleanupTableName);
        log.info("result table Name is " + cleanupTable.getName());
        if (cleanupTable != null) {
            DataCollection.Version version = dataCollectionProxy.getActiveVersion(customerSpace);
            dataCollectionProxy.upsertTable(configuration.getCustomerSpace().toString(), cleanupTableName,
                    cleanupByUploadConfiguration.getEntity().getBatchStore(), version);
        }
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
        BusinessEntity entity = cleanupByUploadConfiguration.getEntity();
        Table masterTable = dataCollectionProxy.getTable(customerSpace.toString(), entity.getBatchStore());
        if (masterTable == null) {
            throw new RuntimeException(
                    String.format("master table in collection shouldn't be null when customer space %s, role %s",
                            customerSpace.toString(), entity.getBatchStore()));
        }
        String deleteName = cleanupByUploadConfiguration.getTableName();
        String masterName = masterTable.getName();
        SourceTable source = new SourceTable(masterName, customerSpace);
        SourceTable delete = new SourceTable(deleteName, customerSpace);
        List<String> sourceNames = new ArrayList<String>();
        Map<String, SourceTable> baseTables = new HashMap<String, SourceTable>();
        sourceNames.add(masterName);
        sourceNames.add(deleteName);
        baseTables.put(masterName, source);
        baseTables.put(deleteName, delete);

        log.info("master name is " + masterName + ", delete Name is " + deleteName);
        String joinColumn = "";
        if (entity == BusinessEntity.Account) {
            joinColumn = masterTable.getAttribute(InterfaceName.AccountId.name()).getName();
            log.info("current joinColumn is " + joinColumn);
        } else {
            joinColumn = masterTable.getAttribute(InterfaceName.ContactId.name()).getName();
        }

        CleanupConfig config = new CleanupConfig();
        config.setBusinessEntity(entity);
        config.setOperationType(cleanupByUploadConfiguration.getCleanupOperationType());
        config.setTransformer(TRANSFORMER);
        config.setJoinColumn(joinColumn);

        String configStr = appendEngineConf(config, lightEngineConfig());
        TargetTable targetTable = new TargetTable();
        targetTable.setCustomerSpace(customerSpace);
        targetTable.setNamePrefix(CLEANUP_TABLE_PREFIX);

        step.setBaseSources(sourceNames);
        step.setBaseTables(baseTables);
        step.setTransformer(TRANSFORMER);
        step.setConfiguration(configStr);
        step.setTargetTable(targetTable);

        return step;
    }

}
