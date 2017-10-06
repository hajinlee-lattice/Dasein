package com.latticeengines.cdl.workflow.steps;

import static com.latticeengines.domain.exposed.datacloud.DataCloudConstants.TRANSFORMER_SORTER;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.datacloud.transformation.PipelineTransformationRequest;
import com.latticeengines.domain.exposed.datacloud.transformation.configuration.impl.SorterConfig;
import com.latticeengines.domain.exposed.datacloud.transformation.step.SourceTable;
import com.latticeengines.domain.exposed.datacloud.transformation.step.TargetTable;
import com.latticeengines.domain.exposed.datacloud.transformation.step.TransformationStepConfig;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.metadata.TableRoleInCollection;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.domain.exposed.serviceflows.cdl.steps.SortProductStepConfiguration;
import com.latticeengines.domain.exposed.serviceflows.datacloud.etl.TransformationWorkflowConfiguration;
import com.latticeengines.domain.exposed.util.TableUtils;

@Component("sortProductStep")
public class SortProductStep extends ProfileStepBase<SortProductStepConfiguration> {

    private static final Logger log = LoggerFactory.getLogger(SortProductStep.class);

    private static final String SORTED_TABLE_PREFIX = TableRoleInCollection.SortedProduct.name();

    @Override
    protected BusinessEntity getEntity() {
        return BusinessEntity.Product;
    }

    @Override
    protected TransformationWorkflowConfiguration executePreTransformation() {
        String customerSpace = configuration.getCustomerSpace().toString();
        Table activeMasterTable = dataCollectionProxy.getTable(customerSpace, TableRoleInCollection.ConsolidatedProduct);
        if (activeMasterTable == null) {
            throw new IllegalStateException("Cannot find the master table in default collection");
        }
        log.info(String.format("masterTableName for customer %s is %s", configuration.getCustomerSpace().toString(),
                activeMasterTable.getName()));
        PipelineTransformationRequest request = generateRequest(configuration.getCustomerSpace(), activeMasterTable);
        return transformationProxy.getWorkflowConf(request, configuration.getPodId());
    }

    @Override
    protected void onPostTransformationCompleted() {
        String sortedTableName = TableUtils.getFullTableName(SORTED_TABLE_PREFIX, pipelineVersion);
        updateEntityValueMapInContext(TABLE_GOING_TO_REDSHIFT, sortedTableName, String.class);
        updateEntityValueMapInContext(APPEND_TO_REDSHIFT_TABLE, false, Boolean.class);
    }

    private PipelineTransformationRequest generateRequest(CustomerSpace customerSpace, Table masterTable) {
        String masterTableName = masterTable.getName();
        try {
            PipelineTransformationRequest request = new PipelineTransformationRequest();
            request.setName("ConsolidateProductStep");
            request.setSubmitter(customerSpace.getTenantId());
            request.setKeepTemp(false);
            request.setEnableSlack(false);

            TransformationStepConfig sort = sort(customerSpace, masterTableName);
            // -----------
            List<TransformationStepConfig> steps = Collections.singletonList(sort);
            // -----------
            request.setSteps(steps);
            return request;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private TransformationStepConfig sort(CustomerSpace customerSpace, String masterTableName) {
        TransformationStepConfig step = new TransformationStepConfig();
        String tableSourceName = "CustomerUniverse";
        SourceTable sourceTable = new SourceTable(masterTableName, customerSpace);
        List<String> baseSources = Collections.singletonList(tableSourceName);
        step.setBaseSources(baseSources);
        Map<String, SourceTable> baseTables = new HashMap<>();
        baseTables.put(tableSourceName, sourceTable);
        step.setBaseTables(baseTables);
        step.setTransformer(TRANSFORMER_SORTER);

        SorterConfig config = new SorterConfig();
        config.setPartitions(20);
        String sortingKey = TableRoleInCollection.SortedProduct.getForeignKeysAsStringList().get(0);
        config.setSortingField(sortingKey);
        config.setCompressResult(true);
        step.setConfiguration(appendEngineConf(config, lightEngineConfig()));

        TargetTable targetTable = new TargetTable();
        targetTable.setCustomerSpace(customerSpace);
        targetTable.setNamePrefix(SORTED_TABLE_PREFIX);
        step.setTargetTable(targetTable);

        return step;
    }

}
