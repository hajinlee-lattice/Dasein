package com.latticeengines.cdl.workflow.steps;

import static com.latticeengines.domain.exposed.datacloud.DataCloudConstants.TRANSFORMER_SORTER;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
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
import com.latticeengines.domain.exposed.serviceflows.cdl.steps.SortContactStepConfiguration;
import com.latticeengines.domain.exposed.serviceflows.datacloud.etl.TransformationWorkflowConfiguration;
import com.latticeengines.domain.exposed.util.TableUtils;
import com.latticeengines.proxy.exposed.datacloudapi.TransformationProxy;
import com.latticeengines.proxy.exposed.metadata.DataCollectionProxy;
import com.latticeengines.proxy.exposed.metadata.MetadataProxy;
import com.latticeengines.serviceflows.workflow.etl.BaseTransformWrapperStep;

@Component("sortContactStep")
public class SortContactStep extends BaseTransformWrapperStep<SortContactStepConfiguration> {

    private static final Logger log = LoggerFactory.getLogger(SortContactStep.class);

    private static final String SORTED_TABLE_PREFIX = TableRoleInCollection.BucketedContact.name();
    private static final List<String> masterTableSortKeys = TableRoleInCollection.BucketedContact
            .getForeignKeysAsStringList();

    @Autowired
    private TransformationProxy transformationProxy;

    @Autowired
    private DataCollectionProxy dataCollectionProxy;

    @Autowired
    private MetadataProxy metadataProxy;

    @Override
    protected TransformationWorkflowConfiguration executePreTransformation() {
        String customerSpace = configuration.getCustomerSpace().toString();
        Table masterTable = dataCollectionProxy.getTable(customerSpace, TableRoleInCollection.ConsolidatedContact);
        if (masterTable == null) {
            throw new IllegalStateException("Cannot find the master table in default collection");
        }
        log.info(String.format("masterTableName for customer %s is %s", configuration.getCustomerSpace().toString(),
                masterTable.getName()));
        PipelineTransformationRequest request = generateRequest(configuration.getCustomerSpace(), masterTable);
        return transformationProxy.getWorkflowConf(request, configuration.getPodId());
    }

    @Override
    protected void onPostTransformationCompleted() {
        String sortedTableName = TableUtils.getFullTableName(SORTED_TABLE_PREFIX, pipelineVersion);
        upsertTable(configuration.getCustomerSpace().toString(), sortedTableName);
        Table sortedTable = metadataProxy.getTable(configuration.getCustomerSpace().toString(), sortedTableName);
        Map<BusinessEntity, Table> entityTableMap = new HashMap<>();
        entityTableMap.put(BusinessEntity.Contact, sortedTable);
        putObjectInContext(TABLE_GOING_TO_REDSHIFT, entityTableMap);
    }

    private PipelineTransformationRequest generateRequest(CustomerSpace customerSpace, Table masterTable) {
        String masterTableName = masterTable.getName();
        try {
            PipelineTransformationRequest request = new PipelineTransformationRequest();
            request.setName("ConsolidateContactStep");
            request.setSubmitter(customerSpace.getTenantId());
            request.setKeepTemp(false);
            request.setEnableSlack(false);
            // -----------
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

    private TransformationStepConfig sort(CustomerSpace customerSpace, String sourceTableName) {
        TransformationStepConfig step = new TransformationStepConfig();
        String tableSourceName = "CustomerUniverse";
        SourceTable sourceTable = new SourceTable(sourceTableName, customerSpace);
        List<String> baseSources = Collections.singletonList(tableSourceName);
        step.setBaseSources(baseSources);
        Map<String, SourceTable> baseTables = new HashMap<>();
        baseTables.put(tableSourceName, sourceTable);
        step.setBaseTables(baseTables);

        step.setTransformer(TRANSFORMER_SORTER);

        TargetTable targetTable = new TargetTable();
        targetTable.setCustomerSpace(customerSpace);
        targetTable.setNamePrefix(SORTED_TABLE_PREFIX);
        targetTable.setExpandBucketedAttrs(true);
        step.setTargetTable(targetTable);

        SorterConfig conf = new SorterConfig();
        conf.setPartitions(500);
        conf.setSplittingThreads(maxSplitThreads);
        conf.setCompressResult(true);
        conf.setSortingField(masterTableSortKeys.get(0)); // TODO: only support
                                                          // single sort key now
        String confStr = appendEngineConf(conf, heavyEngineConfig());
        step.setConfiguration(confStr);
        return step;
    }

    private void upsertTable(String customerSpace, String sortedTableName) {

        Table bktTable = metadataProxy.getTable(customerSpace, sortedTableName);
        if (bktTable == null) {
            throw new RuntimeException("Failed to find bucketed table in customer " + customerSpace);
        }
        dataCollectionProxy.upsertTable(customerSpace, sortedTableName, TableRoleInCollection.BucketedContact);
        bktTable = dataCollectionProxy.getTable(customerSpace, TableRoleInCollection.BucketedContact);
        if (bktTable == null) {
            throw new IllegalStateException("Cannot find the upserted bucketed table in data collection.");
        }
    }

}
