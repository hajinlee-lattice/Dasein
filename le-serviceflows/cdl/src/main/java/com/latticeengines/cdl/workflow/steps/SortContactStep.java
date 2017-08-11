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
import com.latticeengines.domain.exposed.metadata.Attribute;
import com.latticeengines.domain.exposed.metadata.Category;
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

    private static final String SORTED_TABLE_PREFIX = TableRoleInCollection.SortedContact.name();
    private static final List<String> masterTableSortKeys = TableRoleInCollection.SortedContact
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
        Table sortedTable = metadataProxy.getTable(configuration.getCustomerSpace().toString(), sortedTableName);
        Map<BusinessEntity, Table> entityTableMap = getMapObjectFromContext(TABLE_GOING_TO_REDSHIFT,
                BusinessEntity.class, Table.class);
        if (entityTableMap == null) {
            entityTableMap = new HashMap<>();
        }
        enrichTableSchema(sortedTable);
        entityTableMap.put(BusinessEntity.Contact, sortedTable);
        putObjectInContext(TABLE_GOING_TO_REDSHIFT, entityTableMap);
        Map<BusinessEntity, Boolean> appendTableMap = getMapObjectFromContext(APPEND_TO_REDSHIFT_TABLE,
                BusinessEntity.class, Boolean.class);
        if (appendTableMap == null) {
            appendTableMap = new HashMap<>();
        }
        appendTableMap.put(BusinessEntity.Contact, false);
        putObjectInContext(APPEND_TO_REDSHIFT_TABLE, appendTableMap);
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
        String confStr = appendEngineConf(conf, lightEngineConfig());
        step.setConfiguration(confStr);
        return step;
    }

    private void enrichTableSchema(Table table) {
        List<Attribute> attrs = table.getAttributes();
        attrs.forEach(attr -> attr.setCategory(Category.CONTACT_ATTRIBUTES));
    }

}
