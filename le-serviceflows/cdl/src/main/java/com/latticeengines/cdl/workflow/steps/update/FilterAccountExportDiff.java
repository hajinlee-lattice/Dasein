package com.latticeengines.cdl.workflow.steps.update;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.inject.Inject;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.annotation.Lazy;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import com.latticeengines.common.exposed.util.NamingUtils;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.metadata.Attribute;
import com.latticeengines.domain.exposed.metadata.DataCollection;
import com.latticeengines.domain.exposed.metadata.InterfaceName;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.metadata.TableRoleInCollection;
import com.latticeengines.domain.exposed.serviceflows.cdl.steps.process.ProcessAccountStepConfiguration;
import com.latticeengines.domain.exposed.spark.SparkJobResult;
import com.latticeengines.domain.exposed.spark.common.CopyConfig;
import com.latticeengines.proxy.exposed.cdl.DataCollectionProxy;
import com.latticeengines.proxy.exposed.metadata.MetadataProxy;
import com.latticeengines.serviceflows.workflow.dataflow.RunSparkJob;
import com.latticeengines.spark.exposed.job.common.CopyJob;


@Component(FilterAccountExportDiff.BEAN_NAME)
@Lazy
@Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
public class FilterAccountExportDiff extends RunSparkJob<ProcessAccountStepConfiguration, CopyConfig, CopyJob> {

    private static final Logger log = LoggerFactory.getLogger(FilterAccountExportDiff.class);

    static final String BEAN_NAME = "filterAccountExportDiff";

    @Inject
    private MetadataProxy metadataProxy;

    @Inject
    private DataCollectionProxy dataCollectionProxy;

    private Table activeFeaturesTable;

    @Override
    protected Class<CopyJob> getJobClz() {
        return CopyJob.class;
    }

    @Override
    protected CopyConfig configureJob(ProcessAccountStepConfiguration stepConfiguration) {
        CopyConfig config = new CopyConfig();

        String enrichedDiffTableName = getStringValueFromContext(ENRICHED_ACCOUNT_DIFF_TABLE_NAME);
        Table enrichedDiffTable = metadataProxy.getTable(customerSpace.toString(), enrichedDiffTableName);
        config.setInput(Collections.singletonList(enrichedDiffTable.toHdfsDataUnit("EnrichedTable")));

        config.setSelectAttrs(getRetrainAttrs());

        return config;
    }

    @Override
    protected void postJobExecution(SparkJobResult result) {
        String filteredTableName = NamingUtils.timestamp("AccountExportDiff");
        Table filteredTable = toTable(filteredTableName, InterfaceName.AccountId.name(), result.getTargets().get(0));

        Map<String, Attribute> attributeMap = new HashMap<>();
        activeFeaturesTable.getAttributes().forEach(attr -> attributeMap.put(attr.getName(), attr));
        overlayTableSchema(filteredTable, attributeMap);
        metadataProxy.createTable(customerSpace.toString(), filteredTableName, filteredTable);

        Map<TableRoleInCollection, String> processedTableNames = getMapObjectFromContext(PROCESSED_DIFF_TABLES, //
                TableRoleInCollection.class, String.class);
        processedTableNames.put(TableRoleInCollection.AccountExport, filteredTableName);
        putObjectInContext(PROCESSED_DIFF_TABLES, processedTableNames);
        addToListInContext(TEMPORARY_CDL_TABLES, filteredTableName, String.class);
    }

    private List<String> getRetrainAttrs() {
        DataCollection.Version active = getObjectFromContext(CDL_ACTIVE_VERSION, DataCollection.Version.class);
        activeFeaturesTable = dataCollectionProxy.getTable(customerSpace.toString(), //
                TableRoleInCollection.AccountExport, active);
        List<String> retainAttrNames = new ArrayList<>(Arrays.asList(activeFeaturesTable.getAttributeNames()));
        log.info(String.format("retainAttrNames from servingStore: %d", retainAttrNames.size()));
        return retainAttrNames;
    }

    @Override
    protected CustomerSpace parseCustomerSpace(ProcessAccountStepConfiguration stepConfiguration) {
        return stepConfiguration.getCustomerSpace();
    }

}
