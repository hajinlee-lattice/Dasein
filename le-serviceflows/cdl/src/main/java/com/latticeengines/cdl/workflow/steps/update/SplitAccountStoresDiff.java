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
import com.latticeengines.domain.exposed.metadata.datastore.DataUnit;
import com.latticeengines.domain.exposed.metadata.datastore.HdfsDataUnit;
import com.latticeengines.domain.exposed.serviceflows.cdl.steps.process.ProcessAccountStepConfiguration;
import com.latticeengines.domain.exposed.spark.SparkJobResult;
import com.latticeengines.domain.exposed.spark.common.CopyConfig;
import com.latticeengines.domain.exposed.spark.common.MultiCopyConfig;
import com.latticeengines.proxy.exposed.cdl.DataCollectionProxy;
import com.latticeengines.proxy.exposed.metadata.MetadataProxy;
import com.latticeengines.serviceflows.workflow.dataflow.RunSparkJob;
import com.latticeengines.spark.exposed.job.common.MultiCopyJob;

@Component(SplitAccountStoresDiff.BEAN_NAME)
@Lazy
@Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
public class SplitAccountStoresDiff extends RunSparkJob<ProcessAccountStepConfiguration, MultiCopyConfig> {

    private static final Logger log = LoggerFactory.getLogger(SplitAccountStoresDiff.class);

    static final String BEAN_NAME = "splitAccountStoresDiff";

    @Inject
    private MetadataProxy metadataProxy;

    @Inject
    private DataCollectionProxy dataCollectionProxy;

    private Table activeExportTable;
    private Table activeFeaturesTable;

    @Override
    protected Class<MultiCopyJob> getJobClz() {
        return MultiCopyJob.class;
    }

    @Override
    protected CustomerSpace parseCustomerSpace(ProcessAccountStepConfiguration stepConfiguration) {
        return stepConfiguration.getCustomerSpace();
    }

    @Override
    protected MultiCopyConfig configureJob(ProcessAccountStepConfiguration stepConfiguration) {
        CopyConfig copyAccountExport = configureExportCopy();
        CopyConfig copyAccountFeatures = configureFeaturesCopy();

        MultiCopyConfig config = new MultiCopyConfig();
        String enrichedDiffTableName = getStringValueFromContext(ENRICHED_ACCOUNT_DIFF_TABLE_NAME);
        Table enrichedDiffTable = metadataProxy.getTable(customerSpace.toString(), enrichedDiffTableName);
        config.setInput(Collections.singletonList(enrichedDiffTable.toHdfsDataUnit("EnrichedTable")));

        if (copyAccountExport == null) {
            config.setCopyConfigs(Collections.singletonList(copyAccountFeatures));
            config.setSpecialTarget(0, DataUnit.DataFormat.PARQUET);
        } else {
            config.setCopyConfigs(Arrays.asList(copyAccountExport, copyAccountFeatures));
            config.setSpecialTarget(0, DataUnit.DataFormat.PARQUET);
            config.setSpecialTarget(1, DataUnit.DataFormat.PARQUET);
        }

        return config;
    }

    private CopyConfig configureExportCopy() {
        DataCollection.Version active = getObjectFromContext(CDL_ACTIVE_VERSION, DataCollection.Version.class);
        activeExportTable = dataCollectionProxy.getTable(customerSpace.toString(), //
                TableRoleInCollection.AccountExport, active);
        if (activeExportTable == null) {
            log.warn("Cannot find active AccountExport table, skip filtering the diff table.");
            return null;
        }

        List<String> retainAttrNames = new ArrayList<>(Arrays.asList(activeExportTable.getAttributeNames()));
        log.info(String.format("retainAttrNames from servingStore: %d", retainAttrNames.size()));
        CopyConfig config = new CopyConfig();
        config.setSelectAttrs(retainAttrNames);

        return config;
    }

    private CopyConfig configureFeaturesCopy() {
        DataCollection.Version active = getObjectFromContext(CDL_ACTIVE_VERSION, DataCollection.Version.class);
        activeFeaturesTable = dataCollectionProxy.getTable(customerSpace.toString(), //
                TableRoleInCollection.AccountFeatures, active);
        List<String> retainAttrNames = new ArrayList<>(Arrays.asList(activeFeaturesTable.getAttributeNames()));
        log.info(String.format("retainAttrNames from servingStore: %d", retainAttrNames.size()));

        CopyConfig config = new CopyConfig();
        config.setSelectAttrs(retainAttrNames);
        return config;
    }

    @Override
    protected void postJobExecution(SparkJobResult result) {
        processAccountExportResult(result.getTargets().get(0), activeExportTable);
        processAccountFeaturesResult(result.getTargets().get(1), activeFeaturesTable);
    }

    private void processAccountExportResult(HdfsDataUnit result, Table activeExportTable) {
        processFilteredResult(result, activeExportTable, TableRoleInCollection.AccountExport);
    }

    private void processAccountFeaturesResult(HdfsDataUnit result, Table activeFeaturesTable) {
        processFilteredResult(result, activeFeaturesTable, TableRoleInCollection.AccountFeatures);
    }

    private void processFilteredResult(HdfsDataUnit result, Table activeTable, TableRoleInCollection role) {
        String filteredTableName = NamingUtils.timestamp(role.name() + "Diff");
        Table filteredTable = toTable(filteredTableName, InterfaceName.AccountId.name(), result);

        Map<String, Attribute> attributeMap = new HashMap<>();
        activeTable.getAttributes().forEach(attr -> attributeMap.put(attr.getName(), attr));
        overlayTableSchema(filteredTable, attributeMap);
        metadataProxy.createTable(customerSpace.toString(), filteredTableName, filteredTable);

        Map<TableRoleInCollection, String> processedTableNames = getMapObjectFromContext(PROCESSED_DIFF_TABLES, //
                TableRoleInCollection.class, String.class);
        processedTableNames.put(role, filteredTableName);
        putObjectInContext(PROCESSED_DIFF_TABLES, processedTableNames);
        addToListInContext(TEMPORARY_CDL_TABLES, filteredTableName, String.class);
    }

}
