package com.latticeengines.cdl.workflow.steps.legacydelete;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import javax.inject.Inject;

import org.apache.commons.collections4.MapUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.annotation.Lazy;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import com.latticeengines.common.exposed.util.AvroUtils;
import com.latticeengines.common.exposed.util.PathUtils;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.cdl.CleanupOperationType;
import com.latticeengines.domain.exposed.metadata.Attribute;
import com.latticeengines.domain.exposed.metadata.DataCollection;
import com.latticeengines.domain.exposed.metadata.Extract;
import com.latticeengines.domain.exposed.metadata.InterfaceName;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.metadata.TableRoleInCollection;
import com.latticeengines.domain.exposed.metadata.datastore.HdfsDataUnit;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.domain.exposed.serviceflows.cdl.steps.legacydelete.LegacyDeleteSparkStepConfiguration;
import com.latticeengines.domain.exposed.spark.SparkJobResult;
import com.latticeengines.domain.exposed.spark.cdl.LegacyDeleteJobConfig;
import com.latticeengines.domain.exposed.util.TableUtils;
import com.latticeengines.proxy.exposed.cdl.DataCollectionProxy;
import com.latticeengines.proxy.exposed.metadata.DataUnitProxy;
import com.latticeengines.serviceflows.workflow.dataflow.RunSparkJob;
import com.latticeengines.spark.exposed.job.AbstractSparkJob;
import com.latticeengines.spark.exposed.job.cdl.LegacyDeleteJob;

@Component(LegacyDeleteSystemBatchByUpload.BEAN_NAME)
@Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
@Lazy
public class LegacyDeleteSystemBatchByUpload extends RunSparkJob<LegacyDeleteSparkStepConfiguration, LegacyDeleteJobConfig> {

    private static Logger log = LoggerFactory.getLogger(LegacyDeleteSystemBatchByUpload.class);

    static final String BEAN_NAME = "legacyDeleteSystemBatchByUpload";

    @Inject
    private DataCollectionProxy dataCollectionProxy;

    @Inject
    private DataUnitProxy dataUnitProxy;

    private TableRoleInCollection systemBatchStore;
    private Table systemMasterTable;
    private String systemBatchStoreTablePrefix;

    private DataCollection.Version active;
    private DataCollection.Version inactive;
    String pipelineVersion;

    @Override
    protected Class<? extends AbstractSparkJob<LegacyDeleteJobConfig>> getJobClz() {
        return LegacyDeleteJob.class;
    }

    @Override
    protected LegacyDeleteJobConfig configureJob(LegacyDeleteSparkStepConfiguration stepConfiguration) {
        customerSpace = CustomerSpace.parse(configuration.getCustomer());
        active = getObjectFromContext(CDL_ACTIVE_VERSION, DataCollection.Version.class);
        inactive = getObjectFromContext(CDL_INACTIVE_VERSION, DataCollection.Version.class);
        switch (configuration.getEntity()) {
            case Account:
                systemBatchStore = TableRoleInCollection.SystemAccount;
                break;
            case Contact:
                systemBatchStore = TableRoleInCollection.SystemContact;
                break;
            default:
                throw new UnsupportedOperationException("No system batch store for " + configuration.getEntity().name());
        }
        systemMasterTable = dataCollectionProxy.getTable(customerSpace.toString(), systemBatchStore, active);
        systemBatchStoreTablePrefix = systemBatchStore.name();

        Map<BusinessEntity, HdfsDataUnit> mergeDeleteTables = getMapObjectFromContext(LEGACY_DELETE_MERGE_TABLENAMES,
                BusinessEntity.class, HdfsDataUnit.class);
        if (mergeDeleteTables == null) {
            log.info("mergeDeleteTables is null.");
            return null;
        }
        HdfsDataUnit mergeDeleteTable = mergeDeleteTables.get(stepConfiguration.getEntity());
        if (systemMasterTable == null || mergeDeleteTable == null) {
            log.info("systemMasterTable is {}, mergeDeleteTable is {}.", systemMasterTable, mergeDeleteTable);
            return null;
        }
        HdfsDataUnit input2 = systemMasterTable.toHdfsDataUnit("Master");
        CleanupOperationType type = CleanupOperationType.BYUPLOAD_ID;
        LegacyDeleteJobConfig config = new LegacyDeleteJobConfig();
        config.setBusinessEntity(stepConfiguration.getEntity());
        config.setOperationType(type);
        config.setJoinedColumns(getJoinedColumns(stepConfiguration.getEntity()));
        config.setDeleteSourceIdx(0);
        config.setInput(Arrays.asList(mergeDeleteTable, input2));
        return config;
    }

    @Override
    protected void postJobExecution(SparkJobResult result) {
        pipelineVersion = getStringValueFromContext(TRANSFORM_PIPELINE_VERSION);
        String tenantId = CustomerSpace.shortenCustomerSpace(parseCustomerSpace(configuration).toString());
        String cleanupTableName = TableUtils.getFullTableName(systemBatchStoreTablePrefix, pipelineVersion);
        Table cleanupTable = toTable(cleanupTableName, configuration.getEntity().getServingStore().getPrimaryKey(),
                result.getTargets().get(0));
        if (cleanupTable == null) {
            return;
        }
        if (getTableDataLines(cleanupTable) <= 0) {
            if (noImport()) {
                log.error("cannot clean up all systemBatchStore with no import.");
                throw new IllegalStateException("cannot clean up all systemBatchStore with no import, PA failed");
            }
            log.info("Result table is empty, remove " + systemBatchStore.name() + " from data collection!");
            dataCollectionProxy.unlinkTables(tenantId, systemBatchStore, inactive);
            return;
        }
        metadataProxy.createTable(tenantId, cleanupTableName, cleanupTable);
        enrichTableSchema(cleanupTable);
        dataCollectionProxy.upsertTable(customerSpace.toString(), cleanupTableName, systemBatchStore, inactive);
    }

    private LegacyDeleteJobConfig.JoinedColumns getJoinedColumns(BusinessEntity entity) {
        LegacyDeleteJobConfig.JoinedColumns joinedColumns = new LegacyDeleteJobConfig.JoinedColumns();
        switch (entity) {
            case Account:
                joinedColumns.setAccountId(InterfaceName.EntityId.name());
                break;
            case Contact:
                joinedColumns.setContactId(InterfaceName.EntityId.name());
                break;
            default:
                break;
        }
        return joinedColumns;
    }

    private void enrichTableSchema(Table table) {
        Map<String, Attribute> attrsToInherit = new HashMap<>();
        if (systemMasterTable != null) {
            systemMasterTable.getAttributes().forEach(attr -> attrsToInherit.putIfAbsent(attr.getName(), attr));
        }
        List<Attribute> newAttrs = table.getAttributes()
                .stream()
                .map(attr -> attrsToInherit.getOrDefault(attr.getName(), attr))
                .collect(Collectors.toList());
        table.setAttributes(newAttrs);
        metadataProxy.updateTable(customerSpace.toString(), table.getName(), table);
    }

    private Long getTableDataLines(Table table) {
        if (table == null || table.getExtracts() == null) {
            return 0L;
        }
        Long lines = 0L;
        List<String> paths = new ArrayList<>();
        for (Extract extract : table.getExtracts()) {
            paths.add(PathUtils.toAvroGlob(extract.getPath()));
        }
        for (String path : paths) {
            lines += AvroUtils.count(yarnConfiguration, path);
        }
        return lines;
    }

    private boolean noImport() {
        Map<BusinessEntity, List> entityImportsMap = getMapObjectFromContext(CONSOLIDATE_INPUT_IMPORTS,
                BusinessEntity.class, List.class);
        return MapUtils.isEmpty(entityImportsMap) || !entityImportsMap.containsKey(configuration.getEntity());
    }
}
