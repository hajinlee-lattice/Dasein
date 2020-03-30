package com.latticeengines.cdl.workflow.steps.legacydelete;

import static com.latticeengines.domain.exposed.metadata.TableRoleInCollection.ConsolidatedRawTransaction;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import javax.inject.Inject;

import org.apache.commons.collections4.MapUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.annotation.Lazy;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import com.latticeengines.common.exposed.util.AvroUtils;
import com.latticeengines.common.exposed.util.NamingUtils;
import com.latticeengines.common.exposed.util.PathUtils;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.cdl.CleanupOperationType;
import com.latticeengines.domain.exposed.metadata.Attribute;
import com.latticeengines.domain.exposed.metadata.DataCollection;
import com.latticeengines.domain.exposed.metadata.Extract;
import com.latticeengines.domain.exposed.metadata.InterfaceName;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.metadata.TableRoleInCollection;
import com.latticeengines.domain.exposed.metadata.datastore.DataUnit;
import com.latticeengines.domain.exposed.metadata.datastore.DynamoDataUnit;
import com.latticeengines.domain.exposed.metadata.datastore.HdfsDataUnit;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.domain.exposed.serviceflows.cdl.steps.legacydelete.LegacyDeleteSparkStepConfiguration;
import com.latticeengines.domain.exposed.spark.SparkJobResult;
import com.latticeengines.domain.exposed.spark.cdl.LegacyDeleteJobConfig;
import com.latticeengines.proxy.exposed.cdl.DataCollectionProxy;
import com.latticeengines.proxy.exposed.metadata.DataUnitProxy;
import com.latticeengines.serviceflows.workflow.dataflow.RunSparkJob;
import com.latticeengines.spark.exposed.job.AbstractSparkJob;
import com.latticeengines.spark.exposed.job.cdl.LegacyDeleteJob;

@Component(LegacyDeleteByUpload.BEAN_NAME)
@Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
@Lazy
public class LegacyDeleteByUpload extends RunSparkJob<LegacyDeleteSparkStepConfiguration, LegacyDeleteJobConfig> {

    private static Logger log = LoggerFactory.getLogger(LegacyDeleteByUpload.class);

    static final String BEAN_NAME = "legacyDeleteByUpload";

    @Inject
    private DataCollectionProxy dataCollectionProxy;

    @Inject
    private DataUnitProxy dataUnitProxy;

    private TableRoleInCollection batchStore;
    private Table masterTable;

    private DataCollection.Version active;
    private DataCollection.Version inactive;

    @Override
    protected Class<? extends AbstractSparkJob<LegacyDeleteJobConfig>> getJobClz() {
        return LegacyDeleteJob.class;
    }

    @Override
    protected LegacyDeleteJobConfig configureJob(LegacyDeleteSparkStepConfiguration stepConfiguration) {
        batchStore = stepConfiguration.getEntity().equals(BusinessEntity.Transaction)
                ? ConsolidatedRawTransaction : stepConfiguration.getEntity().getBatchStore();
        active = getObjectFromContext(CDL_ACTIVE_VERSION, DataCollection.Version.class);
        inactive = getObjectFromContext(CDL_INACTIVE_VERSION, DataCollection.Version.class);
        masterTable = dataCollectionProxy.getTable(stepConfiguration.getCustomer(), batchStore, active);
        Map<BusinessEntity, HdfsDataUnit> mergeDeleteTables = getMapObjectFromContext(LEGACY_DELETE_MERGE_TABLENAMES,
                BusinessEntity.class, HdfsDataUnit.class);
        if (mergeDeleteTables == null) {
            log.info("mergeDeleteTables is null.");
            return null;
        }
        HdfsDataUnit mergeDeleteTable = mergeDeleteTables.get(stepConfiguration.getEntity());
        if (masterTable == null || mergeDeleteTable == null) {
            log.info("masterTable is {}, mergeDeleteTable is {}.", masterTable, mergeDeleteTable);
            return null;
        }
        HdfsDataUnit input2 = masterTable.toHdfsDataUnit("Master");
        CleanupOperationType type = CleanupOperationType.BYUPLOAD_ID;
        LegacyDeleteJobConfig config = new LegacyDeleteJobConfig();
        config.setBusinessEntity(stepConfiguration.getEntity());
        config.setOperationType(type);
        config.setJoinedColumns(getJoinedColumns(stepConfiguration.getEntity(), type));
        config.setDeleteSourceIdx(0);
        config.setInput(Arrays.asList(mergeDeleteTable, input2));
        return config;
    }

    @Override
    protected void postJobExecution(SparkJobResult result) {
        String tenantId = CustomerSpace.shortenCustomerSpace(parseCustomerSpace(configuration).toString());
        String cleanupTableName = NamingUtils.timestamp("DeleteByFile_");
        Table cleanupTable = toTable(cleanupTableName, configuration.getEntity().getServingStore().getPrimaryKey(),
                result.getTargets().get(0));
        if (cleanupTable == null) {
            return;
        }
        if (batchStore.equals(TableRoleInCollection.ConsolidatedRawTransaction)) {
            enrichTableSchema(cleanupTable);
            return;
        }
        if (getTableDataLines(cleanupTable) <= 0) {
            if (noImport()) {
                log.error("cannot clean up all batchStore with no import.");
                throw new IllegalStateException("cannot clean up all batchStore with no import, PA failed");
            }
            log.info("Result table is empty, remove " + batchStore.name() + " from data collection!");
            dataCollectionProxy.resetTable(tenantId, batchStore);
            return;
        }
        metadataProxy.createTable(tenantId, cleanupTableName, cleanupTable);
        DynamoDataUnit dataUnit = null;
        if (batchStore.equals(BusinessEntity.Account.getBatchStore())) {
            // if replaced account batch store, need to link dynamo table
            String oldBatchStoreName = dataCollectionProxy.getTableName(customerSpace.toString(), batchStore,
                    active);
            dataUnit = (DynamoDataUnit) dataUnitProxy.getByNameAndType(customerSpace.toString(), oldBatchStoreName,
                    DataUnit.StorageType.Dynamo);
            if (dataUnit != null) {
                dataUnit.setLinkedTable(StringUtils.isBlank(dataUnit.getLinkedTable()) ? //
                        dataUnit.getName() : dataUnit.getLinkedTable());
                dataUnit.setName(cleanupTableName);
            }
        }
        enrichTableSchema(cleanupTable);
        dataCollectionProxy.upsertTable(customerSpace.toString(), cleanupTableName, batchStore, inactive);
        if (dataUnit != null) {
            dataUnitProxy.create(customerSpace.toString(), dataUnit);
        }
    }

    private LegacyDeleteJobConfig.JoinedColumns getJoinedColumns(BusinessEntity entity, CleanupOperationType type) {
        LegacyDeleteJobConfig.JoinedColumns joinedColumns = new LegacyDeleteJobConfig.JoinedColumns();
        String account_id = configuration.isEntityMatchGAEnabled()? InterfaceName.CustomerAccountId.name() :
                InterfaceName.AccountId.name();
        String contact_id = configuration.isEntityMatchGAEnabled()? InterfaceName.CustomerContactId.name() :
                InterfaceName.ContactId.name();
        switch (entity) {
            case Account:
                joinedColumns.setAccountId(account_id);
                break;
            case Contact:
                joinedColumns.setContactId(contact_id);
                break;
            case Transaction:
                switch (type) {
                    case BYUPLOAD_MINDATE:
                        joinedColumns.setTransactionTime(InterfaceName.TransactionDayPeriod.name());
                        break;
                    case BYUPLOAD_MINDATEANDACCOUNT:
                        joinedColumns.setAccountId(account_id);
                        joinedColumns.setTransactionTime(InterfaceName.TransactionDayPeriod.name());
                        break;
                    case BYUPLOAD_ACPD:
                        joinedColumns.setAccountId(account_id);
                        joinedColumns.setContactId(contact_id);
                        joinedColumns.setProductId(InterfaceName.ProductId.name());
                        joinedColumns.setTransactionTime(InterfaceName.TransactionDayPeriod.name());
                        break;
                    default:
                        break;
                }
                break;
            default:
                break;
        }
        return joinedColumns;
    }

    private void enrichTableSchema(Table table) {
        Map<String, Attribute> attrsToInherit = new HashMap<>();
        if (masterTable != null) {
            masterTable.getAttributes().forEach(attr -> attrsToInherit.putIfAbsent(attr.getName(), attr));
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
