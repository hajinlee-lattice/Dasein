package com.latticeengines.cdl.workflow.steps.validations;

import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;

import org.apache.avro.generic.GenericRecord;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.latticeengines.cdl.workflow.steps.BaseProcessAnalyzeSparkStep;
import com.latticeengines.common.exposed.util.AvroUtils;
import com.latticeengines.common.exposed.util.NamingUtils;
import com.latticeengines.common.exposed.util.PathUtils;
import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.domain.exposed.metadata.InterfaceName;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.metadata.TableRoleInCollection;
import com.latticeengines.domain.exposed.metadata.datastore.DataUnit;
import com.latticeengines.domain.exposed.metadata.datastore.HdfsDataUnit;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.domain.exposed.serviceapps.cdl.ReportConstants;
import com.latticeengines.domain.exposed.serviceflows.cdl.steps.process.BaseProcessEntityStepConfiguration;
import com.latticeengines.domain.exposed.serviceflows.core.steps.DynamoExportConfig;
import com.latticeengines.domain.exposed.spark.SparkJobResult;
import com.latticeengines.domain.exposed.spark.common.ChangeListConfig;
import com.latticeengines.domain.exposed.workflow.ReportPurpose;
import com.latticeengines.spark.exposed.job.cdl.ReportChangeListJob;
import com.latticeengines.spark.exposed.job.common.CreateChangeListJob;

public abstract class BaseValidateReportBatchStore<T extends BaseProcessEntityStepConfiguration>
        extends BaseProcessAnalyzeSparkStep<T> {

    private static final Logger log = LoggerFactory.getLogger(BaseValidateReportBatchStore.class);


    protected BusinessEntity entity;
    private TableRoleInCollection role;
    private String changeListTablePrefix;
    private String reportChangeListTablePrefix;
    private Table changeListTable;
    private Table reportChangeListTable;

    private long totalRecords;
    private Table activeTable;

    @Override
    public void execute() {
        bootstrap();
        entity = configuration.getMainEntity();
        changeListTablePrefix = entity.name() + "_ChangeList";
        reportChangeListTablePrefix = entity.name() + "_ReportChangeList";
        role = BusinessEntity.Transaction.equals(entity) ? //
                TableRoleInCollection.ConsolidatedRawTransaction : entity.getBatchStore();
        validate();
        syncBatchStoreOrResetServingStore();
        if (checkCreateChangeList()) {
            createChangeList();
            createChangeListReport();
        }
        updateReportPayload();
    }

    private boolean checkCreateChangeList() {
        boolean createChangeList = isChanged(role);
        activeTable = dataCollectionProxy.getTable(customerSpace.toString(), role, active);
        createChangeList = createChangeList && activeTable != null
                && (entity.equals(BusinessEntity.Account) || entity.equals(BusinessEntity.Contact));
        log.info("Create Change List ? " + createChangeList + " for entity=" + entity.name() + " activeTable="
                + activeTable);
        return createChangeList;
    }

    private void updateReportPayload() {
        try {
            ObjectNode report = getObjectFromContext(ReportPurpose.PROCESS_ANALYZE_RECORDS_SUMMARY.getKey(),
                    ObjectNode.class);
            JsonNode entitiesSummaryNode = report.get(ReportPurpose.ENTITIES_SUMMARY.getKey());
            if (entitiesSummaryNode == null) {
                entitiesSummaryNode = report.putObject(ReportPurpose.ENTITIES_SUMMARY.getKey());
            }
            JsonNode entityNode = entitiesSummaryNode.get(entity.name());
            if (entityNode == null) {
                entityNode = ((ObjectNode) entitiesSummaryNode).putObject(entity.name());
            }
            JsonNode consolidateSummaryNode = entityNode.get(ReportPurpose.CONSOLIDATE_RECORDS_SUMMARY.getKey());
            if (consolidateSummaryNode == null) {
                consolidateSummaryNode = ((ObjectNode) entityNode)
                        .putObject(ReportPurpose.CONSOLIDATE_RECORDS_SUMMARY.getKey());
            }
            if (reportChangeListTable != null) {
                String path = PathUtils.toAvroGlob(reportChangeListTable.getExtracts().get(0).getPath());
                List<GenericRecord> records = AvroUtils.getDataFromGlob(yarnConfiguration, path);
                GenericRecord record = records.get(0);
                Long chgNewRecords = (Long) record.get("NewRecords");
                Long chgUpdatedRecords = (Long) record.get("UpdatedRecords");
                Long chgDeletedRecords = (Long) record.get("DeletedRecords");
                log.info(String.format("From change lists, new records=%d, updated records=%d, deleted records=%d",
                        chgNewRecords, chgUpdatedRecords, chgDeletedRecords));

                setUpdateCount(chgUpdatedRecords, consolidateSummaryNode);
                setNewCount(chgNewRecords, consolidateSummaryNode);
                setDeletedCount(chgDeletedRecords, consolidateSummaryNode);
            }
            setTotalCount(totalRecords, consolidateSummaryNode);
            putObjectInContext(ReportPurpose.PROCESS_ANALYZE_RECORDS_SUMMARY.getKey(), report);
            log.info("Report=" + report.toString());

        } catch (Exception e) {
            throw new RuntimeException("Fail to update report payload", e);
        }
    }

    private void setTotalCount(Long totalRecords, JsonNode consolidateSummaryNode) {
        long totalCnt = 0;
        if (totalRecords != null) {
            totalCnt = totalRecords;
            ((ObjectNode) consolidateSummaryNode).put(ReportConstants.TOTAL, totalRecords);
            if (activeTable == null) {
                setNewCount(totalRecords, consolidateSummaryNode);
            } else {
                JsonNode newCnt = consolidateSummaryNode.get(ReportConstants.NEW);
                if (newCnt == null) {
                    setNewCount(0L, consolidateSummaryNode);
                }
            }
        }
        updateEntityValueMapInContext(entity, TOTAL_RECORDS, totalCnt, Long.class);
        log.info(String.format("Save total count %d for entity %s to workflow context", totalCnt, entity));
    }

    private void setDeletedCount(Long chgDeletedRecords, JsonNode consolidateSummaryNode) {
        long delCnt = 0;
        if (chgDeletedRecords != null) {
            delCnt = chgDeletedRecords;
        }
        ((ObjectNode) consolidateSummaryNode).put(ReportConstants.DELETE, delCnt);
        updateEntityValueMapInContext(entity, DELETED_RECORDS, delCnt, Long.class);
        log.info(String.format("Save deleted count %d for entity %s to workflow context", delCnt, entity));
    }

    private void setNewCount(Long chgNewRecords, JsonNode consolidateSummaryNode) {
        long newCnt = 0;
        if (chgNewRecords != null) {
            newCnt = chgNewRecords;
        }
        ((ObjectNode) consolidateSummaryNode).put(ReportConstants.NEW, newCnt);
        updateEntityValueMapInContext(entity, NEW_RECORDS, newCnt, Long.class);
        log.info(String.format("Save new count %d for entity %s to workflow context", newCnt, entity));
    }

    private void setUpdateCount(Long chgUpdatedRecords, JsonNode consolidateSummaryNode) {
        long updateCnt = 0;
        if (chgUpdatedRecords != null) {
            updateCnt = chgUpdatedRecords;
        }
        ((ObjectNode) consolidateSummaryNode).put(ReportConstants.UPDATE, updateCnt);
        updateEntityValueMapInContext(entity, UPDATED_RECORDS, updateCnt, Long.class);
        log.info(String.format("Save updated count %d for entity %s to workflow context", updateCnt, entity));
    }

    private void createChangeListReport() {
        if (changeListTable == null) {
            return;
        }
        DataUnit changeListDataUnit = toDataUnit(changeListTable, "ChangeListTable");
        long count = changeListDataUnit.getCount();
        if (count == 0) {
            log.warn("There's no record in change list table.");
            reportChangeListTable = null;
            return;
        }

        ChangeListConfig config = getChangeListConfig(null);
        List<DataUnit> inputs = new LinkedList<>();
        inputs.add(changeListDataUnit);
        config.setInput(inputs);
        SparkJobResult result = runSparkJob(ReportChangeListJob.class, config);

        String reportChangeListTableName = NamingUtils.timestamp(reportChangeListTablePrefix);
        HdfsDataUnit reportDataUnit = result.getTargets().get(0);
        reportChangeListTable = toTable(reportChangeListTableName, reportDataUnit);
        exportToS3AndAddToContext(reportChangeListTable, ACCOUNT_REPORT_CHANGELIST_TABLE_NAME);
        updateEntityValueMapInContext(ENTITY_REPORT_CHANGELIST_TABLES, reportChangeListTableName, String.class);
        addToListInContext(TEMPORARY_CDL_TABLES, reportChangeListTableName, String.class);

    }

    private void createChangeList() {
        if (activeTable == null) {
            log.info("There's no active batch store!");
            return;
        }
        Table inactiveTable = dataCollectionProxy.getTable(customerSpace.toString(), role, inactive);
        if (inactiveTable == null) {
            log.info("There's no inactive batch store!");
            return;
        }
        HdfsDataUnit activeDataUnit = activeTable.toHdfsDataUnit("ActiveBatchStore");
        HdfsDataUnit inactiveDataUnit = inactiveTable.toHdfsDataUnit("InactiveBatchStore");

        // in migration mode, need to use AccountId because legacy batch store
        // won't have EntityId column
        String joinKey = (configuration.isEntityMatchEnabled() && !inMigrationMode()) ? InterfaceName.EntityId.name()
                : getEntityKey();

        ChangeListConfig config = getChangeListConfig(joinKey);
        List<DataUnit> inputs = new LinkedList<>();
        inputs.add(inactiveDataUnit);
        inputs.add(activeDataUnit);
        config.setInput(inputs);
        SparkJobResult result = runSparkJob(CreateChangeListJob.class, config);

        String changeListTableName = NamingUtils.timestamp(changeListTablePrefix);
        HdfsDataUnit changeListDataUnit = result.getTargets().get(0);
        changeListTable = toTable(changeListTableName, changeListDataUnit);
        metadataProxy.createTable(customerSpace.toString(), changeListTableName, changeListTable);
        log.info("Create change list table=" + changeListTableName);
        exportToS3AndAddToContext(changeListTable, getEntityContextKey());
        if (BusinessEntity.Account.equals(entity)) {
            log.info("Add export {} to dynamo to config list", changeListTableName);
            exportAccountLookupChangeToDynamo(changeListTableName);
            addShortRetentionToTable(changeListTableName);
        }
    }

    protected String getEntityKey() {
        return null;
    }

    protected String getEntityContextKey() {
        return null;
    }

    private void exportAccountLookupChangeToDynamo(String changeListTableName) {
        Table changeListTable = metadataProxy.getTableSummary(customerSpace.toString(), changeListTableName);
        if (changeListTable == null) {
            // ignore error for now
            log.error("Failed to retrieve change list table with name: {}", changeListTableName);
            return;
        }
        DynamoExportConfig config = new DynamoExportConfig();
        config.setTableName(changeListTableName);
        config.setInputPath(PathUtils.toAvroGlob(changeListTable.getExtracts().get(0).getPath()));
        config.setPartitionKey(InterfaceName.AtlasLookupKey.name());
        addToListInContext(ATLAS_ACCOUNT_LOOKUP_TO_DYNAMO, config, DynamoExportConfig.class);
    }

    private ChangeListConfig getChangeListConfig(String joinKey) {
        ChangeListConfig config = new ChangeListConfig();
        config.setJoinKey(joinKey);
        config.setExclusionColumns(
                Arrays.asList(InterfaceName.CDLCreatedTime.name(), InterfaceName.CDLUpdatedTime.name(), joinKey));
        return config;
    }

    private void syncBatchStoreOrResetServingStore() {
        String activeName = dataCollectionProxy.getTableName(customerSpace.toString(), role, active);
        String inactiveName = dataCollectionProxy.getTableName(customerSpace.toString(), role, inactive);
        if (StringUtils.isNotBlank(activeName) && StringUtils.isBlank(inactiveName)) {
            linkInactiveTable(role);
        } else if (StringUtils.isBlank(activeName) && StringUtils.isBlank(inactiveName)) {
            log.info("No {} batch store in both versions, going to reset its serving store", entity);
            updateEntitySetInContext(RESET_ENTITIES, entity);
        }
    }

    private void validate() {
        if (tableInRoleExists(role, inactive)) {
            totalRecords = countRawEntitiesInHdfs(role, inactive);
            if (totalRecords <= 0L) {
                throw new LedpException(LedpCode.LEDP_18239, new String[] { entity.name() });
            }
        }
    }

}
