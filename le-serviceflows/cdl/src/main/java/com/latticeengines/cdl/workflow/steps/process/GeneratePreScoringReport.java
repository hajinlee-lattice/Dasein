package com.latticeengines.cdl.workflow.steps.process;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import javax.inject.Inject;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.collections4.MapUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.annotation.Scope;
import org.springframework.retry.support.RetryTemplate;
import org.springframework.stereotype.Component;

import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.collect.Lists;
import com.latticeengines.cdl.workflow.steps.CloneTableService;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.common.exposed.util.RetryUtils;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.cdl.OrphanRecordsType;
import com.latticeengines.domain.exposed.cdl.activity.AtlasStream;
import com.latticeengines.domain.exposed.datacloud.statistics.Bucket;
import com.latticeengines.domain.exposed.metadata.DataCollection;
import com.latticeengines.domain.exposed.metadata.DataCollectionStatus;
import com.latticeengines.domain.exposed.metadata.InterfaceName;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.metadata.TableRoleInCollection;
import com.latticeengines.domain.exposed.metadata.datastore.DataUnit;
import com.latticeengines.domain.exposed.metadata.datastore.HdfsDataUnit;
import com.latticeengines.domain.exposed.metadata.transaction.ProductStatus;
import com.latticeengines.domain.exposed.metadata.transaction.ProductType;
import com.latticeengines.domain.exposed.pls.Action;
import com.latticeengines.domain.exposed.pls.ActionType;
import com.latticeengines.domain.exposed.pls.CleanupActionConfiguration;
import com.latticeengines.domain.exposed.query.BucketRestriction;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.domain.exposed.query.Restriction;
import com.latticeengines.domain.exposed.query.frontend.FrontEndQuery;
import com.latticeengines.domain.exposed.query.frontend.FrontEndRestriction;
import com.latticeengines.domain.exposed.serviceapps.cdl.ReportConstants;
import com.latticeengines.domain.exposed.serviceflows.cdl.steps.process.ProcessStepConfiguration;
import com.latticeengines.domain.exposed.spark.SparkJobResult;
import com.latticeengines.domain.exposed.spark.cdl.CountOrphanTransactionsConfig;
import com.latticeengines.domain.exposed.util.PAReportUtils;
import com.latticeengines.domain.exposed.util.ProductUtils;
import com.latticeengines.domain.exposed.workflow.ReportPurpose;
import com.latticeengines.proxy.exposed.cdl.ActionProxy;
import com.latticeengines.proxy.exposed.cdl.DataCollectionProxy;
import com.latticeengines.proxy.exposed.metadata.MetadataProxy;
import com.latticeengines.proxy.exposed.objectapi.RatingProxy;
import com.latticeengines.serviceflows.workflow.dataflow.LivySessionManager;
import com.latticeengines.serviceflows.workflow.util.SparkUtils;
import com.latticeengines.spark.exposed.job.cdl.CountOrphanTransactionsJob;
import com.latticeengines.spark.exposed.service.LivySessionService;
import com.latticeengines.spark.exposed.service.SparkJobService;
import com.latticeengines.workflow.exposed.build.BaseWorkflowStep;

@Component("generatePreScoringReport")
@Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
public class GeneratePreScoringReport extends BaseWorkflowStep<ProcessStepConfiguration> {

    protected static final Logger log = LoggerFactory.getLogger(GeneratePreScoringReport.class);

    @Inject
    private CloneTableService cloneTableService;

    @Inject
    private DataCollectionProxy dataCollectionProxy;

    @Inject
    private MetadataProxy metadataProxy;

    @Inject
    private RatingProxy ratingProxy;

    @Inject
    private ActionProxy actionProxy;

    @Inject
    private LivySessionService sessionService;

    @Inject
    private SparkJobService sparkJobService;

    @Inject
    private LivySessionManager livySessionManager;

    private DataCollection.Version active;
    private DataCollection.Version inactive;
    private CustomerSpace customerSpace;
    private List<Action> actions;
    private Map<TableRoleInCollection, String> tableNames = new HashMap<>();

    @Override
    public void execute() {
        customerSpace = configuration.getCustomerSpace();
        active = getObjectFromContext(CDL_ACTIVE_VERSION, DataCollection.Version.class);
        inactive = getObjectFromContext(CDL_INACTIVE_VERSION, DataCollection.Version.class);
        cloneInactiveServingStoresAndBatchStore();
        log.info("Evict attr repo cache for inactive version " + inactive);
        dataCollectionProxy.evictAttrRepoCache(customerSpace.toString(), inactive);
        updateStreamCounts();
        registerReport();
        registerCollectionTables();
    }

    private void cloneInactiveServingStoresAndBatchStore() {
        cloneTableService.setActiveVersion(active);
        cloneTableService.setCustomerSpace(customerSpace);
        Set<BusinessEntity> resetEntities = getSetObjectFromContext(RESET_ENTITIES, BusinessEntity.class);
        Arrays.stream(BusinessEntity.values()).forEach(entity -> {
            if (resetEntities == null || !resetEntities.contains(entity)) {
                TableRoleInCollection servingStore = entity.getServingStore();
                if (servingStore != null) {
                    cloneTableService.linkInactiveTable(servingStore);
                }
                TableRoleInCollection batchStore = entity.getBatchStore();
                if (batchStore != null) {
                    cloneTableService.linkInactiveTable(batchStore);
                }
                TableRoleInCollection systemStore = entity.getSystemBatchStore();
                if (systemStore != null) {
                    cloneTableService.linkInactiveTable(systemStore);
                }
            }
        });
        TableRoleInCollection streamRole = TableRoleInCollection.ConsolidatedActivityStream;
        cloneTableService.linkToInactiveTableWithSignature(streamRole);
    }

    private void registerCollectionTables() {
        List<String> inactiveTables = dataCollectionProxy.getTableNames(customerSpace.toString(), inactive);
        inactiveTables.forEach(this::registerTable);
    }

    private void registerReport() {
        ObjectNode jsonReport = getObjectFromContext(ReportPurpose.PROCESS_ANALYZE_RECORDS_SUMMARY.getKey(),
                ObjectNode.class);
        updateSystemActionsReport(jsonReport);
        updateReportEntitiesSummaryReport(jsonReport);
        putObjectInContext(ReportPurpose.PROCESS_ANALYZE_RECORDS_SUMMARY.getKey(), jsonReport);
        log.info("Registered report: " + jsonReport.toString());
    }

    private void updateSystemActionsReport(ObjectNode report) {
        ArrayNode systemActionNode = report.get(ReportPurpose.SYSTEM_ACTIONS.getKey()) != null
                ? (ArrayNode) report.get(ReportPurpose.SYSTEM_ACTIONS.getKey())
                : report.putArray(ReportPurpose.SYSTEM_ACTIONS.getKey());
        List<Action> dataCloudChangeActions = getDataCloudChangeActions();
        dataCloudChangeActions.forEach(action -> systemActionNode.add(action.getType().getDisplayName()));
    }

    private void updateReportEntitiesSummaryReport(ObjectNode report) {
        Map<BusinessEntity, Long> currentCnts = retrieveCurrentEntityCnts();
        Map<BusinessEntity, Long> deleteCnts = getDeletedCount();
        Map<OrphanRecordsType, Long> orphanCnts = retrieveOrphanEntityCnts();

        ObjectNode entitiesSummaryNode = (ObjectNode) report.get(ReportPurpose.ENTITIES_SUMMARY.getKey());
        if (entitiesSummaryNode == null) {
            log.info("No entity summary reports found. Create it.");
            entitiesSummaryNode = report.putObject(ReportPurpose.ENTITIES_SUMMARY.getKey());
        }

        BusinessEntity[] entities = { BusinessEntity.Account, BusinessEntity.Contact, BusinessEntity.Product,
                BusinessEntity.Transaction, BusinessEntity.PurchaseHistory };
        for (BusinessEntity entity : entities) {
            ObjectNode entityNode = entitiesSummaryNode.get(entity.name()) != null
                    ? (ObjectNode) entitiesSummaryNode.get(entity.name())
                    : PAReportUtils.initEntityReport(entity);
            ObjectNode consolidateSummaryNode = (ObjectNode) entityNode
                    .get(ReportPurpose.CONSOLIDATE_RECORDS_SUMMARY.getKey());
            // Product report is generated in MergeProduct step, update PRODUCT_ID when product not imported
            // PurchaseHistory report is generated in ProfilePurchaseHistory
            // step
            if (entity != BusinessEntity.Product && entity != BusinessEntity.PurchaseHistory) {
                long newCnt = consolidateSummaryNode.get(ReportConstants.NEW).asLong();
                long deleteCnt = deleteCnts.get(entity) == null ? 0L : deleteCnts.get(entity);
                log.info(String.format(
                        "For entity %s, current total count: %d, new count: %s, delete count: %d, entity has delete action attached: %b",
                        entity.name(), currentCnts.get(entity), newCnt, deleteCnt, deleteCnts.containsKey(entity)));
                consolidateSummaryNode.put("DELETE", String.valueOf(deleteCnt));
                ObjectNode entityNumberNode = JsonUtils.createObjectNode();
                entityNumberNode.put(ReportConstants.TOTAL, String.valueOf(currentCnts.get(entity)));
                entityNode.set(ReportPurpose.ENTITY_STATS_SUMMARY.getKey(), entityNumberNode);
            } else if (entity == BusinessEntity.Product && !hasImport(BusinessEntity.Product)) {
                consolidateSummaryNode.put(ReportConstants.PRODUCT_ID, currentCnts.get(entity));
            }
            // Populate entity match summary
            ObjectNode entityMatchNode = JsonUtils.createObjectNode();
            // entity name -> pair<publish #seed, publish #lookup>
            @SuppressWarnings("rawtypes")
            Map<String, Map> entityPublishStats = getMapObjectFromContext(ENTITY_PUBLISH_STATS, String.class,
                    Map.class);
            if (MapUtils.isNotEmpty(entityPublishStats) && entityPublishStats.containsKey(entity.name())) {
                Map<String, Integer> cntMap = JsonUtils.convertMap(entityPublishStats.get(entity.name()), String.class,
                        Integer.class);
                cntMap.entrySet().forEach(ent -> {
                    entityMatchNode.put(ent.getKey(), String.valueOf(ent.getValue()));
                });
            }
            // TODO: @Jonathan Add something here for DP-9342
            if (entityMatchNode.fieldNames().hasNext()) {
                entityNode.set(ReportPurpose.ENTITY_MATCH_SUMMARY.getKey(), entityMatchNode);
            }

            entitiesSummaryNode.set(entity.name(), entityNode);
        }

        updateCollectionStatus(currentCnts, orphanCnts);
    }

    //not available for embedded entity
    private boolean hasImport(BusinessEntity entity) {
        Map<BusinessEntity, List> entityImportsMap = getMapObjectFromContext(CONSOLIDATE_INPUT_IMPORTS, BusinessEntity.class, List.class);
        return MapUtils.isNotEmpty(entityImportsMap) && entityImportsMap.containsKey(entity);
    }

    private void updateCollectionStatus(Map<BusinessEntity, Long> currentCnts,
                                        Map<OrphanRecordsType, Long> orphanCnts) {
        DataCollectionStatus detail = getObjectFromContext(CDL_COLLECTION_STATUS, DataCollectionStatus.class);
        detail.setAccountCount(currentCnts.get(BusinessEntity.Account));
        detail.setContactCount(currentCnts.get(BusinessEntity.Contact));
        detail.setTransactionCount(currentCnts.get(BusinessEntity.Transaction));
        detail.setProductCount(currentCnts.get(BusinessEntity.Product));

        detail.setOrphanContactCount(orphanCnts.get(OrphanRecordsType.CONTACT));
        detail.setUnmatchedAccountCount(orphanCnts.get(OrphanRecordsType.UNMATCHED_ACCOUNT));
        detail.setOrphanTransactionCount(orphanCnts.get(OrphanRecordsType.TRANSACTION));
        putObjectInContext(CDL_COLLECTION_STATUS, detail);
        log.info("GenerateProcessingReport step: dataCollection Status is " + JsonUtils.serialize(detail));
        dataCollectionProxy.saveOrUpdateDataCollectionStatus(customerSpace.toString(), detail, inactive);
    }

    private Map<BusinessEntity, Long> retrieveCurrentEntityCnts() {
        Map<BusinessEntity, Long> currentCnts = new HashMap<>();
        currentCnts.put(BusinessEntity.Account, countInRedshift(BusinessEntity.Account));
        currentCnts.put(BusinessEntity.Contact, countInRedshift(BusinessEntity.Contact));
        currentCnts.put(BusinessEntity.Transaction,
                countRawEntitiesInHdfs(TableRoleInCollection.ConsolidatedRawTransaction));
        currentCnts.put(BusinessEntity.Product, countRawEntitiesInHdfs(TableRoleInCollection.ConsolidatedProduct));
        return currentCnts;
    }

    private Map<OrphanRecordsType, Long> retrieveOrphanEntityCnts() {
        Map<OrphanRecordsType, Long> orphanCnts = new HashMap<>();
        try {
            orphanCnts.put(OrphanRecordsType.UNMATCHED_ACCOUNT,
                    countOrphansInRedshift(OrphanRecordsType.UNMATCHED_ACCOUNT));
        } catch (Exception e) {
            log.warn("Failed to get the number of unmatched accounts", e);
        }
        try {
            orphanCnts.put(OrphanRecordsType.CONTACT, countOrphansInRedshift(OrphanRecordsType.CONTACT));
        } catch (Exception e) {
            log.warn("Failed to get the number of orphan contacts", e);
        }
        try {
            orphanCnts.put(OrphanRecordsType.TRANSACTION, countOrphanTransactionsInHdfs());
        } catch (Exception e) {
            log.warn("Failed to get the number of orphan transactions", e);
        }
        return orphanCnts;
    }

    private long countOrphanTransactionsInHdfs() {
        long result = 0L;
        List<TableRoleInCollection> tableRoles = Lists.newArrayList(TableRoleInCollection.ConsolidatedRawTransaction,
                TableRoleInCollection.ConsolidatedAccount, TableRoleInCollection.ConsolidatedProduct);
        boolean valid = true;
        List<DataUnit> hdfsDataUnits = new ArrayList<>();
        for (TableRoleInCollection tableRole : tableRoles) {
            String tableName = tableNames.get(tableRole);
            if (StringUtils.isEmpty(tableName)) {
                tableName = dataCollectionProxy.getTableName(customerSpace.toString(), tableRole, inactive);
            }
            if (StringUtils.isEmpty(tableName)) {
                valid = false;
                break;
            }
            Table table = metadataProxy.getTable(customerSpace.toString(), tableName);
            if (table == null) {
                log.error("Cannot find table " + tableName);
                valid = false;
                break;
            }
            HdfsDataUnit hdfsDataUnit = new HdfsDataUnit();
            hdfsDataUnit.setPath(table.getExtracts().get(0).getPath());
            hdfsDataUnits.add(hdfsDataUnit);
        }
        if (!valid) {
            return result;
        }
        CountOrphanTransactionsConfig countOrphanTransactionsConfig = new CountOrphanTransactionsConfig();
        countOrphanTransactionsConfig.setInput(hdfsDataUnits);
        countOrphanTransactionsConfig.setJoinKeys(Lists.newArrayList(InterfaceName.AccountId.name(), InterfaceName.ProductId.name()));
        SparkJobResult sparkJobResult = SparkUtils.runJob(customerSpace, yarnConfiguration, sparkJobService,
                livySessionManager, CountOrphanTransactionsJob.class, countOrphanTransactionsConfig);
        result = Long.parseLong(sparkJobResult.getOutput());
        log.info(String.format("There are %d orphan transactions.", result));
        return result;
    }

    private long countOrphansInRedshift(OrphanRecordsType orphanRecordsType) {
        FrontEndQuery frontEndQuery;
        switch (orphanRecordsType) {
            case UNMATCHED_ACCOUNT:
                String tableName = dataCollectionProxy.getTableName(customerSpace.toString(), //
                        BusinessEntity.Account.getServingStore(), inactive);
                if (StringUtils.isNotBlank(tableName)) {
                    Restriction nullLatticeAccountId = Restriction.builder()
                            .let(BusinessEntity.Account, InterfaceName.LatticeAccountId.name()).isNull().build();
                    FrontEndRestriction accountRestriction = new FrontEndRestriction();
                    accountRestriction.setRestriction(nullLatticeAccountId);
                    frontEndQuery = new FrontEndQuery();
                    frontEndQuery.setMainEntity(BusinessEntity.Account);
                    frontEndQuery.setAccountRestriction(accountRestriction);
                    return ratingProxy.getCountFromObjectApi(customerSpace.toString(), frontEndQuery, inactive);
                } else {
                    log.info("There is no account serving store, return 0 as the number of unmatched accounts.");
                    return 0L;
                }
            case CONTACT:
                Table batchStoreTable = dataCollectionProxy.getTable(customerSpace.toString(), //
                        BusinessEntity.Contact.getBatchStore(), inactive);
                if (batchStoreTable != null) {
                    long allContacts = batchStoreTable.getExtracts().get(0).getProcessedRecords();
                    log.debug("There are " + allContacts + " contacts in total.");
                    String servingStoreTable = dataCollectionProxy.getTableName(customerSpace.toString(), //
                            BusinessEntity.Contact.getServingStore(), inactive);
                    if (StringUtils.isNotBlank(servingStoreTable)) {
                        frontEndQuery = new FrontEndQuery();
                        frontEndQuery.setMainEntity(BusinessEntity.Contact);
                        frontEndQuery.setAccountRestriction(new FrontEndRestriction(accountNotNullBucket()));
                        long nonOrphanContacts = ratingProxy.getCountFromObjectApi(customerSpace.toString(), //
                                frontEndQuery, inactive);
                        log.debug("There are " + nonOrphanContacts + " non-orphan contacts in redshift.");
                        long orphanContacts = allContacts - nonOrphanContacts;
                        log.debug("There are " + orphanContacts + " orphan contacts.");
                        return orphanContacts;
                    } else {
                        log.info("There is no contact serving store, all contacts are orphan.");
                        return allContacts;
                    }
                } else {
                    log.info("There is no contact batch store, return 0 as the number of orphan contacts.");
                    return 0L;
                }
            default:
                return 0;
        }
    }

    private long countRawEntitiesInHdfs(TableRoleInCollection tableRole) {
        try {
            String tableName = dataCollectionProxy.getTableName(customerSpace.toString(), tableRole, inactive);
            if (StringUtils.isBlank(tableName)) {
                log.info(String.format("Cannot find table role %s in version %s", tableRole.name(), inactive));
                tableName = dataCollectionProxy.getTableName(customerSpace.toString(), tableRole, active);
                if (StringUtils.isBlank(tableName)) {
                    log.info(String.format("Cannot find table role %s in version %s", tableRole.name(), active));
                    return 0L;
                }
            }
            tableNames.putIfAbsent(tableRole, tableName);
            Table table = metadataProxy.getTable(customerSpace.toString(), tableName);
            if (table == null) {
                log.error("Cannot find table " + tableName);
                return 0L;
            }

            Long result;
            String hdfsPath = table.getExtracts().get(0).getPath();
            if (tableRole.equals(TableRoleInCollection.ConsolidatedProduct)) {
                log.info("Count products in HDFS " + hdfsPath);
                result = ProductUtils.countProducts(yarnConfiguration, hdfsPath,
                        Arrays.asList(ProductType.Bundle.name(), ProductType.Hierarchy.name()), ProductStatus.Active.name());
            } else {
                if (!hdfsPath.endsWith("*.avro")) {
                    if (hdfsPath.endsWith("/")) {
                        hdfsPath += "*.avro";
                    } else {
                        hdfsPath += "/*.avro";
                    }
                }
                log.info("Count records in HDFS " + hdfsPath);
                result = SparkUtils.countRecordsInGlobs(sessionService, sparkJobService, yarnConfiguration, hdfsPath);
            }
            log.info(String.format("Table role %s has %d entities.", tableRole.name(), result));
            return result;
        } catch (Exception ex) {
            log.error("Fail to count raw entities in table.", ex);
            return 0L;
        }
    }

    private long countInRedshift(BusinessEntity entity) {
        String servingStore;
        try {
            servingStore = dataCollectionProxy.getTableName(customerSpace.toString(), entity.getServingStore(),
                    inactive);
        } catch (Exception ex) {
            throw new RuntimeException("Fail to look for serving store for entity " + entity.name(), ex);
        }
        if (StringUtils.isBlank(servingStore)) {
            log.info(String.format("Cannot find serving store for entity %s with version %s", entity.name(),
                    inactive.name()));
            return 0L;
        }
        tableNames.putIfAbsent(entity.getServingStore(), servingStore);
        FrontEndQuery frontEndQuery = new FrontEndQuery();
        frontEndQuery.setMainEntity(entity);

        final int NUM_RETRIES = 3;
        RetryTemplate template = RetryUtils.getExponentialBackoffRetryTemplate(NUM_RETRIES, 5000L, 2.0, null);
        return template.execute(context -> {
            if (context.getRetryCount() > 1) {
                log.warn(String.format(
                        "Retries=%d of %d. Exception in getting count from serving store for entity %s with version %s",
                        context.getRetryCount(), NUM_RETRIES, entity.name(), inactive.name()),
                        context.getLastThrowable());
            }
            try {
                return ratingProxy.getCountFromObjectApi(customerSpace.toString(), frontEndQuery, inactive);
            } catch (Exception exc) {
                throw new RuntimeException(
                        String.format("Fail to get count from serving store for entity %s with version %s.",
                                entity.name(), inactive.name()),
                        exc);
            }
        });
    }

    private void updateStreamCounts() {
        DataCollectionStatus status = getObjectFromContext(CDL_COLLECTION_STATUS, DataCollectionStatus.class);
        Map<String, AtlasStream> streamMap = status.getActivityStreamMap();
        if (MapUtils.isNotEmpty(streamMap)) {
            TableRoleInCollection batchStore = TableRoleInCollection.ConsolidatedActivityStream;
            Map<String, String> tableNames = dataCollectionProxy.getTableNamesWithSignatures(customerSpace.toString(), batchStore, inactive, null);
            log.info("Raw stream tables: " + JsonUtils.serialize(tableNames));
            for (String streamId : streamMap.keySet()) {
                streamMap.get(streamId).setTenant(null);
                if (tableNames.containsKey(streamId)) {
                    String tableName = tableNames.get(streamId);
                    Table summary = metadataProxy.getTableSummary(customerSpace.toString(), tableName);
                    try {
                        Long count = summary.getExtracts().get(0).getProcessedRecords();
                        streamMap.get(streamId).setCount(count);
                        log.info("Update the count of stream {} to {}", streamId, count);
                    } catch (Exception e) {
                        log.warn("Failed to update count for stream {}", streamId, e);
                    }
                } else {
                    log.warn("Not table for stream {}", streamId);
                }
            }
        }
        status.setActivityStreamMap(streamMap);
        putObjectInContext(CDL_COLLECTION_STATUS, status);
    }

    private Map<BusinessEntity, Long> getDeletedCount() {
        List<Action> deleteActions = getDeleteActions();
        if (CollectionUtils.isEmpty(deleteActions)) {
            log.info("No delete action attached to current PA job");
            return Collections.emptyMap();
        }
        Map<BusinessEntity, Long> deleteCounts = new HashMap<>();
        for (Action deleteAction : deleteActions) {
            CleanupActionConfiguration config = (CleanupActionConfiguration) deleteAction.getActionConfiguration();
            if (config != null) {
                config.getDeletedRecords().forEach((entity, deleteCount) -> {
                    if (deleteCounts.containsKey(entity)) {
                        deleteCounts.put(entity, deleteCounts.get(entity) + deleteCount);
                    } else {
                        deleteCounts.put(entity, deleteCount);
                    }
                });
            }
        }
        return deleteCounts;
    }

    private BucketRestriction accountNotNullBucket() {
        Bucket bkt = Bucket.notNullBkt();
        return new BucketRestriction(BusinessEntity.Account, InterfaceName.AccountId.name(), bkt);
    }

    private List<Action> getDeleteActions() {
        if (actions == null) {
            actions = getActions();
        }
        return actions.stream().filter(action -> ActionType.CDL_OPERATION_WORKFLOW.equals(action.getType()))
                .collect(Collectors.toList());
    }

    private List<Action> getDataCloudChangeActions() {
        List<Long> systemActionIds = getListObjectFromContext(SYSTEM_ACTION_IDS, Long.class);
        List<Action> actions = actionProxy.getActionsByPids(customerSpace.toString(), systemActionIds);
        return actions.stream().filter(action -> ActionType.getDataCloudRelatedTypes().contains(action.getType()))
                .filter(action -> action.getOwnerId().equals(configuration.getOwnerId())).collect(Collectors.toList());
    }

    private List<Action> getActions() {
        if (CollectionUtils.isEmpty(configuration.getActionIds())) {
            return Collections.emptyList();
        }
        List<Action> actions = actionProxy.getActionsByPids(customerSpace.toString(), configuration.getActionIds());
        if (actions == null) {
            actions = Collections.emptyList();
        }
        return actions;
    }

}
