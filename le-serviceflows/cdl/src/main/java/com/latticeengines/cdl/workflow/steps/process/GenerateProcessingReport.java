package com.latticeengines.cdl.workflow.steps.process;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
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
import com.latticeengines.cdl.workflow.steps.CloneTableService;
import com.latticeengines.common.exposed.util.AvroUtils;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.common.exposed.util.RetryUtils;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.cdl.OrphanRecordsType;
import com.latticeengines.domain.exposed.metadata.DataCollection;
import com.latticeengines.domain.exposed.metadata.DataCollectionStatus;
import com.latticeengines.domain.exposed.metadata.InterfaceName;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.metadata.TableRoleInCollection;
import com.latticeengines.domain.exposed.metadata.transaction.ProductType;
import com.latticeengines.domain.exposed.pls.Action;
import com.latticeengines.domain.exposed.pls.ActionType;
import com.latticeengines.domain.exposed.pls.CleanupActionConfiguration;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.domain.exposed.query.Restriction;
import com.latticeengines.domain.exposed.query.frontend.FrontEndQuery;
import com.latticeengines.domain.exposed.query.frontend.FrontEndRestriction;
import com.latticeengines.domain.exposed.serviceapps.cdl.ReportConstants;
import com.latticeengines.domain.exposed.serviceflows.cdl.steps.process.ProcessStepConfiguration;
import com.latticeengines.domain.exposed.util.PAReportUtils;
import com.latticeengines.domain.exposed.util.ProductUtils;
import com.latticeengines.domain.exposed.workflow.Report;
import com.latticeengines.domain.exposed.workflow.ReportPurpose;
import com.latticeengines.proxy.exposed.cdl.ActionProxy;
import com.latticeengines.proxy.exposed.cdl.DataCollectionProxy;
import com.latticeengines.proxy.exposed.metadata.MetadataProxy;
import com.latticeengines.proxy.exposed.objectapi.RatingProxy;
import com.latticeengines.workflow.exposed.build.BaseWorkflowStep;

@Component("generateProcessingReport")
@Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
public class GenerateProcessingReport extends BaseWorkflowStep<ProcessStepConfiguration> {

    protected static final Logger log = LoggerFactory.getLogger(GenerateProcessingReport.class);

    @Inject
    private DataCollectionProxy dataCollectionProxy;

    @Inject
    private MetadataProxy metadataProxy;

    @Inject
    private RatingProxy ratingProxy;

    @Inject
    private CloneTableService cloneTableService;

    @Inject
    private ActionProxy actionProxy;

    private DataCollection.Version active;
    private DataCollection.Version inactive;
    private CustomerSpace customerSpace;
    private List<Action> actions;

    @Override
    public void execute() {
        customerSpace = configuration.getCustomerSpace();
        active = getObjectFromContext(CDL_ACTIVE_VERSION, DataCollection.Version.class);
        inactive = getObjectFromContext(CDL_INACTIVE_VERSION, DataCollection.Version.class);
        swapMissingTableRoles();
        log.info("Evict attr repo cache for inactive version " + inactive);
        dataCollectionProxy.evictAttrRepoCache(customerSpace.toString(), inactive);
        registerReport();
    }

    private void swapMissingTableRoles() {
        cloneTableService.setCustomerSpace(customerSpace);
        cloneTableService.setActiveVersion(active);
        Set<BusinessEntity> resetEntities = getSetObjectFromContext(RESET_ENTITIES, BusinessEntity.class);
        if (resetEntities == null) {
            resetEntities = Collections.emptySet();
        }
        for (TableRoleInCollection role : TableRoleInCollection.values()) {
            BusinessEntity ownerEntity = getOwnerEntity(role);
            if (ownerEntity != null && resetEntities.contains(ownerEntity)) {
                // skip swap for reset entities
                log.info("Skip attempt to link " + role + " because its owner entity " //
                        + ownerEntity + " is being reset.");
                continue;
            }
            cloneTableService.linkInactiveTable(role);
        }
    }

    private BusinessEntity getOwnerEntity(TableRoleInCollection role) {
        BusinessEntity owner = Arrays.stream(BusinessEntity.values()).filter(entity -> //
        role.equals(entity.getBatchStore()) || role.equals(entity.getServingStore())) //
                .findFirst().orElse(null);
        if (owner == null) {
            switch (role) {
            case Profile:
                return BusinessEntity.Account;
            case ContactProfile:
                return BusinessEntity.Contact;
            case PurchaseHistoryProfile:
                return BusinessEntity.PurchaseHistory;
            case ConsolidatedRawTransaction:
                return BusinessEntity.Transaction;
            default:
                return null;
            }
        }
        return owner;
    }

    private void registerReport() {
        ObjectNode jsonReport = getObjectFromContext(ReportPurpose.PROCESS_ANALYZE_RECORDS_SUMMARY.getKey(),
                ObjectNode.class);
        updateSystemActionsReport(jsonReport);
        updateDecisionsReport(jsonReport);
        updateReportEntitiesSummaryReport(jsonReport);
        Report report = createReport(jsonReport.toString(), ReportPurpose.PROCESS_ANALYZE_RECORDS_SUMMARY,
                UUID.randomUUID().toString());
        registerReport(configuration.getCustomerSpace(), report);
        log.info("Registered report: " + jsonReport.toString());
    }

    private void updateDecisionsReport(ObjectNode report) {
        Map<String, String> decisions = getMapObjectFromContext(PROCESS_ANALYTICS_DECISIONS_KEY, String.class,
                String.class);
        if (MapUtils.isNotEmpty(decisions)) {
            ObjectNode decisionsNode = (ObjectNode) report
                    .get(ReportPurpose.PROCESS_ANALYZE_DECISIONS_SUMMARY.getKey());
            if (decisionsNode == null) {
                log.info("No decisions summary reports found. Create it.");
                decisionsNode = report.putObject(ReportPurpose.PROCESS_ANALYZE_DECISIONS_SUMMARY.getKey());
            }
            for (String key : decisions.keySet()) {
                decisionsNode.put(key, decisions.get(key));
            }
            StringBuilder builder = new StringBuilder();
            if (getConfiguration().isAutoSchedule()) {
                builder.append("isAutoSchedule=true;");
            }
            if (builder.length() > 0) {
                decisionsNode.put("Generic", builder.toString());
            }
        }
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
                    ? (ObjectNode) entitiesSummaryNode.get(entity.name()) : PAReportUtils.initEntityReport(entity);
            ObjectNode consolidateSummaryNode = (ObjectNode) entityNode
                    .get(ReportPurpose.CONSOLIDATE_RECORDS_SUMMARY.getKey());
            // Product report is generated in MergeProduct step
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

    private void updateCollectionStatus(Map<BusinessEntity, Long> currentCnts,
            Map<OrphanRecordsType, Long> orphanCnts) {
        DataCollectionStatus detail = getObjectFromContext(CDL_COLLECTION_STATUS, DataCollectionStatus.class);
        detail.setAccountCount(currentCnts.get(BusinessEntity.Account));
        detail.setContactCount(currentCnts.get(BusinessEntity.Contact));
        detail.setTransactionCount(currentCnts.get(BusinessEntity.Transaction));
        detail.setProductCount(currentCnts.get(BusinessEntity.Product));

        detail.setOrphanContactCount(orphanCnts.get(OrphanRecordsType.CONTACT));
        detail.setUnmatchedAccountCount(orphanCnts.get(OrphanRecordsType.UNMATCHED_ACCOUNT));
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
        return orphanCnts;
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
            Table table = metadataProxy.getTable(customerSpace.toString(), tableName);
            if (table == null) {
                log.error("Cannot find table " + tableName);
                return 0L;
            }

            Long result;
            String hdfsPath = table.getExtracts().get(0).getPath();
            if (tableRole == TableRoleInCollection.ConsolidatedProduct) {
                log.info("Count products in HDFS " + hdfsPath);
                result = ProductUtils.countProducts(yarnConfiguration, hdfsPath,
                        Arrays.asList(ProductType.Bundle.name(), ProductType.Hierarchy.name()));
            } else {
                if (!hdfsPath.endsWith("*.avro")) {
                    if (hdfsPath.endsWith("/")) {
                        hdfsPath += "*.avro";
                    } else {
                        hdfsPath += "/*.avro";
                    }
                }
                log.info("Count records in HDFS " + hdfsPath);
                result = AvroUtils.count(yarnConfiguration, hdfsPath);
            }
            log.info(String.format("Table role %s has %d entities.", tableRole.name(), result));
            return result;
        } catch (Exception ex) {
            log.error("Fail to count raw entities in table.", ex);
            return 0L;
        }
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
        case TRANSACTION:
        default:
            return 0;
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
        FrontEndQuery frontEndQuery = new FrontEndQuery();
        frontEndQuery.setMainEntity(entity);

        final int NUM_RETRIES = 3;
        RetryTemplate template = RetryUtils.getExponentialBackoffRetryTemplate(NUM_RETRIES, 5000L, 2.0, null);
        return template.execute(context -> {
            if (context.getRetryCount() > 1) {
                log.warn(
                        String.format(
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
