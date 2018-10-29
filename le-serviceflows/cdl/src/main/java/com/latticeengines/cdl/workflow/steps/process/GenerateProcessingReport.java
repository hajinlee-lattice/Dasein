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
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.latticeengines.cdl.workflow.steps.CloneTableService;
import com.latticeengines.common.exposed.util.AvroUtils;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.metadata.DataCollection;
import com.latticeengines.domain.exposed.metadata.DataCollectionStatus;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.metadata.TableRoleInCollection;
import com.latticeengines.domain.exposed.pls.Action;
import com.latticeengines.domain.exposed.pls.ActionType;
import com.latticeengines.domain.exposed.pls.CleanupActionConfiguration;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.domain.exposed.query.frontend.FrontEndQuery;
import com.latticeengines.domain.exposed.serviceapps.cdl.ReportConstants;
import com.latticeengines.domain.exposed.serviceflows.cdl.steps.process.ProcessStepConfiguration;
import com.latticeengines.domain.exposed.util.PAReportUtils;
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
        updateReportEntitiesSummaryReport(jsonReport);
        Report report = createReport(jsonReport.toString(), ReportPurpose.PROCESS_ANALYZE_RECORDS_SUMMARY,
                UUID.randomUUID().toString());
        registerReport(configuration.getCustomerSpace(), report);
        log.info("Registered report: " + jsonReport.toString());
    }

    private void updateSystemActionsReport(ObjectNode report) {
        ArrayNode systemActionNode = report.get(ReportPurpose.SYSTEM_ACTIONS.getKey()) != null
                ? (ArrayNode) report.get(ReportPurpose.SYSTEM_ACTIONS.getKey())
                : report.putArray(ReportPurpose.SYSTEM_ACTIONS.getKey());
        List<Action> dataCloudChangeActions = getDataCloudChangeActions();
        dataCloudChangeActions.forEach(action -> {
            ObjectNode actionNode = JsonUtils.createObjectNode();
            // discussed with Afroz that displaying action time here is
            // confusing and should be removed.
            // actionNode.put(ReportConstants.TIME,
            // sdf.format(action.getCreated()));
            actionNode.put(ReportConstants.ACTION, action.getType().getDisplayName());
            systemActionNode.add(actionNode);
        });
    }

    private void updateReportEntitiesSummaryReport(ObjectNode report) {
        Map<BusinessEntity, Long> currentCnts = retrieveCurrentEntityCnts();
        Map<BusinessEntity, Long> deleteCnts = getDeletedCount();

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

            entitiesSummaryNode.set(entity.name(), entityNode);
        }

        updateCollectionStatus(currentCnts);
    }

    private void updateCollectionStatus(Map<BusinessEntity, Long> currentCnts) {
        DataCollectionStatus detail = getObjectFromContext(CDL_COLLECTION_STATUS, DataCollectionStatus.class);
        detail.setAccountCount(currentCnts.get(BusinessEntity.Account));
        detail.setContactCount(currentCnts.get(BusinessEntity.Contact));
        detail.setTransactionCount(currentCnts.get(BusinessEntity.Transaction));
        putObjectInContext(CDL_COLLECTION_STATUS, detail);
        log.info("GenerateProcessingReport step: dataCollection Status is " + JsonUtils.serialize(detail));
        dataCollectionProxy.saveOrUpdateDataCollectionStatus(customerSpace.toString(), detail, inactive);

    }

    private Map<BusinessEntity, Long> retrieveCurrentEntityCnts() {
        Map<BusinessEntity, Long> currentCnts = new HashMap<>();
        currentCnts.put(BusinessEntity.Account, countInRedshift(BusinessEntity.Account));
        currentCnts.put(BusinessEntity.Contact, countInRedshift(BusinessEntity.Contact));
        currentCnts.put(BusinessEntity.Product, countInRedshift(BusinessEntity.Product));
        currentCnts.put(BusinessEntity.Transaction, countRawTransactionInHdfs());
        return currentCnts;
    }

    private long countRawTransactionInHdfs() {
        try {
            String rawTableName = dataCollectionProxy.getTableName(customerSpace.toString(),
                    TableRoleInCollection.ConsolidatedRawTransaction, inactive);
            if (StringUtils.isBlank(rawTableName)) {
                log.info("Cannot find raw transaction table in version " + inactive);
                rawTableName = dataCollectionProxy.getTableName(customerSpace.toString(),
                        TableRoleInCollection.ConsolidatedRawTransaction, active);
                if (StringUtils.isBlank(rawTableName)) {
                    log.info("Cannot find raw transaction table in version " + active);
                    return 0L;
                }
            }
            Table rawTable = metadataProxy.getTable(customerSpace.toString(), rawTableName);
            if (rawTable == null) {
                log.error("Cannot find raw transaction table " + rawTableName);
                return 0L;
            }

            String hdfsPath = rawTable.getExtracts().get(0).getPath();
            if (!hdfsPath.endsWith("*.avro")) {
                if (hdfsPath.endsWith("/")) {
                    hdfsPath += "*.avro";
                } else {
                    hdfsPath += "/*.avro";
                }
            }
            return AvroUtils.count(yarnConfiguration, hdfsPath);
        } catch (Exception ex) {
            log.error("Fail to count raw transaction table", ex);
            return 0L;
        }
    }

    private long countInRedshift(BusinessEntity entity) {
        String servingStore;
        try {
            servingStore = dataCollectionProxy.getTableName(customerSpace.toString(), entity.getServingStore(),
                    inactive);
        } catch (Exception ex) {
            log.error("Fail to look for serving store for entity " + entity.name(), ex);
            return 0L;
        }
        if (StringUtils.isBlank(servingStore)) {
            log.info("Cannot find serving store for entity " + entity.name() + " with version " + inactive.name());
            return 0L;
        }
        FrontEndQuery frontEndQuery = new FrontEndQuery();
        frontEndQuery.setMainEntity(entity);
        int retries = 0;
        while (retries < 3) {
            try {
                return ratingProxy.getCountFromObjectApi(customerSpace.toString(), frontEndQuery, inactive);
            } catch (Exception ex) {
                log.error("Exception in getting count from serving store for entity " + entity.name() + " with version "
                        + inactive.name(), ex);
                retries++;
                try {
                    Thread.sleep(2000);
                } catch (InterruptedException e) {
                    // do nothing
                }
            }
        }
        log.error("Fail to get count from serving store for entity " + entity.name() + " with version "
                + inactive.name());
        return 0L;
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
        return actions.stream()
                .filter(action -> ActionType.getDataCloudRelatedTypes().contains(action.getType()))
                .filter(action -> action.getOwnerId().equals(configuration.getOwnerId()))
                .collect(Collectors.toList());
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
