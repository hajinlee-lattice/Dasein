package com.latticeengines.cdl.workflow.steps.process;

import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;

import javax.inject.Inject;

import org.apache.commons.collections4.MapUtils;
import org.apache.commons.lang3.StringUtils;
import org.springframework.stereotype.Component;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.metadata.DataCollection;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.metadata.TableRoleInCollection;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.domain.exposed.query.frontend.FrontEndQuery;
import com.latticeengines.domain.exposed.serviceflows.cdl.steps.process.ProcessStepConfiguration;
import com.latticeengines.domain.exposed.workflow.Job;
import com.latticeengines.domain.exposed.workflow.JobStatus;
import com.latticeengines.domain.exposed.workflow.Report;
import com.latticeengines.domain.exposed.workflow.ReportPurpose;
import com.latticeengines.proxy.exposed.cdl.RatingEngineProxy;
import com.latticeengines.proxy.exposed.metadata.DataCollectionProxy;
import com.latticeengines.proxy.exposed.metadata.MetadataProxy;
import com.latticeengines.proxy.exposed.metadata.SegmentProxy;
import com.latticeengines.proxy.exposed.objectapi.EntityProxy;
import com.latticeengines.proxy.exposed.objectapi.RatingProxy;
import com.latticeengines.proxy.exposed.workflowapi.WorkflowProxy;
import com.latticeengines.serviceflows.workflow.core.BaseWorkflowStep;

@Component("finishProcessing")
public class FinishProcessing extends BaseWorkflowStep<ProcessStepConfiguration> {

    @Inject
    private DataCollectionProxy dataCollectionProxy;

    @Inject
    private SegmentProxy segmentProxy;

    @Inject
    private RatingEngineProxy ratingEngineProxy;

    @Inject
    private EntityProxy entityProxy;

    @Inject
    private MetadataProxy metadataProxy;

    @Inject
    private RatingProxy ratingProxy;

    @Inject
    private WorkflowProxy workflowProxy;

    private DataCollection.Version active;
    private DataCollection.Version inactive;
    private CustomerSpace customerSpace;

    @Override
    public void execute() {
        customerSpace = configuration.getCustomerSpace();
        active = getObjectFromContext(CDL_ACTIVE_VERSION, DataCollection.Version.class);
        inactive = getObjectFromContext(CDL_INACTIVE_VERSION, DataCollection.Version.class);

        deleteOrphanTables();
        swapMissingTableRoles();

        log.info("Switch data collection to version " + inactive);
        dataCollectionProxy.switchVersion(customerSpace.toString(), inactive);
        log.info("Evict attr repo cache for inactive version " + inactive);
        dataCollectionProxy.evictAttrRepoCache(customerSpace.toString(), inactive);
        if (StringUtils.isNotBlank(configuration.getDataCloudBuildNumber())) {
            dataCollectionProxy.updateDataCloudBuildNumber(customerSpace.toString(),
                    configuration.getDataCloudBuildNumber());
        }
        try {
            // wait for local cache clean up
            Thread.sleep(5000);
        } catch (InterruptedException e) {
            // ignore
        }

        // update segment and rating engine counts
        SegmentCountUtils.updateEntityCounts(segmentProxy, entityProxy, customerSpace.toString());
        RatingEngineCountUtils.updateRatingEngineCounts(ratingEngineProxy, customerSpace.toString());
        registerReport();
    }

    private void swapMissingTableRoles() {
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
            String inactiveTableName = dataCollectionProxy.getTableName(customerSpace.toString(), role, inactive);
            if (StringUtils.isBlank(inactiveTableName)) {
                String activeTableName = dataCollectionProxy.getTableName(customerSpace.toString(), role, active);
                if (StringUtils.isNotBlank(activeTableName)) {
                    log.info("Swapping table " + activeTableName + " from " + active + " to " + inactive + " as "
                            + role);
                    dataCollectionProxy.unlinkTable(customerSpace.toString(), activeTableName, role, active);
                    dataCollectionProxy.upsertTable(customerSpace.toString(), activeTableName, role, inactive);
                }
            }
        }
    }

    private void deleteOrphanTables() {
        cleanupEntityTableMap(getMapObjectFromContext(ENTITY_DIFF_TABLES, BusinessEntity.class, String.class));
        cleanupEntityTableMap(getMapObjectFromContext(TABLE_GOING_TO_REDSHIFT, BusinessEntity.class, String.class));
    }

    private void cleanupEntityTableMap(Map<BusinessEntity, String> entityTableNames) {
        if (MapUtils.isNotEmpty(entityTableNames)) {
            entityTableNames.forEach((entity, tableName) -> {
                String servingStoreName = dataCollectionProxy.getTableName(customerSpace.toString(),
                        entity.getServingStore(), inactive);
                if (StringUtils.isBlank(servingStoreName)) {
                    log.info("Removing orphan table " + tableName);
                    metadataProxy.deleteTable(customerSpace.toString(), tableName);
                }
            });
        }
    }

    private Map<BusinessEntity, Long> retrievePreviousEntityCnts() {
        Map<BusinessEntity, Long> previousCnts = new HashMap<>();
        List<String> types = Arrays.asList("processAnalyzeWorkflow");
        List<Job> jobs = workflowProxy.getJobs(null, types, Boolean.TRUE, customerSpace.toString());
        Optional<Job> latestSuccessJob = jobs.stream().filter(job -> job.getJobStatus() == JobStatus.COMPLETED)
                .max(new Comparator<Job>() {
                    @Override
                    public int compare(Job job1, Job job2) {
                        return job1.getEndTimestamp().compareTo(job2.getEndTimestamp());
                    }
                });
        BusinessEntity[] entities = { BusinessEntity.Account, BusinessEntity.Contact, BusinessEntity.Product,BusinessEntity.Transaction };
        try {
            if (latestSuccessJob.isPresent()) {
                Report report = latestSuccessJob.get().getReports().get(0); // 0: EntitySummaryReport, 1: PublishSummaryReport
                ObjectMapper om = JsonUtils.getObjectMapper();
                ObjectNode jsonReport = (ObjectNode) om.readTree(report.getJson().getPayload());
                ObjectNode entitiesSummaryNode = (ObjectNode) jsonReport.get(ReportPurpose.ENTITIES_SUMMARY.getKey());
                Arrays.stream(entities)
                        .filter(entity -> entitiesSummaryNode.get(entity.name()) != null && entitiesSummaryNode
                                .get(entity.name()).get(ReportPurpose.ENTITY_STATS_SUMMARY.getKey()) != null)
                        .forEach(entity -> previousCnts.put(entity, entitiesSummaryNode.get(entity.name())
                                .get(ReportPurpose.ENTITY_STATS_SUMMARY.getKey()).get("TOTAL").asLong()));
            }
        } catch (Exception e) {
            log.error("Fail to parse report from job: " + JsonUtils.serialize(latestSuccessJob.get()), e);
        } finally {
            for (BusinessEntity entity : entities) {
                if (!previousCnts.containsKey(entity)) {
                    previousCnts.put(entity, 0L);
                }
            }
        }
        return previousCnts;
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
        String rawTableName = dataCollectionProxy.getTableName(customerSpace.toString(),
                TableRoleInCollection.ConsolidatedRawTransaction, inactive);
        if (StringUtils.isBlank(rawTableName)) {
            log.warn("Cannot find raw transaction table.");
            return 0l;
        }
        log.info(String.format("Found raw transaction table %s in inactive version %s", rawTableName, inactive));
        Table rawTable = metadataProxy.getTable(customerSpace.toString(), rawTableName);
        if (rawTable == null) {
            log.warn("Cannot find raw transaction table.");
            return 0l;
        }
        return rawTable.getExtracts().get(0).getProcessedRecords();
    }

    private long countInRedshift(BusinessEntity entity) {
        FrontEndQuery frontEndQuery = new FrontEndQuery();
        frontEndQuery.setMainEntity(entity);
        return ratingProxy.getCountFromObjectApi(customerSpace.toString(), frontEndQuery, inactive);
    }

    private void registerReport() {
        ObjectNode jsonReport = getObjectFromContext(ReportPurpose.PROCESS_ANALYZE_RECORDS_SUMMARY.getKey(),
                ObjectNode.class);
        updateReport(jsonReport);
        Report report = createReport(jsonReport.toString(), ReportPurpose.PROCESS_ANALYZE_RECORDS_SUMMARY,
                UUID.randomUUID().toString());
        registerReport(configuration.getCustomerSpace(), report);
        log.info("Registered report: " + jsonReport.toString());
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

    private ObjectNode updateReport(ObjectNode report) {
        Map<BusinessEntity, Long> previousCnts = retrievePreviousEntityCnts();
        Map<BusinessEntity, Long> currentCnts = retrieveCurrentEntityCnts();


        ObjectNode entitiesSummaryNode = (ObjectNode)report.get(ReportPurpose.ENTITIES_SUMMARY.getKey());
        BusinessEntity[] entities = { BusinessEntity.Account, BusinessEntity.Contact, BusinessEntity.Product,
                BusinessEntity.Transaction };
        for (BusinessEntity entity : entities) {
            ObjectNode entityNode = entitiesSummaryNode.get(entity.name()) != null
                    ? (ObjectNode) entitiesSummaryNode.get(entity.name())
                    : initEntityReport(entity);
            ObjectNode consolidateSummaryNode = (ObjectNode) entityNode
                    .get(ReportPurpose.CONSOLIDATE_RECORDS_SUMMARY.getKey());
            long newCnt = consolidateSummaryNode.get("NEW").asLong();
            long deleteCnt = newCnt - (currentCnts.get(entity) - previousCnts.get(entity));
            consolidateSummaryNode.put("DELETE", String.valueOf(deleteCnt));

            ObjectNode entityNumberNode = JsonUtils.createObjectNode();
            entityNumberNode.put("TOTAL", String.valueOf(currentCnts.get(entity)));
            entityNode.set(ReportPurpose.ENTITY_STATS_SUMMARY.getKey(), entityNumberNode);

            entitiesSummaryNode.set(entity.name(), entityNode);
        }
        return report;
    }

    private ObjectNode initEntityReport(BusinessEntity entity) {
        ObjectNode entityNode = JsonUtils.createObjectNode();
        ObjectNode consolidateSummaryNode = JsonUtils.createObjectNode();
        switch (entity) {
        case Account:
            consolidateSummaryNode.put("NEW", "0");
            consolidateSummaryNode.put("UPDATE", "0");
            consolidateSummaryNode.put("UNMATCH", "0");
            break;
        case Contact:
            consolidateSummaryNode.put("NEW", "0");
            consolidateSummaryNode.put("UPDATE", "0");
            break;
        case Product:
            consolidateSummaryNode.put("NEW", "0");
            consolidateSummaryNode.put("UPDATE", "0");
            break;
        case Transaction:
            consolidateSummaryNode.put("NEW", "0");
            break;
        default:
            throw new UnsupportedOperationException(entity.name() + " business entity is not supported in P&A report");
        }
        entityNode.set(ReportPurpose.CONSOLIDATE_RECORDS_SUMMARY.getKey(), consolidateSummaryNode);
        return entityNode;
    }

}
