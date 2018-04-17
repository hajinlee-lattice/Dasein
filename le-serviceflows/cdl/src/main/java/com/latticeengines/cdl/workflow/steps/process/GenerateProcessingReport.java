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
import java.util.stream.Collectors;

import javax.inject.Inject;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.latticeengines.cdl.workflow.steps.CloneTableService;
import com.latticeengines.common.exposed.util.AvroUtils;
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
import com.latticeengines.proxy.exposed.cdl.DataCollectionProxy;
import com.latticeengines.proxy.exposed.metadata.MetadataProxy;
import com.latticeengines.proxy.exposed.objectapi.RatingProxy;
import com.latticeengines.proxy.exposed.workflowapi.WorkflowProxy;
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
    private WorkflowProxy workflowProxy;

    @Inject
    private CloneTableService cloneTableService;

    private DataCollection.Version active;
    private DataCollection.Version inactive;
    private CustomerSpace customerSpace;

    @Override
    public void execute() {
        customerSpace = configuration.getCustomerSpace();
        active = getObjectFromContext(CDL_ACTIVE_VERSION, DataCollection.Version.class);
        inactive = getObjectFromContext(CDL_INACTIVE_VERSION, DataCollection.Version.class);
        swapMissingTableRoles();
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
        updateReport(jsonReport);
        Report report = createReport(jsonReport.toString(), ReportPurpose.PROCESS_ANALYZE_RECORDS_SUMMARY,
                UUID.randomUUID().toString());
        registerReport(configuration.getCustomerSpace(), report);
        log.info("Registered report: " + jsonReport.toString());
    }

    private void updateReport(ObjectNode report) {
        Map<BusinessEntity, Long> previousCnts = retrievePreviousEntityCnts();
        Map<BusinessEntity, Long> currentCnts = retrieveCurrentEntityCnts();

        ObjectNode entitiesSummaryNode = (ObjectNode) report.get(ReportPurpose.ENTITIES_SUMMARY.getKey());
        if (entitiesSummaryNode == null) {
            log.info("No entity summary reports found. Create it.");
            entitiesSummaryNode = report.putObject(ReportPurpose.ENTITIES_SUMMARY.getKey());
        }
        BusinessEntity[] entities = { BusinessEntity.Account, BusinessEntity.Contact, BusinessEntity.Product,
                BusinessEntity.Transaction };
        for (BusinessEntity entity : entities) {
            ObjectNode entityNode = entitiesSummaryNode.get(entity.name()) != null
                    ? (ObjectNode) entitiesSummaryNode.get(entity.name()) : initEntityReport(entity);
            ObjectNode consolidateSummaryNode = (ObjectNode) entityNode
                    .get(ReportPurpose.CONSOLIDATE_RECORDS_SUMMARY.getKey());
            if (entity != BusinessEntity.Product) {
                long newCnt = consolidateSummaryNode.get("NEW").asLong();
                long deleteCnt = newCnt - (currentCnts.get(entity) - previousCnts.get(entity));
                log.info(String.format(
                        "For entity %s, previous total count: %d, current total count: %d, new count: %s, delete count: %d",
                        entity.name(), previousCnts.get(entity), currentCnts.get(entity), newCnt, deleteCnt));
                consolidateSummaryNode.put("DELETE", String.valueOf(deleteCnt));
                ObjectNode entityNumberNode = JsonUtils.createObjectNode();
                entityNumberNode.put("TOTAL", String.valueOf(currentCnts.get(entity)));
                entityNode.set(ReportPurpose.ENTITY_STATS_SUMMARY.getKey(), entityNumberNode);
            } else {
                long idCnt = consolidateSummaryNode.get("PRODUCT_ID").asLong();
                long bundleCnt = consolidateSummaryNode.get("PRODUCT_HIERARCHY").asLong();
                long hierarchyCnt = consolidateSummaryNode.get("PRODUCT_HIERARCHY").asLong();
                String errMsg = consolidateSummaryNode.get("ERROR_MESSAGE").asText();
                String warnMsg = consolidateSummaryNode.get("WARN_MESSAGE").asText();
                log.info(String.format("For entity %s, productId count: %d, bundle count: %d, hierarchy count: %d, " +
                                "error message: %s, warning message: %s",
                        entity.name(), idCnt, bundleCnt, hierarchyCnt, errMsg, warnMsg));
                ObjectNode entityNumberNode = JsonUtils.createObjectNode();
                entityNumberNode.put("TOTAL", String.valueOf(idCnt));
                entityNode.set(ReportPurpose.ENTITY_STATS_SUMMARY.getKey(), entityNumberNode);
            }

            entitiesSummaryNode.set(entity.name(), entityNode);
        }
    }

    private Map<BusinessEntity, Long> retrievePreviousEntityCnts() {
        Map<BusinessEntity, Long> previousCnts = new HashMap<>();
        List<String> types = Collections.singletonList("processAnalyzeWorkflow");
        List<Job> jobs = workflowProxy.getJobs(null, types, Boolean.TRUE, customerSpace.toString());
        Optional<Job> latestSuccessJob = jobs.stream().filter(job -> job.getJobStatus() == JobStatus.COMPLETED)
                .max(Comparator.comparing(Job::getEndTimestamp));
        BusinessEntity[] entities = { BusinessEntity.Account, BusinessEntity.Contact, BusinessEntity.Product,
                BusinessEntity.Transaction };
        try {
            if (latestSuccessJob.isPresent()) {
                Report report = latestSuccessJob.get().getReports().stream()
                        .filter(r -> r.getPurpose() == ReportPurpose.PROCESS_ANALYZE_RECORDS_SUMMARY)
                        .collect(Collectors.toList()).get(0);
                ObjectMapper om = JsonUtils.getObjectMapper();
                ObjectNode jsonReport = (ObjectNode) om.readTree(report.getJson().getPayload());
                ObjectNode entitiesSummaryNode = (ObjectNode) jsonReport.get(ReportPurpose.ENTITIES_SUMMARY.getKey());
                Arrays.stream(entities)
                        .filter(entity -> entitiesSummaryNode.get(entity.name()) != null && entitiesSummaryNode
                                .get(entity.name()).get(ReportPurpose.ENTITY_STATS_SUMMARY.getKey()) != null)
                        .forEach(entity -> previousCnts.put(entity, entitiesSummaryNode.get(entity.name())
                                .get(ReportPurpose.ENTITY_STATS_SUMMARY.getKey()).get("TOTAL").asLong()));
            } else {
                log.info("Cannot find previous successful processAnalyzeWorkflow job");
            }
        } catch (Exception e) {
            log.error("Fail to parse report from job: " + JsonUtils.serialize(latestSuccessJob.get()));
        } finally {
            for (BusinessEntity entity : entities) {
                if (!previousCnts.containsKey(entity)) {
                    log.info(String.format("Cannot find previous count for entity %s in previous job report. Set as 0.",
                            entity.name()));
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
        try {
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
            log.error(String.format("Fail to count raw transaction table %s", rawTableName), ex);
            return 0L;
        }
    }

    private long countInRedshift(BusinessEntity entity) {
        if (StringUtils.isBlank(
                dataCollectionProxy.getTableName(customerSpace.toString(), entity.getServingStore(), inactive))) {
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
                }
            }
        }
        log.error("Fail to get count from serving store for entity " + entity.name() + " with version "
                + inactive.name());
        return 0L;
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
            consolidateSummaryNode.put("PRODUCT_ID", "0");
            consolidateSummaryNode.put("PRODUCT_HIERARCHY", "0");
            consolidateSummaryNode.put("PRODUCT_BUNDLE", "0");
            consolidateSummaryNode.put("ERROR_MESSAGE", "");
            consolidateSummaryNode.put("WARN_MESSAGE", "");
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
