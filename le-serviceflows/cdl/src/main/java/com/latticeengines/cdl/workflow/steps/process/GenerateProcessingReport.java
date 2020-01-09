package com.latticeengines.cdl.workflow.steps.process;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import javax.inject.Inject;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.collections4.MapUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.latticeengines.cdl.workflow.steps.CloneTableService;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.metadata.DataCollection;
import com.latticeengines.domain.exposed.metadata.TableRoleInCollection;
import com.latticeengines.domain.exposed.pls.Action;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.domain.exposed.serviceflows.cdl.steps.process.ProcessStepConfiguration;
import com.latticeengines.domain.exposed.workflow.Report;
import com.latticeengines.domain.exposed.workflow.ReportPurpose;
import com.latticeengines.proxy.exposed.cdl.DataCollectionProxy;
import com.latticeengines.workflow.exposed.build.BaseWorkflowStep;

@Component("generateProcessingReport")
@Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
public class GenerateProcessingReport extends BaseWorkflowStep<ProcessStepConfiguration> {

    protected static final Logger log = LoggerFactory.getLogger(GenerateProcessingReport.class);

    @Inject
    private DataCollectionProxy dataCollectionProxy;

    @Inject
    private CloneTableService cloneTableService;

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

            if (role.isHasSignature()) {
                log.info("Role {} has signature attached to its table, linking with signatures", role);
                cloneTableService.linkToInactiveTableWithSignature(role);
            } else {
                cloneTableService.linkInactiveTable(role);
            }
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
        updateDecisionsReport(jsonReport);
        updateWarningReport(jsonReport);
        Report report = createReport(jsonReport.toString(), ReportPurpose.PROCESS_ANALYZE_RECORDS_SUMMARY,
                UUID.randomUUID().toString());
        registerReport(configuration.getCustomerSpace(), report);
        log.info("Registered report: " + jsonReport.toString());
    }

    private void updateWarningReport(ObjectNode report) {
        List<String> warningMessages = getListObjectFromContext(PROCESS_ANALYTICS_WARNING_KEY, String.class);
        if (CollectionUtils.isNotEmpty(warningMessages)) {
            ArrayNode warningMessageNode = (ArrayNode) report
                    .get(ReportPurpose.PROCESS_ANALYZE_WARNING_SUMMARY.getKey());
            if (warningMessageNode == null) {
                log.info("No warningMessage summary reports found. Create it.");
                warningMessageNode = report.putArray(ReportPurpose.PROCESS_ANALYZE_WARNING_SUMMARY.getKey());
            }

            warningMessages.forEach(warningMessageNode::add);
            log.info("PA warning message is " + JsonUtils.serialize(warningMessageNode));
        }
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

}
