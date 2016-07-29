package com.latticeengines.leadprioritization.workflow.steps;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.modelreview.BaseRuleResult;
import com.latticeengines.domain.exposed.modelreview.ColumnRuleResult;
import com.latticeengines.domain.exposed.modelreview.DataRule;
import com.latticeengines.domain.exposed.modelreview.RowRuleResult;
import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.proxy.exposed.metadata.MetadataProxy;
import com.latticeengines.security.exposed.entitymanager.TenantEntityMgr;
import com.latticeengines.serviceflows.workflow.core.BaseWorkflowStep;
import com.latticeengines.serviceflows.workflow.modeling.ModelStepConfiguration;

@Component("persistDataRules")
public class PersistDataRules extends BaseWorkflowStep<ModelStepConfiguration> {

    private static final Log log = LogFactory.getLog(PersistDataRules.class);

    @Autowired
    private MetadataProxy metadataProxy;

    @Autowired
    private TenantEntityMgr tenantEntityMgr;

    @Override
    public void execute() {
        @SuppressWarnings("unchecked")
        Map<String, String> eventToModelId = (Map<String, String>) executionContext.get(EVENT_TO_MODELID);
        log.info("Persisting rule results");
        persistReviewResults(eventToModelId);

        if (configuration.getDataRules() == null) {
            log.info("No datarules in configuration, nothing to do.");
        } else {
            @SuppressWarnings("unchecked")
            List<DataRule> dataRules = (List<DataRule>) executionContext.get(DATA_RULES);
            if (configuration.isDefaultDataRuleConfiguration()) {
                setResultsOnDefaultRules(dataRules);
            }

            log.info("Persisting datarules: " + JsonUtils.serialize(dataRules));
            Table eventTable = JsonUtils.deserialize(getStringValueFromContext(EVENT_TABLE), Table.class);
            eventTable.setDataRules(dataRules);
            metadataProxy.updateTable(configuration.getCustomerSpace().toString(), eventTable.getName(), eventTable);
            putObjectInContext(EVENT_TABLE, JsonUtils.serialize(eventTable));
        }
    }

    private void setResultsOnDefaultRules(List<DataRule> dataRules) {
        for (DataRule dataRule : dataRules) {
            if (dataRule.isEnabled()) {
                List<String> columnNames = new ArrayList<>();

                @SuppressWarnings("unchecked")
                Map<String, List<ColumnRuleResult>> eventToColumnResults = (Map<String, List<ColumnRuleResult>>) executionContext
                        .get(COLUMN_RULE_RESULTS);
                Iterator<List<ColumnRuleResult>> iter = eventToColumnResults.values().iterator();
                if (iter.hasNext()) {
                    List<ColumnRuleResult> results = iter.next();
                    for (ColumnRuleResult result : results) {
                        if (result.getDataRuleName().equals(dataRule.getName())) {
                            columnNames = result.getFlaggedColumnNames();
                            dataRule.setColumnsToRemediate(columnNames);
                        }
                    }
                }

                if (!columnNames.isEmpty()) {
                    log.info(String.format("Datarule %s detected these columns for default remediation: %s",
                            dataRule.getName(), columnNames.toString()));
                }
            }
        }
    }

    @SuppressWarnings("unchecked")
    private void persistReviewResults(Map<String, String> eventToModelId) {
        Map<String, List<ColumnRuleResult>> eventToColumnResults = (Map<String, List<ColumnRuleResult>>) executionContext
                .get(COLUMN_RULE_RESULTS);
        Map<String, List<RowRuleResult>> eventToRowResults = (Map<String, List<RowRuleResult>>) executionContext
                .get(ROW_RULE_RESULTS);
        if (eventToColumnResults == null || eventToRowResults == null) {
            log.warn("COLUMN_RULE_RESULTS or ROW_RULE_RESULTS is null");
            return;
        }
        Tenant tenant = tenantEntityMgr.findByTenantId(configuration.getCustomerSpace().toString());
        for (String event : eventToModelId.keySet()) {
            String modelId = eventToModelId.get(event);

            List<ColumnRuleResult> columnResults = eventToColumnResults.get(event);
            List<RowRuleResult> rowResults = eventToRowResults.get(event);
            setModelIdAndTenantOnRuleResults(columnResults, modelId, tenant);
            setModelIdAndTenantOnRuleResults(rowResults, modelId, tenant);
            metadataProxy.createColumnResults(columnResults);
            metadataProxy.createRowResults(rowResults);
        }
    }

    private void setModelIdAndTenantOnRuleResults(List<? extends BaseRuleResult> results, String modelId, Tenant tenant) {
        for (BaseRuleResult result : results) {
            result.setModelId(modelId);
            result.setTenant(tenant);
        }
    }

}
