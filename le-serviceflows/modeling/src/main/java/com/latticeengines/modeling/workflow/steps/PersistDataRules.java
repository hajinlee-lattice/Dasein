package com.latticeengines.modeling.workflow.steps;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import javax.inject.Inject;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.db.exposed.entitymgr.TenantEntityMgr;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.modelreview.BaseRuleResult;
import com.latticeengines.domain.exposed.modelreview.ColumnRuleResult;
import com.latticeengines.domain.exposed.modelreview.DataRule;
import com.latticeengines.domain.exposed.modelreview.RowRuleResult;
import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.domain.exposed.serviceflows.modeling.steps.ModelStepConfiguration;
import com.latticeengines.proxy.exposed.metadata.MetadataProxy;
import com.latticeengines.workflow.exposed.build.BaseWorkflowStep;

@Component("persistDataRules")
@Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
public class PersistDataRules extends BaseWorkflowStep<ModelStepConfiguration> {

    private static final Logger log = LoggerFactory.getLogger(PersistDataRules.class);

    @Inject
    private MetadataProxy metadataProxy;

    @Inject
    private TenantEntityMgr tenantEntityMgr;

    @Override
    public void execute() {
        @SuppressWarnings("unchecked")
        Map<String, String> eventToModelId = getObjectFromContext(EVENT_TO_MODELID, Map.class);
        log.info("Persisting rule results");
        persistReviewResults(eventToModelId);

        if (configuration.getDataRules() == null) {
            log.info("No datarules in configuration, nothing to do.");
        } else {
            List<DataRule> dataRules = getListObjectFromContext(DATA_RULES, DataRule.class);
            if (configuration.isDefaultDataRuleConfiguration()) {
                setResultsOnDefaultRules(dataRules);
            }

            log.info("Persisting datarules: " + JsonUtils.serialize(dataRules));
            Table eventTable = getObjectFromContext(EVENT_TABLE, Table.class);
            eventTable.setDataRules(dataRules);
            metadataProxy.updateTable(configuration.getCustomerSpace().toString(), eventTable.getName(), eventTable);
            putObjectInContext(EVENT_TABLE, eventTable);
        }
    }

    @SuppressWarnings("rawtypes")
    private void setResultsOnDefaultRules(List<DataRule> dataRules) {
        for (DataRule dataRule : dataRules) {
            if (dataRule.isEnabled()) {
                List<String> columnNames = new ArrayList<>();
                Map<String, List> eventToColumnResults = getMapObjectFromContext(COLUMN_RULE_RESULTS, String.class,
                        List.class);
                Iterator<List> iter = eventToColumnResults.values().iterator();
                if (iter.hasNext()) {
                    List<ColumnRuleResult> results = JsonUtils.convertList(iter.next(), ColumnRuleResult.class);
                    for (ColumnRuleResult result : results) {
                        if (result.getDataRuleName().equals(dataRule.getName())) {
                            columnNames = result.getFlaggedColumnNames();
                            dataRule.setFlaggedColumnNames(columnNames);
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

    @SuppressWarnings("rawtypes")
    private void persistReviewResults(Map<String, String> eventToModelId) {
        Map<String, List> eventToColumnResults = getMapObjectFromContext(COLUMN_RULE_RESULTS, String.class, List.class);
        Map<String, List> eventToRowResults = getMapObjectFromContext(ROW_RULE_RESULTS, String.class, List.class);
        if (eventToColumnResults == null || eventToRowResults == null || eventToColumnResults.isEmpty()
                || eventToRowResults.isEmpty()) {
            log.warn("COLUMN_RULE_RESULTS or ROW_RULE_RESULTS is empty");
            return;
        }
        Tenant tenant = tenantEntityMgr.findByTenantId(configuration.getCustomerSpace().toString());
        for (String event : eventToModelId.keySet()) {
            String modelId = eventToModelId.get(event);

            List<ColumnRuleResult> columnResults = JsonUtils.convertList(eventToColumnResults.get(event),
                    ColumnRuleResult.class);
            List<RowRuleResult> rowResults = JsonUtils.convertList(eventToRowResults.get(event), RowRuleResult.class);
            setModelIdAndTenantOnRuleResults(columnResults, modelId, tenant);
            setModelIdAndTenantOnRuleResults(rowResults, modelId, tenant);
            metadataProxy.createColumnResults(columnResults);
            metadataProxy.createRowResults(rowResults);
        }
    }

    private void setModelIdAndTenantOnRuleResults(List<? extends BaseRuleResult> results, String modelId,
            Tenant tenant) {
        for (BaseRuleResult result : results) {
            result.setModelId(modelId);
            result.setTenant(tenant);
        }
    }

}
