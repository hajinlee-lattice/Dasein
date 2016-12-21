package com.latticeengines.leadprioritization.workflow.steps;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.metadata.ApprovedUsage;
import com.latticeengines.domain.exposed.metadata.Attribute;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.modelreview.ColumnRuleResult;
import com.latticeengines.domain.exposed.modelreview.DataRule;
import com.latticeengines.proxy.exposed.metadata.MetadataProxy;
import com.latticeengines.serviceflows.workflow.core.BaseWorkflowStep;
import com.latticeengines.serviceflows.workflow.modeling.ModelStepConfiguration;

@Component("remediateDataRules")
public class RemediateDataRules extends BaseWorkflowStep<ModelStepConfiguration> {

    private static final Log log = LogFactory.getLog(RemediateDataRules.class);

    @Autowired
    private MetadataProxy metadataProxy;

    @Override
    public void execute() {
        if (configuration.getDataRules() == null) {
            log.info("No datarules in configuration, nothing to do.");
        } else {
            List<DataRule> dataRules = new ArrayList<>(configuration.getDataRules());
            log.info("Remediating datarules: " + JsonUtils.serialize(dataRules));
            Table eventTable = getObjectFromContext(EVENT_TABLE, Table.class);
            editTableAgainstDataRules(dataRules, eventTable,
                    configuration.isDefaultDataRuleConfiguration());
            eventTable.setDataRules(dataRules);
            if (configuration.isDefaultDataRuleConfiguration()) {
                metadataProxy.updateTable(configuration.getCustomerSpace().toString(),
                        eventTable.getName(), eventTable);
            }
            putObjectInContext(EVENT_TABLE, eventTable);
            putObjectInContext(DATA_RULES, dataRules);
        }
    }

    @SuppressWarnings("rawtypes")
    private void editTableAgainstDataRules(List<DataRule> dataRules, Table eventTable, boolean isDefault) {
        for (DataRule dataRule : dataRules) {
            if (dataRule.isEnabled()){
                if (isDefault) {
                    Map<String, List> eventToColumnResults = getMapObjectFromContext(COLUMN_RULE_RESULTS,
                            String.class, List.class);
                    Iterator<List> iter = eventToColumnResults.values().iterator();
                    if (iter.hasNext()) {
                        List<ColumnRuleResult> results = JsonUtils.convertList(iter.next(), ColumnRuleResult.class);
                        for (ColumnRuleResult result : results) {
                            if (result.getDataRuleName().equals(dataRule.getName())) {
                                dataRule.setFlaggedColumnNames(result.getFlaggedColumnNames());
                            }
                        }
                    }
                }
                log.info(String.format("Enabled columns: %s for data rule: %s",
                        JsonUtils.serialize(dataRule.getFlaggedColumnNames()), dataRule.getName()));
                editTableAttributesAgainstDataRule(dataRule, eventTable);
            }
        }
    }

    @SuppressWarnings("rawtypes")
    private void editTableAttributesAgainstDataRule(DataRule dataRule, Table eventTable) {
        if (!CollectionUtils.isEmpty(dataRule.getFlaggedColumnNames())) {
            for (String flaggedColumn : dataRule.getFlaggedColumnNames()) {
                Attribute attribute = eventTable.getAttribute(flaggedColumn);
                attribute.addAssociatedDataRuleName(dataRule.getName());
                if (dataRule.hasMandatoryRemoval()) {
                    attribute.setApprovedUsage(ApprovedUsage.NONE);
                    attribute.setIsCoveredByMandatoryRule(true);
                } else {
                    attribute.setIsCoveredByOptionalRule(true);
                }
            }
        }
    }

}
