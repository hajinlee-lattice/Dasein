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
            List<DataRule> dataRules = configuration.getDataRules();
            log.info("Remediating datarules: " + JsonUtils.serialize(dataRules));
            Table eventTable = JsonUtils.deserialize(getStringValueFromContext(EVENT_TABLE), Table.class);
            eventTable = remediateAttributes(dataRules, eventTable, configuration.isDefaultDataRuleConfiguration());
            eventTable.setDataRules(dataRules);
            if (configuration.isDefaultDataRuleConfiguration()) {
                metadataProxy
                        .updateTable(configuration.getCustomerSpace().toString(), eventTable.getName(), eventTable);
            }
            putObjectInContext(EVENT_TABLE, JsonUtils.serialize(eventTable));
        }
    }

    public Table remediateAttributes(List<DataRule> dataRules, Table eventTable, boolean isDefault) {
        Set<String> columnsToRemove = new HashSet<>();

        for (DataRule dataRule : dataRules) {
            if (dataRule.isEnabled()) {
                List<String> columnNames = new ArrayList<>();
                if (isDefault) {
                    @SuppressWarnings("unchecked")
                    Map<String, List<ColumnRuleResult>> eventToColumnResults = (Map<String, List<ColumnRuleResult>>) executionContext
                            .get(COLUMN_RULE_RESULTS);
                    Iterator<List<ColumnRuleResult>> iter = eventToColumnResults.values().iterator();
                    if (iter.hasNext()) {
                        List<ColumnRuleResult> results = iter.next();
                        for (ColumnRuleResult result : results) {
                            if (result.getDataRuleName().equals(dataRule.getName())) {
                                columnNames = result.getFlaggedColumnNames();
                            }
                        }
                    }
                } else {
                    if (!CollectionUtils.isEmpty(dataRule.getColumnsToRemediate())) {
                        columnNames = dataRule.getColumnsToRemediate();
                    }
                }
                for (String columnName : columnNames) {
                    columnsToRemove.add(columnName);
                }
            }
        }

        log.info("Remediating these columns: " + JsonUtils.serialize(columnsToRemove));
        for (Attribute attr : eventTable.getAttributes()) {
            if (columnsToRemove.contains(attr.getName())) {
                attr.setApprovedUsage(ApprovedUsage.NONE);
            }
        }

        return eventTable;
    }

}
