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
            eventTable = remediateAttributesAgainstMandatoryRules(dataRules, eventTable,
                    configuration.isDefaultDataRuleConfiguration());
            eventTable = markAttributesAgainstOptionalRules(dataRules, eventTable,
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
    public Table remediateAttributesAgainstMandatoryRules(List<DataRule> dataRules, Table eventTable, boolean isDefault) {
        Set<String> columnsToRemove = new HashSet<>();

        for (DataRule dataRule : dataRules) {
            if (dataRule.isEnabled() && dataRule.hasMandatoryRemoval()) {
                List<String> columnNames = new ArrayList<>();
                if (isDefault) {
                    Map<String, List> eventToColumnResults = getMapObjectFromContext(COLUMN_RULE_RESULTS, String.class, List.class);
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
                } else {
                    if (!CollectionUtils.isEmpty(dataRule.getFlaggedColumnNames())) {
                        columnNames = dataRule.getFlaggedColumnNames();
                    }
                }
                log.info(String.format("Enabled mandatory Datarule %s flagged %d columns for remediation: %s",
                        dataRule.getName(), columnNames.size(), JsonUtils.serialize(columnNames)));
                for (String columnName : columnNames) {
                    columnsToRemove.add(columnName);
                }
            }
        }

        log.info(String.format("Remediating total %d columns: %s", columnsToRemove.size(),
                JsonUtils.serialize(columnsToRemove)));
        for (Attribute attr : eventTable.getAttributes()) {
            if (columnsToRemove.contains(attr.getName())) {
                attr.setApprovedUsage(ApprovedUsage.NONE);
                attr.setIsCoveredByMandatoryRule(true);
            } else {
                attr.setIsCoveredByMandatoryRule(false);
            }
        }

        return eventTable;
    }

    @SuppressWarnings("rawtypes")
    public Table markAttributesAgainstOptionalRules(List<DataRule> dataRules, Table eventTable, boolean isDefault) {
        Set<String> columnsCoveredByOptionalRules = new HashSet<>();

        for (DataRule dataRule : dataRules) {
            if (dataRule.isEnabled() && !dataRule.hasMandatoryRemoval()) {
                if (isDefault) {
                    Map<String, List> eventToColumnResults = getMapObjectFromContext(COLUMN_RULE_RESULTS, String.class, List.class);
                    Iterator<List> iter = eventToColumnResults.values().iterator();
                    if (iter.hasNext()) {
                        List<ColumnRuleResult> results = JsonUtils.convertList(iter.next(), ColumnRuleResult.class);
                        for (ColumnRuleResult result : results) {
                            if (result.getDataRuleName().equals(dataRule.getName())) {
                                columnsCoveredByOptionalRules.addAll(result.getFlaggedColumnNames());
                            }
                        }
                    }
                } else {
                    if (!CollectionUtils.isEmpty(dataRule.getFlaggedColumnNames())) {
                        columnsCoveredByOptionalRules.addAll(dataRule.getFlaggedColumnNames());
                    }
                }
            }
        }

        log.info(String.format("Columns with name: %s are covered by optional data rules. Marking their properties",
                JsonUtils.serialize(columnsCoveredByOptionalRules)));
        for (Attribute attribute : eventTable.getAttributes()) {
            if (columnsCoveredByOptionalRules.contains(attribute.getName())) {
                attribute.setIsCoveredByOptionalRule(true);
            } else {
                attribute.setIsCoveredByOptionalRule(false);
            }
        }

        return eventTable;
    }

}
