package com.latticeengines.leadprioritization.workflow.steps;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.commons.collections.CollectionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
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
import com.latticeengines.domain.exposed.serviceflows.core.steps.ModelStepConfiguration;

@Component("remediateDataRules")
public class RemediateDataRules extends BaseWorkflowStep<ModelStepConfiguration> {

    private static final Logger log = LoggerFactory.getLogger(RemediateDataRules.class);

    @Autowired
    private MetadataProxy metadataProxy;

    @Override
    public void execute() {
        if (configuration.getDataRules() == null) {
            log.info("No datarules in configuration, nothing to do.");
        } else {
            List<DataRule> dataRules = new ArrayList<>(configuration.getDataRules());

            // This rule causes problematic derived attributes to be removed in
            // the pipeline before the modeling algorithm is executed. Doing
            // this (1) makes the ApprovedUsage consistent with the presence of
            // the derived attribute in the model and (2) leaves the attribute
            // available at the start of the pipeline for pipeline steps to use.
            DataRule derivedAttribute = new DataRule("DerivedAttribute");
            derivedAttribute.setDisplayName("Derived from a Removed Attribute");
            derivedAttribute.setDescription(
                    "This attribute is derived from an attribute that has been flagged and removed by a rule, and it suffers from the same problem as the source attribute.");
            derivedAttribute.setMandatoryRemoval(true);

            Table eventTable = getObjectFromContext(EVENT_TABLE, Table.class);
            editTableAgainstDataRules(dataRules, derivedAttribute, eventTable,
                    configuration.isDefaultDataRuleConfiguration());
            dataRules.add(derivedAttribute);
            log.info("Remediating datarules: " + JsonUtils.serialize(dataRules));
            eventTable.setDataRules(dataRules);
            if (configuration.isDefaultDataRuleConfiguration()) {
                metadataProxy.updateTable(configuration.getCustomerSpace().toString(), eventTable.getName(),
                        eventTable);
            }
            putObjectInContext(EVENT_TABLE, eventTable);
            putObjectInContext(DATA_RULES, dataRules);
        }
    }

    @SuppressWarnings("rawtypes")
    private void editTableAgainstDataRules(List<DataRule> dataRules, DataRule derivedAttribute, Table eventTable,
            boolean isDefault) {
        for (DataRule dataRule : dataRules) {
            if (dataRule.isEnabled()) {
                if (isDefault) {
                    Map<String, List> eventToColumnResults = getMapObjectFromContext(COLUMN_RULE_RESULTS, String.class,
                            List.class);
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
                editTableAttributesAgainstDataRule(dataRule, derivedAttribute, eventTable);
            }
        }
    }

    private void editTableAttributesAgainstDataRule(DataRule dataRule, DataRule derivedAttribute, Table eventTable) {
        if (!CollectionUtils.isEmpty(dataRule.getFlaggedColumnNames())) {

            Map<Attribute, List<Attribute>> parentChild = new HashMap<>();
            for (Attribute attribute : eventTable.getAttributes()) {
                for (String parentName : attribute.getParentAttributeNames()) {
                    Attribute parent = eventTable.getAttribute(parentName);
                    if (!parentChild.containsKey(parent)) {
                        parentChild.put(parent, new ArrayList<Attribute>());
                    }
                    parentChild.get(parent).add(attribute);
                }
            }

            for (String flaggedColumn : dataRule.getFlaggedColumnNames()) {
                Attribute attribute = eventTable.getAttribute(flaggedColumn);
                if (attribute != null) {
                    attribute.addAssociatedDataRuleName(dataRule.getName());
                    if (attribute.isCustomerPredictor()) {
                        dataRule.addCustomerPredictor(attribute.getName());
                    }
                    if (dataRule.hasMandatoryRemoval()) {
                        attribute.setApprovedUsage(ApprovedUsage.NONE);
                        attribute.setIsCoveredByMandatoryRule(true);
                    } else {
                        attribute.setIsCoveredByOptionalRule(true);
                    }

                    if (attribute.getIsCoveredByMandatoryRule() && attribute.getIsCoveredByOptionalRule()
                            && parentChild.containsKey(attribute)) {
                        setApprovedUsageNoneRecursively(parentChild.get(attribute), derivedAttribute, parentChild);
                    }
                }
            }
        }
    }

    private void setApprovedUsageNoneRecursively(List<Attribute> attributes, DataRule derivedAttribute,
            Map<Attribute, List<Attribute>> parentChild) {
        for (Attribute attribute : attributes) {
            attribute.setApprovedUsage(ApprovedUsage.NONE);
            if (!derivedAttribute.getFlaggedColumnNames().contains(attribute.getName()))
                derivedAttribute.getFlaggedColumnNames().add(attribute.getName());
            if (attribute.isCustomerPredictor()) {
                derivedAttribute.addCustomerPredictor(attribute.getName());
            }
            if (parentChild.containsKey(attribute)) {
                setApprovedUsageNoneRecursively(parentChild.get(attribute), derivedAttribute, parentChild);
            }
        }
    }

}
