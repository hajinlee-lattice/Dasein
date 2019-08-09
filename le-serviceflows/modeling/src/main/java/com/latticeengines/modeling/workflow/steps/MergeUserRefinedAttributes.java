package com.latticeengines.modeling.workflow.steps;

import java.util.List;
import java.util.Map;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.collections4.MapUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.metadata.ApprovedUsage;
import com.latticeengines.domain.exposed.metadata.Attribute;
import com.latticeengines.domain.exposed.metadata.ColumnMetadata;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.serviceflows.leadprioritization.steps.MergeUserRefinedAttributesConfiguration;
import com.latticeengines.proxy.exposed.metadata.MetadataProxy;
import com.latticeengines.workflow.exposed.build.BaseWorkflowStep;

@Component("mergeUserRefinedAttributes")
@Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
public class MergeUserRefinedAttributes extends BaseWorkflowStep<MergeUserRefinedAttributesConfiguration> {

    private static final Logger log = LoggerFactory.getLogger(MergeUserRefinedAttributes.class);

    @Autowired
    private MetadataProxy metadataProxy;

    @Override
    public void execute() {
        Map<String, ColumnMetadata> userRefinedAttributes = configuration.getUserRefinedAttributes();
        if (MapUtils.isEmpty(userRefinedAttributes)) {
            log.info("No User defined attributes found, skipping Merging UserRefinedAttributes");
            return;
        }
        Table eventTable = getObjectFromContext(EVENT_TABLE, Table.class);
        log.info("Attributes from user:" + JsonUtils.serialize(userRefinedAttributes));
        log.info("Attributes from event table:" + JsonUtils.serialize(eventTable.getAttributes()));
        eventTable = mergeUserRefinedAttributes(userRefinedAttributes, eventTable);
        metadataProxy.updateTable(configuration.getCustomerSpace().toString(), eventTable.getName(), eventTable);
        putObjectInContext(EVENT_TABLE, eventTable);
    }

    public Table mergeUserRefinedAttributes(Map<String, ColumnMetadata> userRefinedAttributes, Table eventTable) {
        for (Attribute eventTableAttribute : eventTable.getAttributes()) {
            if (userRefinedAttributes.containsKey(eventTableAttribute.getName())) {
                List<ApprovedUsage> approvedUsage = userRefinedAttributes.get(eventTableAttribute.getName())
                        .getApprovedUsageList();
                eventTableAttribute.setApprovedUsage( //
                        CollectionUtils.isEmpty(approvedUsage) || approvedUsage.get(0) == null //
                                ? ApprovedUsage.NONE //
                                : approvedUsage.get(0));
            } else {
                log.warn(String.format(
                        "Attribute %s not found in attributes from previous Iteration's event table.",
                        eventTableAttribute.getName()));
                List<String> approvedUsages = eventTableAttribute.getApprovedUsage();
                if (CollectionUtils.isEmpty(approvedUsages) || approvedUsages.get(0) == null) {
                    eventTableAttribute.setApprovedUsage(ApprovedUsage.NONE);
                }
            }
        }
        eventTable.setName(eventTable.getName() + "_With_UserRefinedAttributes");
        eventTable.setDisplayName("EventTable_With_UserRefinedAttributes");
        return eventTable;
    }
}
