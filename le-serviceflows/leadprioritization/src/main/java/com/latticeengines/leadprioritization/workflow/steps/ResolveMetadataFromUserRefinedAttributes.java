package com.latticeengines.leadprioritization.workflow.steps;

import java.util.List;

import com.latticeengines.domain.exposed.serviceflows.leadprioritization.steps.ResolveMetadataFromUserRefinedAttributesConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.metadata.Attribute;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.metadata.Tag;
import com.latticeengines.domain.exposed.util.AttributeUtils;
import com.latticeengines.proxy.exposed.metadata.MetadataProxy;
import com.latticeengines.serviceflows.workflow.core.BaseWorkflowStep;

@Component("resolveMetadataFromUserRefinedAttributes")
public class ResolveMetadataFromUserRefinedAttributes extends BaseWorkflowStep<ResolveMetadataFromUserRefinedAttributesConfiguration> {

    private static final Logger log = LoggerFactory.getLogger(ResolveMetadataFromUserRefinedAttributes.class);

    @Autowired
    private MetadataProxy metadataProxy;

    @Override
    public void execute() {
        List<Attribute> userRefinedAttributes = configuration.getUserRefinedAttributes();
        Table eventTable = getObjectFromContext(EVENT_TABLE, Table.class);
        log.info("from user:" + JsonUtils.serialize(userRefinedAttributes));
        log.info("from event table:" + eventTable);
        eventTable = mergeUserRefinedAttributes(userRefinedAttributes, eventTable);
        metadataProxy.createTable(configuration.getCustomerSpace().toString(), eventTable.getName(), eventTable);
        putObjectInContext(EVENT_TABLE, eventTable);
    }

    public Table mergeUserRefinedAttributes(List<Attribute> userRefinedAttributes, Table eventTable){
        for (Attribute userRefinedAttribute : userRefinedAttributes) {
            boolean found = false;
            for (Attribute attr : eventTable.getAttributes()) {
                if (userRefinedAttribute.getName().equals(attr.getName())) {
                    List<String> tags = attr.getTags();
                    if (tags != null && !tags.isEmpty()) {
                        if (tags.get(0).equals(Tag.EXTERNAL.toString())) {
                            attr.setApprovedUsage(userRefinedAttribute.getApprovedUsage());
                        } else {
                            userRefinedAttribute.setRTSAttribute(attr.getRTSAttribute());
                            userRefinedAttribute.setNullable(attr.getNullable());
                            AttributeUtils.copyPropertiesFromAttribute(userRefinedAttribute, attr, false);
                        }
                    } else {
                        log.error(String.format("Ignore unknow attribute %s with unknown tags", userRefinedAttribute.getName()));
                    }
                    found = true;
                    break;
                }
            }
            if (!found) {
                log.error(String.format("Attribute %s not found from event table", userRefinedAttribute.getName()));
            }
        }
        eventTable.setName(eventTable.getName() + "_With_UserRefinedAttributes");
        eventTable.setDisplayName("EventTable_With_UserRefinedAttributes");
        return eventTable;
    }

}
