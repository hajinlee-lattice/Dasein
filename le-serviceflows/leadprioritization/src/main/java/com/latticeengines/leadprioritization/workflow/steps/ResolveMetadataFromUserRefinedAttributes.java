package com.latticeengines.leadprioritization.workflow.steps;

import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.metadata.Attribute;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.util.AttributeUtils;
import com.latticeengines.proxy.exposed.metadata.MetadataProxy;
import com.latticeengines.serviceflows.workflow.core.BaseWorkflowStep;

@Component("resolveMetadataFromUserRefinedAttributes")
public class ResolveMetadataFromUserRefinedAttributes extends BaseWorkflowStep<ResolveMetadataFromUserRefinedAttributesConfiguration>{

    private static final Log log = LogFactory.getLog(ResolveMetadataFromUserRefinedAttributes.class);

    @Autowired
    private MetadataProxy metadataProxy;

    @Override
    public void execute() {
        List<Attribute> userRefinedAttributes = configuration.getUserRefinedAttributes();
        Table eventTable = JsonUtils.deserialize(getStringValueFromContext(EVENT_TABLE), Table.class);
        log.info("from user:" + userRefinedAttributes);
        log.info("from event table:" + eventTable);

        for(Attribute userRefinedAttribute : userRefinedAttributes){
            boolean found = false;
            for(Attribute attr : eventTable.getAttributes()){
                if(userRefinedAttribute.getName().equals(attr.getName())){
                    AttributeUtils.copyPropertiesFromAttribute(userRefinedAttribute, attr, false);
                    found = true;
                    break;
                }
            }
            if(!found){
                log.error(userRefinedAttribute.getName() + " not found from event table");
            }
        }
        eventTable.setName(eventTable.getName() + "_With_UserRefinedAttributes");
        eventTable.setDisplayName("EventTable");
        metadataProxy.createTable(configuration.getCustomerSpace().toString(), eventTable.getName(), eventTable);
        putObjectInContext(EVENT_TABLE, JsonUtils.serialize(eventTable));
    }

}
