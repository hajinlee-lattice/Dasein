package com.latticeengines.cdl.workflow.steps;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import com.latticeengines.domain.exposed.metadata.ApprovedUsage;
import com.latticeengines.domain.exposed.metadata.Attribute;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.serviceflows.cdl.steps.LdcOnlyAttributesConfiguration;
import com.latticeengines.proxy.exposed.metadata.MetadataProxy;
import com.latticeengines.workflow.exposed.build.BaseWorkflowStep;

@Component("ldcOnlyAttributesStep")
@Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
public class LdcOnlyAttributesStep extends BaseWorkflowStep<LdcOnlyAttributesConfiguration> {

    @Autowired
    private MetadataProxy metadataProxy;

    @Override
    public void execute() {
        log.info("Starting LdcOnlyAttributesStep");
        LdcOnlyAttributesConfiguration configuration = getConfiguration();
        Table eventTable = getObjectFromContext(EVENT_TABLE, Table.class);

        for (Attribute attribute : eventTable.getAttributes()) {
            if (attribute.getTags() == null || attribute.isInternalAndInternalTransformField())
                attribute.setApprovedUsage(ApprovedUsage.NONE);
        }

        metadataProxy.updateTable(configuration.getCustomerSpace().toString(), eventTable.getName(), eventTable);
    }

}
