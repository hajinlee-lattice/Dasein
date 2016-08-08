package com.latticeengines.leadprioritization.workflow.steps;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.modeling.factory.DataFlowFactory;
import com.latticeengines.domain.exposed.transform.TransformationGroup;
import com.latticeengines.proxy.exposed.metadata.MetadataProxy;
import com.latticeengines.serviceflows.workflow.dataflow.RunDataFlow;

@Component("addStandardAttributes")
public class AddStandardAttributes extends RunDataFlow<AddStandardAttributesConfiguration> {

    @Autowired
    private MetadataProxy metadataProxy;

    @Override
    public void onConfigurationInitialized() {
        AddStandardAttributesConfiguration configuration = getConfiguration();
        Table eventTable = JsonUtils.deserialize(executionContext.getString(EVENT_TABLE), Table.class);
        configuration.setTargetTableName(eventTable.getName() + "_with_std_attrib");
        TransformationGroup transformationGroup = configuration.getTransformationGroup();
        configuration.setDataFlowParams(DataFlowFactory.getAddStandardAttributesParameters( //
                eventTable.getName(), transformationGroup, configuration.getRuntimeParams()));
    }

    @Override
    public void onExecutionCompleted() {
        Table eventTable = metadataProxy.getTable(configuration.getCustomerSpace().toString(),
                configuration.getTargetTableName());
        executionContext.putString(EVENT_TABLE, JsonUtils.serialize(eventTable));
        executionContext.putString(TRANSFORMATION_GROUP_NAME, configuration.getTransformationGroup().getName());
    }

}
