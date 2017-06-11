package com.latticeengines.leadprioritization.workflow.steps;

import com.latticeengines.domain.exposed.serviceflows.leadprioritization.steps.AddStandardAttributesConfiguration;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.modeling.factory.DataFlowFactory;
import com.latticeengines.proxy.exposed.metadata.MetadataProxy;
import com.latticeengines.serviceflows.workflow.dataflow.RunDataFlow;

@Component("addStandardAttributes")
public class AddStandardAttributes extends RunDataFlow<AddStandardAttributesConfiguration> {

    @Autowired
    private MetadataProxy metadataProxy;

    @Override
    public void onConfigurationInitialized() {
        AddStandardAttributesConfiguration configuration = getConfiguration();
        Table eventTable = getObjectFromContext(EVENT_TABLE, Table.class);
        configuration.setTargetTableName(eventTable.getName() + "_with_std_attrib");
        configuration.setDataFlowParams(DataFlowFactory.getAddStandardAttributesParameters( //
                eventTable.getName(), configuration.getTransforms(), configuration.getRuntimeParams(), configuration.getSourceSchemaInterpretation()));
    }

    @Override
    public void onExecutionCompleted() {
        Table eventTable = metadataProxy.getTable(configuration.getCustomerSpace().toString(),
                configuration.getTargetTableName());
        putObjectInContext(EVENT_TABLE, eventTable);
        putStringValueInContext(TRANSFORMATION_GROUP_NAME, configuration.getTransformationGroup().getName());
    }

    @Override
    public void skipStep() {
        super.skipStep();
        putStringValueInContext(TRANSFORMATION_GROUP_NAME, configuration.getTransformationGroup().getName());
    }

}
