package com.latticeengines.serviceflows.workflow.transformation;

import java.util.Properties;

import javax.inject.Inject;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.modeling.factory.DataFlowFactory;
import com.latticeengines.domain.exposed.serviceflows.core.dataflow.AddStandardAttributesParameters;
import com.latticeengines.domain.exposed.serviceflows.core.steps.AddStandardAttributesConfiguration;
import com.latticeengines.proxy.exposed.metadata.MetadataProxy;
import com.latticeengines.serviceflows.workflow.dataflow.RunDataFlow;

@Component("addStandardAttributesDataFlow")
@Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
public class AddStandardAttributes extends RunDataFlow<AddStandardAttributesConfiguration> {

    @Inject
    private MetadataProxy metadataProxy;

    @Value("${pls.cdl.transform.cascading.partitions}")
    protected int cascadingPartitions;

    @Value("${pls.cdl.transform.tez.am.mem.gb}")
    private int tezAmMemGb; // requested memory for application master

    @Value("${pls.cdl.transform.tez.task.vcores}")
    private int tezVCores;

    @Value("${pls.cdl.transform.tez.task.mem.gb}")
    private int tezMemGb;

    @Override
    public void onConfigurationInitialized() {
        AddStandardAttributesConfiguration configuration = getConfiguration();
        Table eventTable = getObjectFromContext(EVENT_TABLE, Table.class);
        configuration.setTargetTableName(eventTable.getName() + "_std_attrs");
        AddStandardAttributesParameters parameters = DataFlowFactory.getAddStandardAttributesParameters( //
                eventTable.getName(), configuration.getTransforms(), configuration.getRuntimeParams(),
                configuration.getSourceSchemaInterpretation());
        parameters.inputSkippedAttributeList = getListObjectFromContext(INPUT_SKIPPED_ATTRIBUTES_KEY, String.class);
        configuration.setDataFlowParams(parameters);
        configuration.setJobProperties(initJobProperties());

    }

    protected Properties initJobProperties() {
        Properties jobProperties = new Properties();
        int partitions = cascadingPartitions;
        configuration.setPartitions(partitions * 4); // used by TEZ
        jobProperties.put("mapreduce.job.reduces", String.valueOf(partitions * 4));
        jobProperties.put("mapred.reduce.tasks", String.valueOf(partitions * 4));
        jobProperties.put("tez.task.resource.cpu.vcores", String.valueOf(tezVCores * 2));
        jobProperties.put("tez.task.resource.memory.mb", String.valueOf(tezMemGb * 1024 * 2));
        jobProperties.put("tez.am.resource.memory.mb", String.valueOf(tezAmMemGb * 1024 * 2));
        return jobProperties;
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
