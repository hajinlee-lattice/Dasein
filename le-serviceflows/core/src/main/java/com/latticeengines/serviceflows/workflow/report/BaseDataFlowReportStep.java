package com.latticeengines.serviceflows.workflow.report;

import java.util.ArrayList;
import java.util.List;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.joda.time.DateTime;
import org.springframework.beans.factory.annotation.Autowired;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.latticeengines.common.exposed.util.AvroUtils;
import com.latticeengines.domain.exposed.dataflow.flows.CreateReportParameters;
import com.latticeengines.domain.exposed.metadata.Extract;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.proxy.exposed.metadata.MetadataProxy;
import com.latticeengines.serviceflows.workflow.dataflow.DataFlowStepConfiguration;

public abstract class BaseDataFlowReportStep<T extends BaseDataFlowReportStepConfiguration> extends BaseReportStep<T> {

    @Autowired
    private MetadataProxy metadataProxy;

    @Autowired
    private RunCreateReportDataFlow runCreateReportDataFlow;

    public abstract CreateReportParameters getDataFlowParameters();

    @Override
    protected ObjectNode getJson() {
        DataFlowStepConfiguration configuration = new DataFlowStepConfiguration();
        configuration.setCustomerSpace(getConfiguration().getCustomerSpace());
        configuration.setMicroServiceHostPort(getConfiguration().getMicroServiceHostPort());
        configuration.setInternalResourceHostPort(getConfiguration().getInternalResourceHostPort());
        configuration.setPodId(getConfiguration().getPodId());
        configuration.setTargetTableName("Report_" + DateTime.now().getMillis());
        configuration.setBeanName("createReport");
        configuration.setPurgeSources(false);
        CreateReportParameters parameters = getDataFlowParameters();
        configuration.setDataFlowParams(parameters);
        runCreateReportDataFlow.setConfiguration(configuration);
        runCreateReportDataFlow.execute();

        Table table = metadataProxy.getTable(getConfiguration().getCustomerSpace().toString(),
                configuration.getTargetTableName());

        List<String> paths = new ArrayList<>();
        for (Extract extract : table.getExtracts()) {
            paths.add(extract.getPath());
        }
        List<GenericRecord> records = AvroUtils.getDataFromGlob(yarnConfiguration, paths);

        ObjectNode json = new ObjectMapper().createObjectNode();

        if (records.size() != 1) {
            throw new RuntimeException(String.format(
                    "Expected exactly 1 record from report data flow.  Instead found %d", records.size()));
        }

        GenericRecord record = records.get(0);
        Schema schema = record.getSchema();
        // TODO Autodetect schema types
        for (Schema.Field field : schema.getFields()) {
            json.put(field.name(), ((Long) record.get(field.name())).longValue());
        }

        return json;
    }
}
