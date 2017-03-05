package com.latticeengines.cdl.dataflow.redshiftdb;

import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.RecordReader;

import cascading.flow.FlowProcess;
import cascading.jdbc.RedshiftFactory.CopyOption;
import cascading.jdbc.RedshiftScheme;
import cascading.jdbc.RedshiftTableDesc;
import cascading.scheme.Scheme;
import cascading.tap.Tap;
import cascading.tuple.Fields;

public class RedshiftAvroScheme extends RedshiftScheme {

    private Scheme<Configuration, RecordReader, OutputCollector, Object[], Object[]> sinkScheme;

    public RedshiftAvroScheme(Fields fields, RedshiftTableDesc redshiftTableDesc,
            Scheme<Configuration, RecordReader, OutputCollector, Object[], Object[]> scheme, String jsonPathPrefix) {
        super(fields, redshiftTableDesc);
        Map<CopyOption, String> options = getCopyOptions();
        options.clear();
        options.put(CopyOption.FORMAT, String.format("AVRO \'%s\'", jsonPathPrefix));
        this.sinkScheme = scheme;
        sinkScheme.setSinkFields(getSinkFields());
    }

    @Override
    public void sinkConfInit(FlowProcess<? extends Configuration> flowProcess,
            Tap<Configuration, RecordReader, OutputCollector> tap, Configuration jobConf) {
        sinkScheme.sinkConfInit(flowProcess, tap, jobConf);
    }

}
