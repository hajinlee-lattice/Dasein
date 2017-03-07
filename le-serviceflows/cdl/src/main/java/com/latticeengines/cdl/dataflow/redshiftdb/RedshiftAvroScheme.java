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
            Scheme<Configuration, RecordReader, OutputCollector, Object[], Object[]> scheme, Map<CopyOption, String> options) {
        super(fields, redshiftTableDesc);
        getCopyOptions().clear();
        getCopyOptions().putAll(options);
        this.sinkScheme = scheme;
        sinkScheme.setSinkFields(getSinkFields());
    }

    @Override
    public void sinkConfInit(FlowProcess<? extends Configuration> flowProcess,
            Tap<Configuration, RecordReader, OutputCollector> tap, Configuration jobConf) {
        sinkScheme.sinkConfInit(flowProcess, tap, jobConf);
    }

}
