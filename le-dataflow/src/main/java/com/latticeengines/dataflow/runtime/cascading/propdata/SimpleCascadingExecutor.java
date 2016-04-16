package com.latticeengines.dataflow.runtime.cascading.propdata;

import java.io.IOException;
import java.util.Iterator;
import java.util.Map.Entry;
import java.util.Properties;

import org.apache.avro.Schema;
import org.apache.hadoop.conf.Configuration;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import cascading.avro.AvroScheme;
import cascading.flow.Flow;
import cascading.flow.FlowDef;
import cascading.flow.hadoop.HadoopFlowConnector;
import cascading.pipe.Pipe;
import cascading.scheme.hadoop.TextDelimited;
import cascading.scheme.util.DelimitedParser;
import cascading.tap.SinkMode;
import cascading.tap.Tap;
import cascading.tap.hadoop.Hfs;

import com.latticeengines.common.exposed.util.HdfsUtils;

@Component("simpleCascadingExecutor")
public class SimpleCascadingExecutor {
    private static final String CSV_TO_AVRO_PIPE = "pipe";

    private static final String QUOTE = "\"";

    private static final String CSV_DELIMITER = ",";

    private static final String QUEUENAME_PROP_DATA = "PropData";

    private static final String MAPREDUCE_JOB_QUEUENAME = "mapreduce.job.queuename";

    @Autowired
    private Configuration yarnConfiguration;

    public void transformCsvToAvro(String uncompressedFilePath, String avroDirPath, String avroSchemaPath)
            throws IOException {
        Schema schema = new Schema.Parser().parse(HdfsUtils.getHdfsFileContents(yarnConfiguration, avroSchemaPath));
        Properties properties = new Properties();
        Iterator<Entry<String, String>> confItr = yarnConfiguration.iterator();
        while (confItr.hasNext()) {
            Entry<String, String> conf = confItr.next();
            properties.put(conf.getKey(), conf.getValue());
        }

        // will configure this to run on tez instead of mapred in next txn
        properties.put(MAPREDUCE_JOB_QUEUENAME, QUEUENAME_PROP_DATA);

        HadoopFlowConnector flowConnector = new HadoopFlowConnector(properties);

        Tap<?, ?, ?> csvTap = new Hfs(new TextDelimited(true, new DelimitedParser(CSV_DELIMITER, QUOTE, null, false, true)),
                uncompressedFilePath);
        Tap<?, ?, ?> avroTap = new Hfs(new AvroScheme(schema), avroDirPath, SinkMode.REPLACE);

        Pipe csvToAvroPipe = new Pipe(CSV_TO_AVRO_PIPE);

        FlowDef flowDef = FlowDef.flowDef().setName(CSV_TO_AVRO_PIPE).addSource(csvToAvroPipe, csvTap)
                .addTailSink(csvToAvroPipe, avroTap);

        Flow<?> wcFlow = flowConnector.connect(flowDef);

        wcFlow.complete();
    }
}
