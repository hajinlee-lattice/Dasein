package com.latticeengines.dataflow.runtime.cascading.propdata;

import java.io.IOException;
import java.util.List;
import java.util.Map.Entry;
import java.util.Properties;

import org.apache.avro.Schema;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
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
import com.latticeengines.common.exposed.version.VersionManager;

@Component("simpleCascadingExecutor")
public class SimpleCascadingExecutor {
    private static final Log log = LogFactory.getLog(SimpleCascadingExecutor.class);

    private static final String CSV_TO_AVRO_PIPE = "pipe";

    private static final String QUOTE = "\"";

    private static final String CSV_DELIMITER = ",";

    private static final String QUEUENAME_PROP_DATA = "PropData";

    private static final String MAPREDUCE_JOB_QUEUENAME = "mapreduce.job.queuename";

    @Autowired
    private Configuration yarnConfiguration;

    @Autowired
    private VersionManager versionManager;

    public void transformCsvToAvro(String uncompressedFilePath, String avroDirPath, String avroSchemaPath)
            throws IOException {
        Schema schema = new Schema.Parser().parse(HdfsUtils.getHdfsFileContents(yarnConfiguration, avroSchemaPath));
        Properties properties = new Properties();
        for (Entry<String, String> conf : yarnConfiguration) {
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

        try {
            String artifactVersion = versionManager.getCurrentVersion();
            String dataFlowLibDir = StringUtils.isEmpty(artifactVersion) ? "/app/dataflow/lib/" : "/app/"
                    + artifactVersion + "/dataflow/lib/";
            log.info("Using dataflow lib path = " + dataFlowLibDir);
            List<String> files = HdfsUtils.getFilesForDir(yarnConfiguration, dataFlowLibDir);
            for (String file : files) {
                flowDef.addToClassPath(file);
            }
        } catch (Exception e) {
            log.warn("Exception retrieving library jars for this flow.", e);
        }

        Flow<?> wcFlow = flowConnector.connect(flowDef);

        wcFlow.complete();
    }
}
