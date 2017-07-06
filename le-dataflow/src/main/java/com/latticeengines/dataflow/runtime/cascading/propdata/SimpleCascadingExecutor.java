package com.latticeengines.dataflow.runtime.cascading.propdata;

import java.io.IOException;
import java.util.List;
import java.util.Map.Entry;
import java.util.Properties;

import org.apache.avro.Schema;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;

import com.latticeengines.common.exposed.util.HdfsUtils;
import com.latticeengines.common.exposed.version.VersionManager;
import com.latticeengines.scheduler.exposed.LedpQueueAssigner;

import cascading.avro.AvroScheme;
import cascading.flow.Flow;
import cascading.flow.FlowDef;
import cascading.flow.hadoop.HadoopFlowConnector;
import cascading.pipe.Pipe;
import cascading.scheme.hadoop.TextDelimited;
import cascading.scheme.util.DelimitedParser;
import cascading.scheme.util.FieldTypeResolver;
import cascading.tap.SinkMode;
import cascading.tap.Tap;
import cascading.tap.hadoop.Hfs;
import cascading.tuple.Fields;

public class SimpleCascadingExecutor {
    private static final Log log = LogFactory.getLog(SimpleCascadingExecutor.class);

    private static final String CSV_TO_AVRO_PIPE = "pipe";

    private static final String CSV_DELIMITER = ",";

    private static final String MAPREDUCE_JOB_QUEUENAME = "mapreduce.job.queuename";

//    private static final String TEZ_JOB_QUEUENAME = "tez.queue.name";

    private final Configuration yarnConfiguration;

    @Value("${dataplatform.queue.scheme:legacy}")
    private String yarnQueueScheme;

    public SimpleCascadingExecutor(Configuration yarnConfiguration) {
        this.yarnConfiguration = yarnConfiguration;
    }

    @Autowired
    private VersionManager versionManager;

    @Value("${dataflow.hdfs.stack:}")
    private String stackName;

    public void transformCsvToAvro(CsvToAvroFieldMapping fieldMapping, String uncompressedFilePath, String avroDirPath,
            String delimiter, String qualifier, String charset, boolean treatEqualQuoteSpecial)
            throws IOException {
        delimiter = delimiter == null ? CSV_DELIMITER : delimiter;
        log.info(String.format("Delimiter: %s, Qualifier: %s", delimiter, qualifier));

        Schema schema = fieldMapping.getAvroSchema();
        Properties properties = new Properties();
        for (Entry<String, String> conf : yarnConfiguration) {
            properties.put(conf.getKey(), conf.getValue());
        }

        String translatedQueue = LedpQueueAssigner
                .overwriteQueueAssignment(LedpQueueAssigner.getPropDataQueueNameForSubmission(), yarnQueueScheme);
        properties.put(MAPREDUCE_JOB_QUEUENAME, translatedQueue);
        /*
        properties.put(TEZ_JOB_QUEUENAME, translatedQueue);
        properties = FlowRuntimeProps.flowRuntimeProps().setGatherPartitions(1)
                .buildProperties(properties);
        Hadoop2TezFlowConnector flowConnector = new Hadoop2TezFlowConnector(properties);
        */

        HadoopFlowConnector flowConnector = new HadoopFlowConnector(properties);
        AvroScheme avroScheme = new AvroScheme(schema);
        FieldTypeResolver fieldTypeResolver = new CustomFieldTypeResolver(fieldMapping);
        DelimitedParser delimitedParser = (treatEqualQuoteSpecial && qualifier != null)
                ? new CustomDelimitedParserSpecialEqualQuote(fieldMapping, delimiter, qualifier, false, true,
                        fieldTypeResolver)
                : new CustomDelimitedParser(fieldMapping, delimiter, qualifier, false, true,
                        fieldTypeResolver);
        TextDelimited textDelimited = charset == null ? new TextDelimited(true, delimitedParser)
                : new TextDelimited(Fields.ALL, null, true, true, charset, delimitedParser);

        Tap<?, ?, ?> csvTap = new Hfs(textDelimited, uncompressedFilePath);
        Tap<?, ?, ?> avroTap = new Hfs(avroScheme, avroDirPath, SinkMode.REPLACE);

        Pipe csvToAvroPipe = new Pipe(CSV_TO_AVRO_PIPE);

        FlowDef flowDef = FlowDef.flowDef().setName(CSV_TO_AVRO_PIPE).addSource(csvToAvroPipe, csvTap)
                .addTailSink(csvToAvroPipe, avroTap);

        try {
            String artifactVersion = versionManager.getCurrentVersionInStack(stackName);
            String dataFlowLibDir = StringUtils.isEmpty(artifactVersion) ? "/app/dataflow/lib/"
                    : "/app/" + artifactVersion + "/dataflow/lib/";
            log.info("Using dataflow lib path = " + dataFlowLibDir);
            List<String> files = HdfsUtils.getFilesForDir(yarnConfiguration, dataFlowLibDir);
            for (String file : files) {
                flowDef.addToClassPath(file);
            }
        } catch (Exception e) {
            log.warn("Exception retrieving library jars for this flow.");
        }

        Flow<?> wcFlow = flowConnector.connect(flowDef);

        wcFlow.complete();
    }
}
