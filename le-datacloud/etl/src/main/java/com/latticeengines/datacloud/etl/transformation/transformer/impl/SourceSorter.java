package com.latticeengines.datacloud.etl.transformation.transformer.impl;

import static com.latticeengines.datacloud.etl.transformation.transformer.impl.SourceSorter.TRANSFORMER_NAME;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.retry.annotation.Backoff;
import org.springframework.retry.annotation.Retryable;
import org.springframework.stereotype.Component;

import com.latticeengines.common.exposed.util.AvroUtils;
import com.latticeengines.common.exposed.util.HdfsUtils;
import com.latticeengines.datacloud.core.source.Source;
import com.latticeengines.datacloud.dataflow.transformation.Sort;
import com.latticeengines.domain.exposed.camille.Path;
import com.latticeengines.domain.exposed.datacloud.dataflow.SorterParameters;
import com.latticeengines.domain.exposed.datacloud.transformation.configuration.impl.SorterConfig;
import com.latticeengines.domain.exposed.metadata.Table;

@Component(TRANSFORMER_NAME)
public class SourceSorter extends AbstractDataflowTransformer<SorterConfig, SorterParameters> {

    @SuppressWarnings("unused")
    private static final Log log = LogFactory.getLog(SourceSorter.class);

    public static final String TRANSFORMER_NAME = "sourceSorter";
    private static final String SORTED_PARTITION = "_DC_Sorted_Partition_";

    private static final int BUFFER_SIZE = 5000;

    private String wd;
    private String out;
    private Schema targetSchema;

    @Autowired
    private Configuration yarnConfiguration;

    @Override
    protected String getDataFlowBeanName() {
        return Sort.BEAN_NAME;
    }

    @Override
    public String getName() {
        return TRANSFORMER_NAME;
    }

    @Override
    protected Class<SorterParameters> getDataFlowParametersClass() {
        return SorterParameters.class;
    }

    @Override
    protected Class<? extends SorterConfig> getConfigurationClass() {
        return SorterConfig.class;
    }

    @Override
    protected void updateParameters(SorterParameters parameters, Source[] baseTemplates, Source targetTemplate,
                                    SorterConfig config) {
        String field = config.getSortingField();
        if (StringUtils.isBlank(field)) {
            throw new IllegalArgumentException("Must specify a sorting field, which is a unique, not-null field with complete ordering.");
        }
        parameters.setSortingField(field);
        parameters.setPartitions(config.getPartitions());
        parameters.setPartitionField(SORTED_PARTITION);
    }

    @Override
    protected boolean validateConfig(SorterConfig config, List<String> sourceNames) {
        return true;
    }

    @Override
    protected boolean needBaseAvsc() {
        return true;
    }

    @Override
    protected Schema getTargetSchema(Table result, SorterParameters parameters, List<Schema> baseAvscSchemas) {
        if (baseAvscSchemas != null) {
            Schema schema = baseAvscSchemas.get(0);
            if (schema != null) {
                log.info("Found schema from base sources' avsc.");
                targetSchema = schema;
                return targetSchema;
            }
        }
        log.info("Did not find schema from base sources' avsc, trying to extract from result avros.");
        String extractPath = result.getExtracts().get(0).getPath();
        String glob;
        if (extractPath.endsWith(".avro")) {
            glob = extractPath;
        } else if (extractPath.endsWith(File.pathSeparator)) {
            glob = extractPath + "*.avro";
        } else {
            glob = extractPath + File.separator + "*.avro";
        }
        Schema parsed = AvroUtils.getSchemaFromGlob(yarnConfiguration, glob);
        targetSchema = stripTempField(parsed);
        return targetSchema;
    }

    private Schema stripTempField(Schema schema) {
        return AvroUtils.removeFields(schema, SORTED_PARTITION);
    }

    protected void postDataFlowProcessing(String workflowDir, SorterParameters paramters, SorterConfig configuration) {
        try {
            moveAvrosToOutput(workflowDir);
            splitAvros();
            cleanupOutputDir();
        } catch (Exception e) {
            throw new RuntimeException("Failed to process avro files result from cascading flow.",  e);
        }
    }

    private void moveAvrosToOutput(String workflowDir) throws IOException {
        out = new Path(workflowDir).append("output").toString();
        wd = new Path(workflowDir).toString();
        String avroGlob = wd + (wd.endsWith("/") ? "*.avro" : "/*.avro");
        List<String> avroFiles = HdfsUtils.getFilesByGlob(yarnConfiguration, avroGlob);
        log.info(avroFiles.size() + " files found at glob " + avroGlob);
        cleanupOutputDir();
        for (String avroFile : avroFiles) {
            String newPath = avroFile.replace(wd, out);
            HdfsUtils.moveFile(yarnConfiguration, avroFile, newPath);
            log.info("Moved avro file " + avroFile.replace(wd, "") + " to " + newPath.replace(wd, ""));
        }
    }

    private void cleanupOutputDir() throws IOException {
        if (!HdfsUtils.fileExists(yarnConfiguration, out)) {
            HdfsUtils.mkdir(yarnConfiguration, out);
        }
    }

    private void splitAvros() throws IOException {
        String avroGlob = out + (out.endsWith("/") ? "*.avro" : "/*.avro");
        List<String> avroFiles = HdfsUtils.getFilesByGlob(yarnConfiguration, avroGlob);
        ExecutorService executors = Executors.newFixedThreadPool(4);
        Map<String, Future<Boolean>> futures = new HashMap<>();
        for (String avroFile : avroFiles) {
            Future<Boolean> future = executors.submit(new AvroSplitCallable(avroFile));
            futures.put(avroFile, future);
        }
        long startTime = System.currentTimeMillis();
        while (System.currentTimeMillis() - startTime < TimeUnit.HOURS.toMillis(10) && !futures.isEmpty()) {
            Set<String> toDelete = consumeFutures(futures);
            toDelete.forEach(futures::remove);
            if (!futures.isEmpty()) {
                try {
                    Thread.sleep(5000L);
                } catch (Exception e) {
                    // do nothing
                }
            }
        }
    }

    private Set<String> consumeFutures(Map<String, Future<Boolean>> futures) {
        Set<String> toDelete = new HashSet<>();

        for (Map.Entry<String, Future<Boolean>> entry: futures.entrySet()) {
            boolean success = false;
            try {
                success = entry.getValue().get(1, TimeUnit.SECONDS);
            } catch (InterruptedException|TimeoutException e) {
                // ignore
            } catch (ExecutionException e) {
                throw new RuntimeException("The thread processing " + entry.getKey() + " throws an exception.", e);
            }
            if (success) {
                toDelete.add(entry.getKey());
                log.info("Processing of avro file " + entry.getKey() + " is finished.");
            }
        }

        return toDelete;
    }

    private class AvroSplitCallable implements Callable<Boolean> {

        private final String avroFile;
        private int attempt = 0;

        AvroSplitCallable(String avroFile) {
            this.avroFile = avroFile;
        }

        @Override
        public Boolean call() {
            split();
            return true;
        }

        @Retryable(backoff = @Backoff(delay = 1000L, multiplier = 2.0))
        private void split() {
            log.info(String.format("(Attempt = %d) Trying to split file %s", attempt++, avroFile));
            Set<String> createdFiles = new HashSet<>();
            Iterator<GenericRecord> iterator = AvroUtils.iterator(yarnConfiguration, avroFile);
            List<GenericRecord> buffer = new ArrayList<>();
            int bufferedPartition = -1;
            while (iterator.hasNext()) {
                GenericRecord record = iterator.next();
                int partition = (int) record.get(SORTED_PARTITION);
                String outputFile = outputFile(partition);
                cleanupOutputFile(outputFile, createdFiles);

                if (partition != bufferedPartition || buffer.size() >= BUFFER_SIZE) {
                    // should dump when switching partition or buffer is full
                    dumpBuffer(buffer, bufferedPartition);
                    buffer.clear();
                    bufferedPartition = partition;
                }

                buffer.add(stripTempField(record));
            }
            dumpBuffer(buffer, bufferedPartition);
        }

        private void cleanupOutputFile(String outputFile, Set<String> createdFiles) {
            if (!createdFiles.contains(outputFile)) {
                try {
                    if (HdfsUtils.fileExists(yarnConfiguration, outputFile)) {
                        HdfsUtils.rmdir(yarnConfiguration, outputFile);
                    }
                } catch (IOException e) {
                    throw new RuntimeException("Failed to clean up output file " + outputFile);
                }
                createdFiles.add(outputFile);
            }
        }

        private GenericRecord stripTempField(GenericRecord record) {
            GenericRecordBuilder builder = new GenericRecordBuilder(targetSchema);
            for (Schema.Field field: targetSchema.getFields()) {
                builder.set(field, record.get(field.name()));
            }
            return builder.build();
        }

        private void dumpBuffer(List<GenericRecord> buffer, int partition) {
            if (buffer.isEmpty()) {
                return;
            }
            String outputFile = outputFile(partition);
            try {
                if (!HdfsUtils.fileExists(yarnConfiguration, outputFile)) {
                    AvroUtils.writeToHdfsFile(yarnConfiguration, targetSchema, outputFile, buffer, true);
                } else {
                    AvroUtils.appendToHdfsFile(yarnConfiguration, outputFile, buffer, true);
                }
                String fileName = outputFile.substring(outputFile.lastIndexOf("/") + 1);
                log.info("Dumped " + buffer.size() + " records to the output file " + fileName + " in " + outputFile.replace(fileName, ""));
            } catch (Exception e) {
                throw new RuntimeException("Failed to dump " + buffer.size() + " records to the output file " + outputFile, e);
            }
        }

        private String outputFile(int partition) {
            return wd + (wd.endsWith("/") ? "" : "/") + String.format("part-s-%05d.avro", partition);
        }
    }

}
