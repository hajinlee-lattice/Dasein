package com.latticeengines.datacloud.yarn.runtime;

import java.io.File;
import java.io.IOException;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;
import org.apache.commons.io.FileUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Component;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.latticeengines.common.exposed.util.AvroUtils;
import com.latticeengines.common.exposed.util.HdfsUtils;
import com.latticeengines.datacloud.match.annotation.MatchStep;
import com.latticeengines.datacloud.match.exposed.util.MatchUtils;
import com.latticeengines.datacloud.match.metric.MatchResponse;
import com.latticeengines.datacloud.match.service.MatchExecutor;
import com.latticeengines.datacloud.match.service.MatchPlanner;
import com.latticeengines.datacloud.match.service.impl.MatchContext;
import com.latticeengines.domain.exposed.datacloud.match.MatchInput;
import com.latticeengines.domain.exposed.datacloud.match.MatchOutput;
import com.latticeengines.domain.exposed.datacloud.match.OutputRecord;
import com.latticeengines.domain.exposed.monitor.metric.MetricDB;
import com.latticeengines.monitor.exposed.metric.service.MetricService;
import com.latticeengines.proxy.exposed.matchapi.MatchProxy;

@Component("bulkMatchProcessorExecutor")
public abstract class AbstractBulkMatchProcessorExecutorImpl implements BulkMatchProcessorExecutor {

    private static final Log log = LogFactory.getLog(AbstractBulkMatchProcessorExecutorImpl.class);

    private static final String INTEGER = Integer.class.getSimpleName();
    private static final String LONG = Long.class.getSimpleName();
    private static final String FLOAT = Float.class.getSimpleName();
    private static final String DOUBLE = Double.class.getSimpleName();
    private static final String STRING = String.class.getSimpleName();
    private static final String MATCHOUTPUT_BUFFER_FILE = "matchoutput.buffer";

    @Autowired
    private Configuration yarnConfiguration;

    @Autowired
    @Qualifier("bulkMatchPlanner")
    protected MatchPlanner matchPlanner;

    @Autowired
    @Qualifier("bulkMatchExecutor")
    protected MatchExecutor matchExecutor;

    @Autowired
    protected MatchProxy matchProxy;

    @Autowired
    private MetricService metricService;

    @Override
    public void finalize(ProcessorContext processorContext) throws Exception {
        finalizeBlock(processorContext);
    }

    protected MatchInput constructMatchInputFromData(ProcessorContext processorContext) {
        MatchInput matchInput = new MatchInput();
        matchInput.setRootOperationUid(processorContext.getRootOperationUid());
        matchInput.setTenant(processorContext.getTenant());
        if (processorContext.getPredefinedSelection() == null) {
            matchInput.setCustomSelection(processorContext.getCustomizedSelection());
        } else {
            matchInput.setPredefinedSelection(processorContext.getPredefinedSelection());
            matchInput.setPredefinedVersion("1.0");
        }
        matchInput.setMatchEngine(MatchContext.MatchEngine.BULK.getName());
        matchInput.setFields(processorContext.getDivider().getFields());
        matchInput.setKeyMap(processorContext.getKeyMap());
        matchInput.setData(processorContext.getDivider().nextGroup());
        matchInput.setDecisionGraph(processorContext.getDecisionGraph());
        matchInput.setExcludeUnmatchedWithPublicDomain(processorContext.getExcludeUnmatchedWithPublicDomain());
        matchInput.setPublicDomainAsNormalDomain(processorContext.getPublicDomainAsNormalDomain());
        matchInput.setDataCloudVersion(processorContext.getDataCloudVersion());

        if (processorContext.isUseProxy()) {
            matchInput.setSkipKeyResolution(true);
        }

        matchInput.setUseDnBCache(processorContext.getJobConfiguration().getUseDnBCache());

        return matchInput;
    }

    @MatchStep
    protected void processMatchOutput(ProcessorContext processorContext, MatchOutput groupOutput) {
        try {
            writeDataToAvro(processorContext, groupOutput.getResult());
        } catch (IOException e) {
            throw new RuntimeException("Failed to write result to avro.", e);
        }

        List<OutputRecord> recordsWithErrors = new ArrayList<>();
        for (OutputRecord record : groupOutput.getResult()) {
            if (record.getErrorMessages() != null && !record.getErrorMessages().isEmpty()) {
                // TODO: per record error might cause out of memory. change to
                // stream to output file on the fly
                // record.setOutput(null);
                // recordsWithErrors.add(record);
                for (String msg : record.getErrorMessages()) {
                    log.warn(msg);
                }
            }
        }
        groupOutput.setResult(recordsWithErrors);

        MatchOutput blockOutput = MatchUtils.mergeOutputs(processorContext.getBlockOutput(), groupOutput);
        processorContext.setBlockOutput(blockOutput);
        log.info("Merge group output into block output.");
    }

    private void writeDataToAvro(ProcessorContext processorContext, List<OutputRecord> outputRecords)
            throws IOException {
        List<GenericRecord> records = new ArrayList<>();
        for (OutputRecord outputRecord : outputRecords) {
            if (outputRecord.getOutput() == null || outputRecord.getOutput().isEmpty()) {
                continue;
            }

            List<Object> allValues = new ArrayList<>(outputRecord.getInput());
            allValues.addAll(outputRecord.getOutput());

            GenericRecordBuilder builder = new GenericRecordBuilder(processorContext.getOutputSchema());
            List<Schema.Field> fields = processorContext.getOutputSchema().getFields();
            for (int i = 0; i < fields.size(); i++) {
                Object value = allValues.get(i);
                if (value instanceof Date) {
                    value = ((Date) value).getTime();
                }
                if (value instanceof Timestamp) {
                    value = ((Timestamp) value).getTime();
                }
                Schema.Type type = AvroUtils.getType(fields.get(i));
                value = convertToClaimedType(type, value, fields.get(i).name());
                builder.set(fields.get(i), value);
            }
            records.add(builder.build());
        }
        if (!HdfsUtils.fileExists(yarnConfiguration, processorContext.getOutputAvro())) {
            AvroUtils.writeToHdfsFile(yarnConfiguration, processorContext.getOutputSchema(),
                    processorContext.getOutputAvro(), records);
        } else {
            AvroUtils.appendToHdfsFile(yarnConfiguration, processorContext.getOutputAvro(), records);
        }
        log.info("Write " + records.size() + " generic records to " + processorContext.getOutputAvro());
    }

    private Object convertToClaimedType(Schema.Type avroType, Object value, String columnName) {
        if (value != null) {
            String javaClass = AvroUtils.getJavaType(avroType).getSimpleName();
            try {
                value = convertByJavaClass(value, javaClass);
            } catch (Exception e) {
                log.warn("Failed to parse value " + value + " of attribute " + columnName + " to " + javaClass
                        + ". Using null instead", e);
                return null;
            }
        }
        return value;
    }

    private static Object convertByJavaClass(Object value, String javaClass) {
        if (STRING.equalsIgnoreCase(javaClass) && !(value instanceof String)) {
            return String.valueOf(value);
        }
        if (INTEGER.equalsIgnoreCase(javaClass) && !(value instanceof Integer)) {
            return Integer.valueOf(String.valueOf(value));
        }
        if (LONG.equalsIgnoreCase(javaClass) && !(value instanceof Long)) {
            return Long.valueOf(String.valueOf(value));
        }
        if (FLOAT.equalsIgnoreCase(javaClass) && !(value instanceof Float)) {
            return Float.valueOf(String.valueOf(value));
        }
        if (DOUBLE.equalsIgnoreCase(javaClass) && !(value instanceof Double)) {
            return Double.valueOf(String.valueOf(value));
        }
        return value;
    }

    @MatchStep
    private void finalizeBlock(ProcessorContext processorContext) throws IOException {
        finalizeMatchOutput(processorContext);
        generateOutputMetric(processorContext.getMatchInput(), processorContext.getBlockOutput());
        Long count = AvroUtils.count(yarnConfiguration, processorContext.getOutputAvro());
        log.info("There are in total " + count + " records in the avro " + processorContext.getOutputAvro());
        if (processorContext.getReturnUnmatched()) {
            if (!processorContext.getExcludeUnmatchedWithPublicDomain()
                    && !processorContext.getBlockSize().equals(count.intValue())) {
                throw new RuntimeException(String.format(
                        "Block size [%d] does not equal to the count of the avro [%d].",
                        processorContext.getBlockSize(), count));
            }
        } else {
            // check matched rows
            if (!processorContext.getBlockOutput().getStatistics().getRowsMatched().equals(count.intValue())) {
                throw new RuntimeException(String.format(
                        "RowsMatched in MatchStatistics [%d] does not equal to the count of the avro [%d].",
                        processorContext.getBlockOutput().getStatistics().getRowsMatched(), count));
            }
        }
        processorContext.getDataCloudProcessor().setProgress(1f);
    }

    @MatchStep
    private void generateOutputMetric(MatchInput input, MatchOutput output) {
        try {
            MatchContext context = new MatchContext();
            context.setInput(input);
            context.setOutput(output);
            context.setMatchEngine(MatchContext.MatchEngine.BULK);
            MatchResponse response = new MatchResponse(context);
            metricService.write(MetricDB.LDC_Match, response);
        } catch (Exception e) {
            log.warn("Failed to extract output metric.");
        }
    }

    private void finalizeMatchOutput(ProcessorContext processorContext) throws IOException {
        Date finishedAt = new Date();
        MatchOutput blockOutput = processorContext.getBlockOutput();
        blockOutput.setFinishedAt(finishedAt);
        blockOutput.setReceivedAt(processorContext.getReceivedAt());
        blockOutput.getStatistics().setRowsRequested(processorContext.getBlockSize());
        blockOutput.getStatistics().setTimeElapsedInMsec(
                finishedAt.getTime() - processorContext.getReceivedAt().getTime());
        try {
            ObjectMapper mapper = new ObjectMapper();
            mapper.writeValue(new File(MATCHOUTPUT_BUFFER_FILE), blockOutput);
            HdfsUtils.copyFromLocalToHdfs(yarnConfiguration, MATCHOUTPUT_BUFFER_FILE, processorContext.getOutputJson());
            FileUtils.deleteQuietly(new File(MATCHOUTPUT_BUFFER_FILE));
        } catch (Exception e) {
            log.error("Failed to save match output json.", e);
        }
    }

}
