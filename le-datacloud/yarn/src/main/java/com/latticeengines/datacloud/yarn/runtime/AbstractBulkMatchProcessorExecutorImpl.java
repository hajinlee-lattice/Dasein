package com.latticeengines.datacloud.yarn.runtime;

import java.io.File;
import java.io.IOException;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import javax.annotation.Resource;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.collections4.MapUtils;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.retry.support.RetryTemplate;
import org.springframework.stereotype.Component;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.latticeengines.common.exposed.util.AvroUtils;
import com.latticeengines.common.exposed.util.HdfsUtils;
import com.latticeengines.common.exposed.util.RetryUtils;
import com.latticeengines.datacloud.core.service.DnBCacheService;
import com.latticeengines.datacloud.match.annotation.MatchStep;
import com.latticeengines.datacloud.match.exposed.service.DomainCollectService;
import com.latticeengines.datacloud.match.exposed.service.MatchCommandService;
import com.latticeengines.datacloud.match.exposed.util.MatchUtils;
import com.latticeengines.datacloud.match.metric.MatchResponse;
import com.latticeengines.datacloud.match.service.MatchExecutor;
import com.latticeengines.datacloud.match.service.MatchPlanner;
import com.latticeengines.datacloud.match.service.impl.MatchContext;
import com.latticeengines.datacloud.match.util.EntityMatchUtils;
import com.latticeengines.domain.exposed.datacloud.match.EntityMatchResult;
import com.latticeengines.domain.exposed.datacloud.match.MatchConstants;
import com.latticeengines.domain.exposed.datacloud.match.MatchInput;
import com.latticeengines.domain.exposed.datacloud.match.MatchOutput;
import com.latticeengines.domain.exposed.datacloud.match.MatchRequestSource;
import com.latticeengines.domain.exposed.datacloud.match.MatchStatistics;
import com.latticeengines.domain.exposed.datacloud.match.OperationalMode;
import com.latticeengines.domain.exposed.datacloud.match.OutputRecord;
import com.latticeengines.domain.exposed.monitor.metric.MetricDB;
import com.latticeengines.monitor.exposed.metric.service.MetricService;
import com.latticeengines.proxy.exposed.matchapi.MatchProxy;

@Component("bulkMatchProcessorExecutor")
public abstract class AbstractBulkMatchProcessorExecutorImpl implements BulkMatchProcessorExecutor {

    private static final Logger log = LoggerFactory.getLogger(AbstractBulkMatchProcessorExecutorImpl.class);

    private static final String INTEGER = Integer.class.getSimpleName();
    private static final String LONG = Long.class.getSimpleName();
    private static final String FLOAT = Float.class.getSimpleName();
    private static final String DOUBLE = Double.class.getSimpleName();
    private static final String BOOLEAN = Boolean.class.getSimpleName();
    private static final String STRING = String.class.getSimpleName();
    private static final String MATCHOUTPUT_BUFFER_FILE = "matchoutput.buffer";

    private String[] suppressedWarnings = new String[] { //
            "Cannot find a match in data cloud for the input", //
            "Parsed to a public domain" //
    };

    @Autowired
    private Configuration yarnConfiguration;

    @Resource(name = "bulkMatchPlanner")
    protected MatchPlanner matchPlanner;

    @Resource(name = "bulkMatchExecutor")
    protected MatchExecutor matchExecutor;

    @Autowired
    protected MatchProxy matchProxy;

    @Autowired
    private MetricService metricService;

    @Autowired
    private DomainCollectService domainCollectService;

    @Autowired
    private DnBCacheService dnbCacheService;

    @Autowired
    private DedupeHelper dedupeHelper;

    @Autowired
    private MatchCommandService matchCommandService;

    @Value("${datacloud.match.bulk.snappy.compress}")
    private boolean useSnappy;

    private Random random = new Random(System.currentTimeMillis());

    @Override
    public void finalize(ProcessorContext processorContext) throws Exception {
        finalizeBlock(processorContext);
    }

    protected MatchInput constructMatchInputFromData(ProcessorContext processorContext) {
        // make a deep copy of original input
        MatchInput matchInput = processorContext.getOriginalInput().configurationDeepCopy();

        // replace column selection
        if (processorContext.getColumnSelection() != null) {
            matchInput.setPredefinedSelection(null);
            matchInput.setUnionSelection(null);
            matchInput.setCustomSelection(processorContext.getColumnSelection());
        }

        // overwrite fields in match input
        // TODO: many of these copy overs can be eliminated
        matchInput.setRootOperationUid(processorContext.getRootOperationUid());
        matchInput.setTenant(processorContext.getTenant());
        matchInput.setMatchEngine(MatchContext.MatchEngine.BULK.getName());
        matchInput.setFields(processorContext.getDivider().getFields());
        matchInput.setKeyMap(processorContext.getKeyMap());
        matchInput.setData(processorContext.getDivider().nextGroup());
        matchInput.setDecisionGraph(processorContext.getDecisionGraph());
        matchInput.setExcludePublicDomain(processorContext.getExcludePublicDomain());
        matchInput.setPublicDomainAsNormalDomain(processorContext.getPublicDomainAsNormalDomain());
        matchInput.setDataCloudVersion(processorContext.getDataCloudVersion());

        if (processorContext.isUseProxy()) {
            matchInput.setSkipKeyResolution(true);
        }

        matchInput.setUseDnBCache(processorContext.getJobConfiguration().getMatchInput().isUseDnBCache());
        matchInput.setMatchDebugEnabled(processorContext.getJobConfiguration().getMatchInput().isMatchDebugEnabled());
        matchInput.setUseRemoteDnB(processorContext.isUseRemoteDnB());
        matchInput.setLogDnBBulkResult(processorContext.getJobConfiguration().getMatchInput().isLogDnBBulkResult());
        matchInput.setTimeout(processorContext.getRecordTimeOut());
        matchInput.setUseRemoteDnB(processorContext.isUseRemoteDnB());
        matchInput.setDisableDunsValidation(processorContext.isDisableDunsValidation());
        matchInput.setMetadatas(processorContext.getMetadatas());
        matchInput.setMetadataFields(processorContext.getMetadataFields());
        return matchInput;
    }

    @MatchStep
    protected void processMatchOutput(ProcessorContext processorContext, MatchOutput groupOutput) {
        try {
            writeDataToAvro(processorContext, groupOutput.getResult());
            logError(processorContext, groupOutput);
            writeNewEntityDataToAvro(processorContext, groupOutput.getResult());
        } catch (IOException e) {
            throw new RuntimeException("Failed to write result to avro.", e);
        }

        groupOutput.setResult(new ArrayList<>());

        MatchOutput blockOutput = MatchUtils.mergeOutputs(processorContext.getBlockOutput(), groupOutput);
        processorContext.setBlockOutput(blockOutput);
        log.info("Merge group output into block output.");
    }

    private void logError(ProcessorContext processorContext, MatchOutput groupOutput) throws IOException {
        for (OutputRecord record : groupOutput.getResult()) {
            if (record.getErrorMessages() != null && !record.getErrorMessages().isEmpty()) {
                // TODO: per record error might cause out of memory. change to
                // stream to output file on the fly
                // record.setOutput(null);
                // recordsWithErrors.add(record);
                for (String msg : record.getErrorMessages()) {
                    logMatchErrorMessage(msg);
                }
            }
        }

        writeErrorDataToAvro(processorContext, groupOutput.getResult());
    }

    private void logMatchErrorMessage(String msg) {
        for (String w : suppressedWarnings) {
            if (msg.contains(w)) {
                return;
            }
        }
        log.warn(msg);
    }

    private void writeDataToAvro(ProcessorContext processorContext, List<OutputRecord> outputRecords)
            throws IOException {
        List<GenericRecord> records = new ArrayList<>();
        for (OutputRecord outputRecord : outputRecords) {
            if (outputRecord.getOutput() == null) {
                continue;
            }

            // sequence is very important in all values
            // input attr -> output attr -> dedupe -> debug
            // the sequence is the same as constructing the output schema
            List<Object> allValues = copyAndfilterOverwrittenInput(outputRecord, processorContext);
            allValues.addAll(outputRecord.getOutput());
            MatchInput originalInput = processorContext.getOriginalInput();
            if (MatchRequestSource.MODELING.equals(originalInput.getRequestSource())
                    && originalInput.isPrepareForDedupe()) {
                dedupeHelper.appendDedupeValues(processorContext, allValues, outputRecord);
            }
            if (processorContext.isMatchDebugEnabled()) {
                appendDebugValues(allValues, outputRecord);
            }
            GenericRecordBuilder builder = new GenericRecordBuilder(processorContext.getOutputSchema());
            List<Schema.Field> fields = processorContext.getOutputSchema().getFields();
            buildAvroRecords(allValues, builder, fields);
            records.add(builder.build());
        }
        int randomSplit = random.nextInt(processorContext.getSplits());
        String splitAvro = processorContext.getOutputAvro(randomSplit);
        FileUtils.forceMkdirParent(new File(splitAvro));
        Schema schema = processorContext.getOutputSchema();
        if (new File(splitAvro).exists()) {
            AvroUtils.appendToLocalFile(records, splitAvro, useSnappy);
        } else {
            AvroUtils.writeToLocalFile(schema, records, splitAvro, useSnappy);
        }
        log.info("Write " + records.size() + " generic records to " + splitAvro);
    }

    /*
     * copy the original input and filter out any values that will be overwritten by
     * output
     */
    private List<Object> copyAndfilterOverwrittenInput(OutputRecord outputRecord, ProcessorContext processorContext) {
        List<Object> input = outputRecord.getInput();
        if (CollectionUtils.isNotEmpty(processorContext.getOverriddenInputColumnIdx())) {
            // filter out entity ID values in input
            return IntStream.range(0, input.size()) //
                    .filter(idx -> !processorContext.getOverriddenInputColumnIdx().contains(idx)) //
                    .mapToObj(input::get) //
                    .collect(Collectors.toList());
        }
        return new ArrayList<>(input);
    }

    private void buildAvroRecords(List<Object> allValues, GenericRecordBuilder builder, List<Schema.Field> fields) {
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
    }

    /*
     * Aggregate all newly allocate entities from a list of output records and write
     * to avro
     */
    private void writeNewEntityDataToAvro(ProcessorContext processorContext, List<OutputRecord> outputRecords)
            throws IOException {
        if (processorContext == null || CollectionUtils.isEmpty(outputRecords)) {
            return;
        }
        // check whether we need to generate new entity file or not
        if (!EntityMatchUtils.shouldOutputNewEntities(processorContext.getOriginalInput())) {
            return;
        }

        // generate avro records for newly allocated entities
        // [ entity, entityId ]
        List<GenericRecord> records = new ArrayList<>();
        Schema schema = processorContext.getNewEntitySchema();
        for (OutputRecord outputRecord : outputRecords) {
            if (outputRecord == null || MapUtils.isEmpty(outputRecord.getNewEntityIds())) {
                continue;
            }

            List<List<Object>> values = outputRecord.getNewEntityIds() //
                    .entrySet() //
                    .stream() //
                    // filter out the entities that we don't need to output
                    .filter(entry -> EntityMatchUtils.shouldOutputNewEntity(processorContext.getOriginalInput(),
                            entry.getKey())) //
                    .map(entry -> Arrays.<Object> asList(entry.getKey(), entry.getValue())) //
                    .collect(Collectors.toList());
            records.addAll(values.stream().map(row -> {
                // generate avro records
                GenericRecordBuilder builder = new GenericRecordBuilder(schema);
                List<Schema.Field> fields = schema.getFields();
                buildAvroRecords(row, builder, fields);
                return builder.build();
            }).collect(Collectors.toList()));
        }

        // write to hdfs
        int randomSplit = random.nextInt(processorContext.getSplits());
        String newEntityAvroFile = processorContext.getNewEntityOutputAvro(randomSplit);
        if (!HdfsUtils.fileExists(yarnConfiguration, newEntityAvroFile)) {
            AvroUtils.writeToHdfsFile(yarnConfiguration, schema, newEntityAvroFile, records, useSnappy);
        } else {
            AvroUtils.appendToHdfsFile(yarnConfiguration, newEntityAvroFile, records, useSnappy);
        }
        log.info("Write {} newly allocated entity records to {}", records.size(), newEntityAvroFile);
    }

    private void writeErrorDataToAvro(ProcessorContext processorContext, List<OutputRecord> outputRecords)
            throws IOException {
        if (!EntityMatchUtils.shouldOutputNewEntities(processorContext.getOriginalInput())) {
            return;
        }
        List<GenericRecord> records = new ArrayList<>();
        Schema errorSchema = processorContext.appendErrorSchema(processorContext.getInputSchema());
        for (OutputRecord outputRecord : outputRecords) {
            if (CollectionUtils.isEmpty(outputRecord.getErrorMessages())) {
                continue;
            }

            List<Object> allValues = new ArrayList<>(outputRecord.getInput());
            allValues.add(StringUtils.join(outputRecord.getErrorMessages(), "|"));
            GenericRecordBuilder builder = new GenericRecordBuilder(errorSchema);
            List<Schema.Field> fields = errorSchema.getFields();
            buildAvroRecords(allValues, builder, fields);
            records.add(builder.build());
        }
        if (CollectionUtils.isEmpty(records)) {
            return;
        }
        int randomSplit = random.nextInt(processorContext.getSplits());
        String errorAvroFile = processorContext.getErrorOutputAvro(randomSplit);
        if (!HdfsUtils.fileExists(yarnConfiguration, errorAvroFile)) {
            AvroUtils.writeToHdfsFile(yarnConfiguration, errorSchema, errorAvroFile, records, useSnappy);
        } else {
            AvroUtils.appendToHdfsFile(yarnConfiguration, errorAvroFile, records, useSnappy);
        }
        log.info("Write " + records.size() + " error generic records to " + errorAvroFile);
    }

    private void appendDebugValues(List<Object> allValues, OutputRecord outputRecord) {
        if (CollectionUtils.isNotEmpty(outputRecord.getDebugValues())) {
            allValues.addAll(outputRecord.getDebugValues());
        } else {
            String[] values = new String[MatchConstants.matchDebugFields.size()];
            Arrays.fill(values, "");
            allValues.addAll(Arrays.asList(values));
        }
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
        if (value == null) {
            return null;
        }
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
        if (BOOLEAN.equalsIgnoreCase(javaClass) && !(value instanceof Boolean)) {
            String str = String.valueOf(value);
            if ("1".equals(str) || "yes".equalsIgnoreCase(str) || "Y".equalsIgnoreCase(str)) {
                return true;
            } else if ("0".equals(str) || "no".equalsIgnoreCase(str) || "N".equalsIgnoreCase(str)) {
                return false;
            } else {
                return Boolean.valueOf(str);
            }
        }

        return value;
    }

    @MatchStep
    private void finalizeBlock(ProcessorContext processorContext) throws IOException {
        Long count = uploadOutput(processorContext);
        finalizeMatchOutput(processorContext);
        generateOutputMetric(processorContext.getGroupMatchInput(), processorContext.getBlockOutput());
        if (processorContext.getReturnUnmatched()) {
            if (!processorContext.getExcludePublicDomain()
                    && !processorContext.getBlockSize().equals(count.intValue())) {
                throw new RuntimeException(
                        String.format("Block size [%d] does not equal to the count of the avro [%d].",
                                processorContext.getBlockSize(), count));
            }
        } else {
            // NOTE: This path appears not to get executed.
            // check matched rows
            if (!processorContext.getBlockOutput().getStatistics().getRowsMatched().equals(count.intValue())) {
                throw new RuntimeException(String.format(
                        "RowsMatched in MatchStatistics [%d] does not equal to the count of the avro [%d].",
                        processorContext.getBlockOutput().getStatistics().getRowsMatched(), count));
            }
        }

        Map<EntityMatchResult, Long> matchResultMap = new HashMap<>();
        if (OperationalMode.isEntityMatch(processorContext.getGroupMatchInput().getOperationalMode())) {
            MatchStatistics matchStats = processorContext.getBlockOutput().getStatistics();
            matchResultMap.put(EntityMatchResult.ORPHANED_NO_MATCH,
                    matchStats.getOrphanedNoMatchCount() == null ? 0L : matchStats.getOrphanedNoMatchCount());
            matchResultMap.put(EntityMatchResult.ORPHANED_UNMATCHED_ACCOUNTID,
                    matchStats.getOrphanedUnmatchedAccountIdCount() == null ? 0L
                            : matchStats.getOrphanedUnmatchedAccountIdCount());
            matchResultMap.put(EntityMatchResult.MATCHED_BY_MATCHKEY,
                    matchStats.getMatchedByMatchKeyCount() == null ? 0L : matchStats.getMatchedByMatchKeyCount());
            matchResultMap.put(EntityMatchResult.MATCHED_BY_ACCOUNTID,
                    matchStats.getMatchedByAccountIdCount() == null ? 0L : matchStats.getMatchedByAccountIdCount());
        }

        try {
            matchCommandService.updateBlock(processorContext.getBlockOperationUid()).matchedRows(count.intValue())
                    .matchResults(matchResultMap).commit();
        } catch (Exception e) {
            log.warn("Failed to update block matched rows.", e);
        }
        processorContext.getDataCloudProcessor().setProgress(1f);
        try {
            domainCollectService.setDrainMode();
            domainCollectService.dumpQueue();
        } catch (Exception e) {
            log.error("Failed to dump domains to SQL.", e);
        }
        try {
            dnbCacheService.dumpQueue();
        } catch (Exception e) {
            log.error("Failed to dump remaining DnBCaches to Dynamo.", e);
        }
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
            log.error("Failed to extract output metric.", e);
        }
    }

    @MatchStep
    private long uploadOutput(ProcessorContext processorContext) {
        long count = AvroUtils.countLocalDir("output");
        log.info("There are in total " + count + " records in the avros in output dir.");
        String hdfsDir = processorContext.getHdfsOutputDir();
        RetryTemplate retry = RetryUtils.getRetryTemplate(5);
        try {
            retry.execute(ctx -> {
                log.info("Attempt=" + (ctx.getRetryCount() + 1) + ": uploading match block result to " + hdfsDir);
                try {
                    if (HdfsUtils.fileExists(yarnConfiguration, hdfsDir)) {
                        HdfsUtils.rmdir(yarnConfiguration, hdfsDir);
                    }
                    HdfsUtils.copyFromLocalDirToHdfs(yarnConfiguration, "output", hdfsDir);
                } catch (IOException e) {
                    throw new RuntimeException(" Failed to upload from local to hdfs.");
                }
                return true;
            });
        } finally {
            FileUtils.deleteQuietly(new File("output"));
        }
        return count;
    }

    private void finalizeMatchOutput(ProcessorContext processorContext) throws IOException {
        try {
            Date finishedAt = new Date();
            MatchOutput blockOutput = processorContext.getBlockOutput();
            if (blockOutput == null) {
                throw new IllegalStateException("Block match output object is null.");
            }
            blockOutput.setFinishedAt(finishedAt);
            blockOutput.setReceivedAt(processorContext.getReceivedAt());
            blockOutput.getStatistics().setRowsRequested(processorContext.getBlockSize());
            blockOutput.getStatistics()
                    .setTimeElapsedInMsec(finishedAt.getTime() - processorContext.getReceivedAt().getTime());
            ObjectMapper mapper = new ObjectMapper();
            mapper.writeValue(new File(MATCHOUTPUT_BUFFER_FILE), blockOutput);
            HdfsUtils.copyFromLocalToHdfs(yarnConfiguration, MATCHOUTPUT_BUFFER_FILE, processorContext.getOutputJson());
            FileUtils.deleteQuietly(new File(MATCHOUTPUT_BUFFER_FILE));
        } catch (Exception e) {
            log.error("Failed to save match output json.", e);
        }
    }

}
