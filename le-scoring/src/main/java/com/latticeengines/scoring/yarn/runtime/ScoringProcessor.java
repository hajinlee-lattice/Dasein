package com.latticeengines.scoring.yarn.runtime;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.avro.Schema;
import org.apache.avro.Schema.Type;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.file.FileReader;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;
import org.apache.avro.io.DatumWriter;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVPrinter;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.Marker;
import org.slf4j.MarkerFactory;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.beans.BeansException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;

import com.google.common.annotations.VisibleForTesting;
import com.latticeengines.baton.exposed.service.BatonService;
import com.latticeengines.common.exposed.csv.LECSVFormat;
import com.latticeengines.common.exposed.util.AvroUtils;
import com.latticeengines.common.exposed.util.HdfsUtils;
import com.latticeengines.domain.exposed.admin.LatticeFeatureFlag;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.domain.exposed.metadata.Attribute;
import com.latticeengines.domain.exposed.metadata.InterfaceName;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.pls.LeadEnrichmentAttribute;
import com.latticeengines.domain.exposed.pls.ModelType;
import com.latticeengines.domain.exposed.scoring.RTSBulkScoringConfiguration;
import com.latticeengines.domain.exposed.scoring.ScoreResultField;
import com.latticeengines.domain.exposed.scoringapi.BulkRecordScoreRequest;
import com.latticeengines.domain.exposed.scoringapi.Record;
import com.latticeengines.domain.exposed.scoringapi.RecordScoreResponse;
import com.latticeengines.domain.exposed.scoringapi.RecordScoreResponse.ScoreModelTuple;
import com.latticeengines.domain.exposed.util.ExtractUtils;
import com.latticeengines.domain.exposed.util.TableUtils;
import com.latticeengines.proxy.exposed.pls.InternalResourceRestApiProxy;
import com.latticeengines.proxy.exposed.scoringapi.InternalScoringApiProxy;
import com.latticeengines.scoring.orchestration.service.ScoringDaemonService;
import com.latticeengines.yarn.exposed.runtime.SingleContainerYarnProcessor;

public class ScoringProcessor extends SingleContainerYarnProcessor<RTSBulkScoringConfiguration>
        implements ItemProcessor<RTSBulkScoringConfiguration, String>, ApplicationContextAware {

    private static final Logger log = LoggerFactory.getLogger(ScoringProcessor.class);
    private static final Marker fatal = MarkerFactory.getMarker("FATAL");

    public static final String RECORD_RULE = "manual";
    public static final String RECORD_SOURCE = "file";
    public static final String DEFAULT_ID_TYPE = "internal";
    public static final boolean DEFAULT_ENRICHMENT = false;

    @Value("${scoring.processor.threadpool.size}")
    private int threadpoolSize = 5;

    @Value("${scoring.processor.threadpool.timeoutmin}")
    private long threadPoolTimeoutMin = 1440;

    @Value("${scoring.processor.bulkrecord.size}")
    private int bulkRecordSize = 100;

    @SuppressWarnings("unused")
    private ApplicationContext applicationContext;

    @Autowired
    private Configuration yarnConfiguration;

    @Autowired
    private InternalScoringApiProxy internalScoringApiProxy;

    @Autowired
    private BatonService batonService;

    private InternalResourceRestApiProxy internalResourceRestApiProxy;

    private boolean useInternalId = false;

    private boolean isEnableDebug = false;

    private Map<String, Long> idToInternalIdMap = new HashMap<>();

    private RTSBulkScoringConfiguration rtsBulkScoringConfig;

    public ScoringProcessor() {
        super();
    }

    public ScoringProcessor(RTSBulkScoringConfiguration rtsBulkScoringConfig) {
        this.rtsBulkScoringConfig = rtsBulkScoringConfig;
    }

    @Override
    public void setApplicationContext(ApplicationContext applicationContext) throws BeansException {
        this.applicationContext = applicationContext;
    }

    public void setConfiguration(Configuration configuration) {
        this.yarnConfiguration = configuration;
    }

    @Override
    public String process(RTSBulkScoringConfiguration rtsBulkScoringConfig) throws Exception {
        this.rtsBulkScoringConfig = rtsBulkScoringConfig;
        log.info("Inside the rts bulk scoring processor.");
        internalResourceRestApiProxy = new InternalResourceRestApiProxy(
                rtsBulkScoringConfig.getInternalResourceHostPort());
        String path = getExtractPath(rtsBulkScoringConfig);
        log.info(String.format("The extract path is: %s", path));

        Map<String, String> fieldNameMapping = getFieldNameMapping(rtsBulkScoringConfig);

        Map<String, Schema.Type> leadEnrichmentAttributeMap = null;
        Map<String, String> leadEnrichmentAttributeDisplayNameMap = null;
        Map<String, Boolean> leadEnrichmentInternalAttributeFlagMap = null;
        isEnableDebug = rtsBulkScoringConfig.isEnableDebug();
        boolean enrichmentEnabledForInternalAttributes = batonService.isEnabled(rtsBulkScoringConfig.getCustomerSpace(),
                LatticeFeatureFlag.ENABLE_INTERNAL_ENRICHMENT_ATTRIBUTES);

        if (rtsBulkScoringConfig.isEnableLeadEnrichment()) {
            leadEnrichmentAttributeMap = new HashMap<>();
            leadEnrichmentAttributeDisplayNameMap = new HashMap<>();
            leadEnrichmentInternalAttributeFlagMap = new HashMap<>();

            getLeadEnrichmentAttributes(rtsBulkScoringConfig.getCustomerSpace(), leadEnrichmentAttributeMap,
                    leadEnrichmentAttributeDisplayNameMap, leadEnrichmentInternalAttributeFlagMap,
                    enrichmentEnabledForInternalAttributes);
        }

        if (path == null) {
            throw new IllegalArgumentException("The path is null.");
        }
        List<String> modelGuids = rtsBulkScoringConfig.getModelGuids();
        if (modelGuids.isEmpty()) {
            throw new LedpException(LedpCode.LEDP_20033);
        }

        String fileName = UUID.randomUUID() + ScoringDaemonService.AVRO_FILE_SUFFIX;
        long recordCount = checkForInternalIdAndCountRecords(path);
        Schema schema = createOutputSchema(leadEnrichmentAttributeMap, leadEnrichmentAttributeDisplayNameMap);
        log.info(String.format("schema is %s", schema));

        try (CSVPrinter csvFilePrinter = initErrorCSVFilePrinter(rtsBulkScoringConfig.getImportErrorPath())) {
            try (FileReader<GenericRecord> reader = instantiateReaderForBulkScoreRequest(path);
                    DataFileWriter<GenericRecord> dataFileWriter = createDataFileWriter(schema, fileName)) {
                GenericRecordBuilder builder = new GenericRecordBuilder(schema);
                execute(rtsBulkScoringConfig, reader, dataFileWriter, builder, leadEnrichmentAttributeMap,
                        csvFilePrinter, recordCount, fieldNameMapping, enrichmentEnabledForInternalAttributes);
            }
        }
        copyScoreOutputToHdfs(fileName, rtsBulkScoringConfig.getTargetResultDir());

        return "Inside the rts bulk scoring processor.";
    }

    public CSVPrinter initErrorCSVFilePrinter(String importErrorPath) throws IOException {
        CSVFormat format = LECSVFormat.format.withHeader("LineNumber", "Id", "ErrorMessage");
        if (StringUtils.isNotEmpty(importErrorPath) && HdfsUtils.fileExists(yarnConfiguration, importErrorPath)) {
            HdfsUtils.copyHdfsToLocal(yarnConfiguration, importErrorPath, ".");
            FileUtils.deleteQuietly(new File("." + ScoringDaemonService.IMPORT_ERROR_FILE_NAME + ".crc"));
            format = format.withSkipHeaderRecord();
        }
        return new CSVPrinter(new FileWriter(ScoringDaemonService.IMPORT_ERROR_FILE_NAME, true), format); //

    }

    @VisibleForTesting
    void copyScoreOutputToHdfs(String fileName, String targetDir) throws IOException {
        String scorePath = String.format(targetDir + "/%s", fileName);
        log.info(String.format("The output score path is %s", scorePath));
        HdfsUtils.copyLocalToHdfs(yarnConfiguration, fileName, scorePath);

        HdfsUtils.copyLocalToHdfs(yarnConfiguration, ScoringDaemonService.IMPORT_ERROR_FILE_NAME,
                targetDir + "/error.csv");
    }

    @VisibleForTesting
    DataFileWriter<GenericRecord> createDataFileWriter(Schema schema, String fileName) throws IOException {
        DatumWriter<GenericRecord> userDatumWriter = new GenericDatumWriter<>();
        DataFileWriter<GenericRecord> dataFileWriter = new DataFileWriter<>(userDatumWriter);
        dataFileWriter.create(schema, new File(fileName));
        return dataFileWriter;
    }

    private List<RecordScoreResponse> bulkScore(BulkRecordScoreRequest scoreRequest, String customerSpace,
            Boolean enrichmentEnabledForInternalAttributes) {
        long startTime = System.currentTimeMillis();
        log.info(String.format("Sending internal bulk score request with %d records for tenant %s",
                scoreRequest.getRecords().size(), customerSpace));
        List<RecordScoreResponse> recordScoreResponse = null;
        if (isEnableDebug) {
            log.info("Score in the debug mode");
            recordScoreResponse = internalScoringApiProxy.scorePercentileAndProbabilityRecords(scoreRequest,
                    customerSpace, enrichmentEnabledForInternalAttributes,
                    rtsBulkScoringConfig.isEnableLeadEnrichment());
        } else {
            recordScoreResponse = internalScoringApiProxy.scorePercentileRecords(scoreRequest, customerSpace,
                    enrichmentEnabledForInternalAttributes, rtsBulkScoringConfig.isEnableLeadEnrichment());
        }
        long endTime = System.currentTimeMillis();
        long oneBatchTime = endTime - startTime;
        log.info(String.format("Bulk score request with %d records took %d sec", scoreRequest.getRecords().size(),
                (oneBatchTime / 1000)));

        return recordScoreResponse;
    }

    private String getExtractPath(RTSBulkScoringConfiguration rtsBulkScoringConfig) {
        Table metadataTable = rtsBulkScoringConfig.getMetadataTable();
        if (metadataTable == null) {
            throw new LedpException(LedpCode.LEDP_20028, new String[] { rtsBulkScoringConfig.toString() });
        }
        String path = ExtractUtils.getSingleExtractPath(yarnConfiguration, metadataTable);
        return path;
    }

    private Map<String, String> getFieldNameMapping(RTSBulkScoringConfiguration rtsBulkScoringConfig) {
        Table metadataTable = rtsBulkScoringConfig.getMetadataTable();
        if (metadataTable == null) {
            throw new LedpException(LedpCode.LEDP_20028, new String[] { rtsBulkScoringConfig.toString() });
        }

        List<Attribute> attributes = metadataTable.getAttributes();
        Map<String, String> fieldNameMapping = new HashMap<>();

        for (Attribute attr : attributes) {
            String displayName = attr.getDisplayName();
            String internalName = attr.getName();
            fieldNameMapping.put(displayName, internalName);
        }
        return fieldNameMapping;
    }

    private void getLeadEnrichmentAttributes(CustomerSpace customerSpace, Map<String, Schema.Type> attributeMap,
            Map<String, String> attributeDisplayNameMap, Map<String, Boolean> internalAttributeFlagMap,
            boolean enrichmentEnabledForInternalAttributes) {
        List<LeadEnrichmentAttribute> leadEnrichmentAttributeList = internalResourceRestApiProxy
                .getLeadEnrichmentAttributes(customerSpace, null, null, Boolean.TRUE,
                        enrichmentEnabledForInternalAttributes);
        for (LeadEnrichmentAttribute attribute : leadEnrichmentAttributeList) {
            String fieldType = attribute.getFieldType();
            Schema.Type avroType = null;
            try {
                avroType = AvroUtils.convertSqlTypeToAvro(fieldType);
            } catch (IllegalArgumentException e) {
                throw new LedpException(LedpCode.LEDP_20040, e);
            } catch (IllegalAccessException e) {
                throw new LedpException(LedpCode.LEDP_20041, e);
            }
            attributeMap.put(attribute.getFieldName(), avroType);
            attributeDisplayNameMap.put(attribute.getFieldName(), attribute.getDisplayName());
            internalAttributeFlagMap.put(attribute.getFieldName(), attribute.getIsInternal());
        }
        log.info(String.format("The attributeMap is: %s", attributeMap));
    }

    @VisibleForTesting
    FileReader<GenericRecord> instantiateReaderForBulkScoreRequest(String path) throws IOException {
        String fileName = getAvroFileName(path);
        return AvroUtils.getAvroFileReader(yarnConfiguration, new Path(fileName));
    }

    @VisibleForTesting
    BulkRecordScoreRequest getBulkScoreRequest(FileReader<GenericRecord> reader,
            RTSBulkScoringConfiguration rtsBulkScoringConfig) throws IOException {
        if (!reader.hasNext()) {
            return null;
        }

        BulkRecordScoreRequest scoreRequest = new BulkRecordScoreRequest();
        scoreRequest.setSource(RECORD_SOURCE);
        List<Record> records = new ArrayList<Record>();
        scoreRequest.setRecords(records);

        int recordCount = 1;
        while (reader.hasNext() && recordCount <= bulkRecordSize) {
            GenericRecord avroRecord = reader.next();
            Schema schema = avroRecord.getSchema();
            List<Schema.Field> fields = schema.getFields();

            Record record = new Record();
            record.setPerformEnrichment(rtsBulkScoringConfig.isEnableLeadEnrichment());
            record.setIdType(DEFAULT_ID_TYPE);

            String idStr = null;
            if (!useInternalId) {
                idStr = avroRecord.get(InterfaceName.Id.toString()).toString();
                idToInternalIdMap.put(idStr,
                        Long.valueOf(avroRecord.get(InterfaceName.InternalId.toString()).toString()));
            } else {
                idStr = avroRecord.get(InterfaceName.InternalId.toString()).toString();
            }

            Map<String, Object> attributeValues = new HashMap<>();
            if (rtsBulkScoringConfig.isEnableLeadEnrichment()) { // Score a file
                for (Schema.Field field : fields) {
                    String fieldName = field.name();
                    Object fieldValue = avroRecord.get(fieldName) == null //
                            ? null : avroRecord.get(fieldName).toString();
                    attributeValues.put(fieldName, fieldValue);
                }
            } else { // Score training data
                Table metadataTable = rtsBulkScoringConfig.getMetadataTable();
                if (metadataTable == null) {
                    throw new LedpException(LedpCode.LEDP_20028, new String[] { rtsBulkScoringConfig.toString() });
                }
                List<Attribute> attributes = metadataTable.getAttributes();

                Set<String> internalPlusMustHaveAttributeNames = new HashSet<>();
                internalPlusMustHaveAttributeNames.add(InterfaceName.LatticeAccountId.toString());
                internalPlusMustHaveAttributeNames.add(InterfaceName.InternalId.toString());
                for (Attribute attribute : attributes) {
                    if (attribute.isInternalPredictor()) {
                        internalPlusMustHaveAttributeNames.add(attribute.getName());
                    }
                }
                if (log.isDebugEnabled()) {
                    log.debug("internalPlusMustHaveAttributeNames is " + internalPlusMustHaveAttributeNames);
                }

                for (Schema.Field field : fields) {
                    String fieldName = field.name();
                    if (internalPlusMustHaveAttributeNames.contains(fieldName)) {
                        Object fieldValue = avroRecord.get(fieldName) == null //
                                ? null : avroRecord.get(fieldName).toString();
                        attributeValues.put(fieldName, fieldValue);
                    }
                }
            }

            if (log.isDebugEnabled()) {
                log.debug("attributeValues is " + attributeValues);
            }
            Map<String, Map<String, Object>> modelAttributeValuesMap = new HashMap<>();
            for (String modelguid : rtsBulkScoringConfig.getModelGuids()) {
                modelAttributeValuesMap.put(modelguid, attributeValues);
            }

            record.setModelAttributeValuesMap(modelAttributeValuesMap);
            record.setRecordId(idStr);
            record.setRule(RECORD_RULE);

            records.add(record);
            recordCount++;
        }

        return scoreRequest;
    }

    @VisibleForTesting
    long checkForInternalIdAndCountRecords(String path) throws IOException {
        long count = 0;
        try (FileReader<GenericRecord> prereader = instantiateReaderForBulkScoreRequest(path);) {
            GenericRecord avroRecord = prereader.next();
            count++;
            Object idObj = avroRecord.get(InterfaceName.Id.toString());
            if (idObj == null) {
                idObj = avroRecord.get(InterfaceName.InternalId.toString());
                useInternalId = true;
            }
            if (idObj == null) {
                throw new LedpException(LedpCode.LEDP_20034);
            }
            while (prereader.hasNext()) {
                prereader.next();
                count++;
            }
        }
        log.info(String.format("There are %d total records in the input avro", count));
        return count;
    }

    @VisibleForTesting
    Schema createOutputSchema(Map<String, Schema.Type> leadEnrichmentAttributeMap,
            Map<String, String> leadEnrichmentAttributeDisplayNameMap) {

        Table outputTable = new Table();
        outputTable.setName("scoreOutput");
        Attribute idAttr = new Attribute();
        if (!useInternalId) {
            idAttr.setName(InterfaceName.Id.toString());
            idAttr.setDisplayName(InterfaceName.Id.toString());
            idAttr.setSourceLogicalDataType("");
            idAttr.setPhysicalDataType(Type.STRING.name());
        } else {
            idAttr.setName(InterfaceName.InternalId.toString());
            idAttr.setDisplayName(InterfaceName.InternalId.toString());
            idAttr.setSourceLogicalDataType("");
            idAttr.setPhysicalDataType(Type.LONG.name());
        }
        Attribute modelIdAttr = new Attribute();
        modelIdAttr.setName(ScoringDaemonService.MODEL_ID);
        modelIdAttr.setDisplayName(ScoringDaemonService.MODEL_ID);
        modelIdAttr.setSourceLogicalDataType("");
        modelIdAttr.setPhysicalDataType(Type.STRING.name());
        Attribute scoreAttr = new Attribute();
        scoreAttr.setName(ScoreResultField.Percentile.displayName);
        scoreAttr.setDisplayName(ScoreResultField.Percentile.displayName);
        scoreAttr.setSourceLogicalDataType("");
        scoreAttr.setPhysicalDataType(Type.INT.name());
        outputTable.addAttribute(idAttr);
        outputTable.addAttribute(modelIdAttr);
        outputTable.addAttribute(scoreAttr);
        if (modelIsPythonType()) {
            Attribute bucketAttr = new Attribute();
            bucketAttr.setName(ScoreResultField.Rating.displayName);
            bucketAttr.setDisplayName(ScoreResultField.Rating.displayName);
            bucketAttr.setSourceLogicalDataType("");
            bucketAttr.setPhysicalDataType(Type.STRING.name());
            outputTable.addAttribute(bucketAttr);
        }

        if (isEnableDebug) {
            Attribute rawScoreAttr = new Attribute();
            rawScoreAttr.setName(ScoreResultField.RawScore.displayName);
            rawScoreAttr.setDisplayName(ScoreResultField.RawScore.displayName);
            rawScoreAttr.setSourceLogicalDataType("");
            rawScoreAttr.setPhysicalDataType(Type.DOUBLE.name());
            outputTable.addAttribute(rawScoreAttr);
        }

        if (leadEnrichmentAttributeMap != null) {
            Iterator<Entry<String, Type>> it = leadEnrichmentAttributeMap.entrySet().iterator();
            while (it.hasNext()) {
                Map.Entry<String, Schema.Type> pair = (Entry<String, Type>) it.next();
                String leadEnrichmentAttrName = pair.getKey();
                Type avroType = pair.getValue();
                Attribute attr = new Attribute();
                attr.setName(leadEnrichmentAttrName);
                attr.setDisplayName(leadEnrichmentAttributeDisplayNameMap.get(leadEnrichmentAttrName));
                attr.setSourceLogicalDataType("");
                attr.setPhysicalDataType(avroType.name());
                outputTable.addAttribute(attr);
            }
        }
        return TableUtils.createSchema(outputTable.getName(), outputTable);
    }

    private boolean modelIsPythonType() {
        return ModelType.isPythonTypeModel(rtsBulkScoringConfig.getModelType());
    }

    @VisibleForTesting
    void appendScoreResponseToAvro(List<RecordScoreResponse> recordScoreResponseList,
            DataFileWriter<GenericRecord> dataFileWriter, GenericRecordBuilder builder,
            Map<String, Schema.Type> leadEnrichmentAttributeMap, CSVPrinter csvFilePrinter) throws IOException {

        boolean leadEnrichmentEnabled = false;
        if (leadEnrichmentAttributeMap != null) {
            leadEnrichmentEnabled = true;
        } else {
            log.info("Lead enrichment is not enabled for this tenant.");
        }

        for (RecordScoreResponse scoreResponse : recordScoreResponseList) {
            List<ScoreModelTuple> scoreModelTupleList = scoreResponse.getScores();
            String id = scoreResponse.getId();
            if (StringUtils.isBlank(id)) {
                throw new LedpException(LedpCode.LEDP_20035);
            }
            for (ScoreModelTuple tuple : scoreModelTupleList) {
                if (log.isDebugEnabled()) {
                    log.debug("tuple is: " + tuple);
                }
                if (!useInternalId) {
                    builder.set(InterfaceName.Id.toString(), id);
                } else {
                    builder.set(InterfaceName.InternalId.toString(), Long.valueOf(id));
                }
                String modelId = tuple.getModelId();
                if (StringUtils.isBlank(modelId)) {
                    throw new LedpException(LedpCode.LEDP_20036);
                }
                Integer score = tuple.getScore();
                if (modelIsPythonType()) {
                    validateScore(score);
                    String bucketName = tuple.getBucket() == null ? "" : tuple.getBucket();
                    builder.set(ScoreResultField.Rating.displayName, bucketName);
                }
                if (isEnableDebug) {
                    Double rawScore = tuple.getProbability();
                    if (rawScore != null && (rawScore > 1 || rawScore < 0)) {
                        throw new LedpException(LedpCode.LEDP_20038);
                    }
                    builder.set(ScoreResultField.RawScore.displayName, rawScore);
                }
                builder.set(ScoringDaemonService.MODEL_ID, modelId);
                builder.set(ScoreResultField.Percentile.displayName, score);

                writeToErrorFile(csvFilePrinter, id, tuple.getErrorDescription());

                Map<String, Object> enrichmentAttributeValues = scoreResponse.getEnrichmentAttributeValues();
                if (leadEnrichmentEnabled) {
                    Iterator<Entry<String, Schema.Type>> it = leadEnrichmentAttributeMap.entrySet().iterator();
                    while (it.hasNext()) {
                        Entry<String, Schema.Type> entry = it.next();
                        Object value = null;
                        if (enrichmentAttributeValues == null
                                || !enrichmentAttributeValues.containsKey(entry.getKey())) {
                            log.warn(String.format(
                                    "The enrichment attribute values in the score response is null or does match this entry. Will set enrichment attribute %s to NULL value",
                                    entry.getKey()));
                        } else {
                            value = enrichmentAttributeValues.get(entry.getKey());
                            Schema.Type avroType = entry.getValue();
                            value = AvroUtils.checkTypeAndConvert(entry.getKey(), value, avroType);
                        }
                        builder.set(entry.getKey(), value);
                    }
                }
                GenericData.Record record = builder.build();
                dataFileWriter.append(record);
            }
        }
    }

    private void validateScore(Integer score) {
        if (score != null && (score > 99 || score < 5)) {
            throw new LedpException(LedpCode.LEDP_20037);
        }
    }

    private void writeToErrorFile(CSVPrinter csvFilePrinter, String id, String errorMessage) throws IOException {
        if (StringUtils.isNotEmpty(errorMessage)) {
            if (!useInternalId) {
                csvFilePrinter.printRecord(idToInternalIdMap.get(id), id, errorMessage);
            } else {
                csvFilePrinter.printRecord(id, "", errorMessage);
            }
        }
    }

    private String getAvroFileName(String path) throws IOException {
        List<String> files = HdfsUtils.getFilesForDir(yarnConfiguration, path, ".*.avro");
        String fileName = files.size() > 0 ? files.get(0) : null;
        if (fileName == null) {
            throw new LedpException(LedpCode.LEDP_12003, new String[] { path });
        }
        return fileName;
    }

    private void execute(RTSBulkScoringConfiguration rtsBulkScoringConfig, FileReader<GenericRecord> reader,
            DataFileWriter<GenericRecord> dataFileWriter, GenericRecordBuilder builder,
            Map<String, Type> leadEnrichmentAttributeMap, CSVPrinter csvFilePrinter, long recordCount,
            Map<String, String> fieldNameMapping, boolean enrichmentEnabledForInternalAttributes) throws Exception {

        List<Future<Integer>> futures = new ArrayList<>();
        ExecutorService scoreExecutorService = Executors.newFixedThreadPool(threadpoolSize);
        final AtomicLong counter = new AtomicLong(0);

        for (int i = 0; i < threadpoolSize; i++) {
            futures.add(scoreExecutorService.submit(new BulkScoreApiCallable(rtsBulkScoringConfig, reader,
                    dataFileWriter, builder, leadEnrichmentAttributeMap, csvFilePrinter, counter, recordCount,
                    fieldNameMapping, enrichmentEnabledForInternalAttributes)));
        }

        for (Future<Integer> future : futures) {
            try {
                future.get(threadPoolTimeoutMin, TimeUnit.MINUTES);
            } catch (Exception e) {
                log.error(fatal, e.getMessage(), e);
                throw e;
            }
        }

        scoreExecutorService.shutdown();
        try {
            scoreExecutorService.awaitTermination(threadPoolTimeoutMin, TimeUnit.MINUTES);
        } catch (InterruptedException e) {
        }
    }

    class BulkScoreApiCallable implements Callable<Integer> {

        private RTSBulkScoringConfiguration rtsBulkScoringConfig;
        private FileReader<GenericRecord> reader;
        private DataFileWriter<GenericRecord> dataFileWriter;
        private GenericRecordBuilder builder;
        private Map<String, Schema.Type> leadEnrichmentAttributeMap;
        private CSVPrinter csvFilePrinter;
        private AtomicLong counter;
        private long recordCount;
        private Map<String, String> fieldNameMapping;
        private boolean enrichmentEnabledForInternalAttributes;

        BulkScoreApiCallable(RTSBulkScoringConfiguration rtsBulkScoringConfig, FileReader<GenericRecord> reader,
                DataFileWriter<GenericRecord> dataFileWriter, GenericRecordBuilder builder,
                Map<String, Type> leadEnrichmentAttributeMap, CSVPrinter csvFilePrinter, AtomicLong counter,
                long recordCount, Map<String, String> fieldNameMapping,
                boolean enrichmentEnabledForInternalAttributes) {

            super();
            this.rtsBulkScoringConfig = rtsBulkScoringConfig;
            this.reader = reader;
            this.dataFileWriter = dataFileWriter;
            this.builder = builder;
            this.leadEnrichmentAttributeMap = leadEnrichmentAttributeMap;
            this.csvFilePrinter = csvFilePrinter;
            this.counter = counter;
            this.recordCount = recordCount;
            this.fieldNameMapping = fieldNameMapping;
            this.enrichmentEnabledForInternalAttributes = enrichmentEnabledForInternalAttributes;
        }

        @Override
        public Integer call() throws Exception {
            while (true) {
                BulkRecordScoreRequest scoreRequest = null;
                synchronized (reader) {
                    scoreRequest = ScoringProcessor.this.getBulkScoreRequest(reader, rtsBulkScoringConfig);
                    if (log.isDebugEnabled()) {
                        log.debug("scoreRequest is " + scoreRequest);
                    }
                    if (scoreRequest == null) {
                        break;
                    }
                    // from matching perspective all the rows in file based
                    // scoring are same from metadata perspective therefore we
                    // can set homogeneous flag to true to improve matching
                    // performance
                    scoreRequest.setHomogeneous(true);
                }
                List<RecordScoreResponse> scoreResponseList = ScoringProcessor.this.bulkScore(scoreRequest,
                        rtsBulkScoringConfig.getCustomerSpace().toString(), enrichmentEnabledForInternalAttributes);

                log.info(String.format("Scored %d out of %d total records",
                        counter.addAndGet(scoreRequest.getRecords().size()), recordCount));
                synchronized (dataFileWriter) {
                    ScoringProcessor.this.appendScoreResponseToAvro(scoreResponseList, dataFileWriter, builder,
                            leadEnrichmentAttributeMap, csvFilePrinter);
                }
            }
            return 0;
        }
    }

}
