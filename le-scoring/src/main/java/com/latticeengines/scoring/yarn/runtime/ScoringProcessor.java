package com.latticeengines.scoring.yarn.runtime;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.apache.avro.Schema;
import org.apache.avro.Schema.Type;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.file.FileReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;
import org.apache.avro.io.DatumWriter;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.beans.BeansException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;

import com.google.common.annotations.VisibleForTesting;
import com.latticeengines.common.exposed.util.AvroUtils;
import com.latticeengines.common.exposed.util.HdfsUtils;
import com.latticeengines.dataplatform.exposed.yarn.runtime.SingleContainerYarnProcessor;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.domain.exposed.metadata.Attribute;
import com.latticeengines.domain.exposed.metadata.InterfaceName;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.pls.LeadEnrichmentAttribute;
import com.latticeengines.domain.exposed.scoring.RTSBulkScoringConfiguration;
import com.latticeengines.domain.exposed.scoringapi.BulkRecordScoreRequest;
import com.latticeengines.domain.exposed.scoringapi.Record;
import com.latticeengines.domain.exposed.scoringapi.RecordScoreResponse;
import com.latticeengines.domain.exposed.scoringapi.RecordScoreResponse.ScoreModelTuple;
import com.latticeengines.domain.exposed.util.ExtractUtils;
import com.latticeengines.domain.exposed.util.TableUtils;
import com.latticeengines.proxy.exposed.pls.InternalResourceRestApiProxy;
import com.latticeengines.proxy.exposed.scoringapi.InternalScoringApiProxy;
import com.latticeengines.scoring.orchestration.service.ScoringDaemonService;

public class ScoringProcessor extends SingleContainerYarnProcessor<RTSBulkScoringConfiguration> implements
        ItemProcessor<RTSBulkScoringConfiguration, String>, ApplicationContextAware {

    private static final Log log = LogFactory.getLog(ScoringProcessor.class);

    public static final String RECORD_RULE = "manual";
    public static final String RECORD_SOURCE = "file";
    public static final String DEFAULT_ID_TYPE = "internal";
    public static final boolean DEFAULT_ENRICHMENT = false;

    @Value("${scoring.processor.threadpool.size}")
    private int threadpoolSize = 5;

    @Value("${scoring.processor.threadpool.timeoutmin}")
    private long threadPoolTimeoutMin = 1440;

    private ApplicationContext applicationContext;

    @Autowired
    private Configuration yarnConfiguration;

    @Autowired
    private InternalScoringApiProxy internalScoringApiProxy;

    private InternalResourceRestApiProxy internalResourceRestApiProxy;

    private boolean useInternalId = false;

    @Override
    public void setApplicationContext(ApplicationContext applicationContext) throws BeansException {
        this.applicationContext = applicationContext;
    }

    public void setConfiguration(Configuration configuration) {
        this.yarnConfiguration = configuration;
    }

    @Override
    public String process(RTSBulkScoringConfiguration rtsBulkScoringConfig) throws Exception {
        log.info("In side the rts bulk scoring processor.");
        internalResourceRestApiProxy = new InternalResourceRestApiProxy(
                rtsBulkScoringConfig.getInternalResourceHostPort());
        String path = getExtractPath(rtsBulkScoringConfig);
        Map<String, Schema.Type> leadEnrichmentAttributeMap = null;
        Map<String, String> leadEnrichmentAttributeDisplayNameMap = null;

        if (rtsBulkScoringConfig.isEnableLeadEnrichment()) {
            leadEnrichmentAttributeMap = new HashMap<>();
            leadEnrichmentAttributeDisplayNameMap = new HashMap<>();

            getLeadEnrichmentAttributes(rtsBulkScoringConfig.getCustomerSpace(), leadEnrichmentAttributeMap,
                    leadEnrichmentAttributeDisplayNameMap);
        }

        if (path == null) {
            throw new IllegalArgumentException("The path is null.");
        }
        List<String> modelGuids = rtsBulkScoringConfig.getModelGuids();
        if (modelGuids.isEmpty()) {
            throw new LedpException(LedpCode.LEDP_20033);
        }

        String fileName = UUID.randomUUID() + ScoringDaemonService.AVRO_FILE_SUFFIX;
        checkForInternalId(path);
        Schema schema = createOutputSchema(leadEnrichmentAttributeMap, leadEnrichmentAttributeDisplayNameMap);

        try (FileReader<GenericRecord> reader = instantiateReaderForBulkScoreRequest(path);
                DataFileWriter<GenericRecord> dataFileWriter = createDataFileWriter(schema, fileName)) {
            GenericRecordBuilder builder = new GenericRecordBuilder(schema);
            execute(rtsBulkScoringConfig, reader, dataFileWriter, builder, leadEnrichmentAttributeMap,
                    leadEnrichmentAttributeDisplayNameMap);
        }

        copyScoreOutputToHdfs(fileName, rtsBulkScoringConfig.getTargetResultDir());

        return "In side the rts bulk scoring processor.";
    }

    public String processBak(RTSBulkScoringConfiguration rtsBulkScoringConfig) throws Exception {
        log.info("In side the rts bulk scoring processor.");
        internalResourceRestApiProxy = new InternalResourceRestApiProxy(
                rtsBulkScoringConfig.getInternalResourceHostPort());
        String path = getExtractPath(rtsBulkScoringConfig);
        Map<String, Schema.Type> leadEnrichmentAttributeMap = null;
        Map<String, String> leadEnrichmentAttributeDisplayNameMap = null;
        if (rtsBulkScoringConfig.isEnableLeadEnrichment()) {
            leadEnrichmentAttributeMap = new HashMap<>();
            leadEnrichmentAttributeDisplayNameMap = new HashMap<>();

            getLeadEnrichmentAttributes(rtsBulkScoringConfig.getCustomerSpace(), leadEnrichmentAttributeMap,
                    leadEnrichmentAttributeDisplayNameMap);
        }

        if (path == null) {
            throw new IllegalArgumentException("The path is null.");
        }
        List<String> modelGuids = rtsBulkScoringConfig.getModelGuids();
        if (modelGuids.isEmpty()) {
            throw new LedpException(LedpCode.LEDP_20033);
        }

        FileReader<GenericRecord> reader = instantiateReaderForBulkScoreRequest(path);
        String fileName = UUID.randomUUID() + ScoringDaemonService.AVRO_FILE_SUFFIX;
        checkForInternalId(path);
        Schema schema = createOutputSchema(leadEnrichmentAttributeMap, leadEnrichmentAttributeDisplayNameMap);

        try (DataFileWriter<GenericRecord> dataFileWriter = createDataFileWriter(schema, fileName)) {
            GenericRecordBuilder builder = new GenericRecordBuilder(schema);

            BulkRecordScoreRequest scoreRequest = null;
            do {
                scoreRequest = getBulkScoreRequest(reader, rtsBulkScoringConfig);
                if (scoreRequest == null) {
                    break;
                }
                List<RecordScoreResponse> scoreResponseList = bulkScore(scoreRequest, rtsBulkScoringConfig
                        .getCustomerSpace().toString());
                appendScoreResponseToAvro(scoreResponseList, dataFileWriter, builder, leadEnrichmentAttributeMap,
                        leadEnrichmentAttributeDisplayNameMap);
            } while (scoreRequest != null);
        }

        copyScoreOutputToHdfs(fileName, rtsBulkScoringConfig.getTargetResultDir());

        return "In side the rts bulk scoring processor.";
    }

    @VisibleForTesting
    void copyScoreOutputToHdfs(String fileName, String targetDir) throws IOException {
        String scorePath = String.format(targetDir + "/%s", fileName);
        log.info("The output score path is " + scorePath);
        HdfsUtils.copyLocalToHdfs(new Configuration(), fileName, scorePath);
    }

    @VisibleForTesting
    DataFileWriter<GenericRecord> createDataFileWriter(Schema schema, String fileName) throws IOException {
        DatumWriter<GenericRecord> userDatumWriter = new GenericDatumWriter<>();
        DataFileWriter<GenericRecord> dataFileWriter = new DataFileWriter<>(userDatumWriter);
        dataFileWriter.create(schema, new File(fileName));
        return dataFileWriter;
    }

    private List<RecordScoreResponse> bulkScore(BulkRecordScoreRequest scoreRequest, String customerSpace) {
        long startTime = System.currentTimeMillis();
        log.info(String.format("Sending internal scoring api with %d records to for tenant %s", scoreRequest
                .getRecords().size(), customerSpace));
        List<RecordScoreResponse> recordScoreResponse = internalScoringApiProxy.scorePercentileRecords(scoreRequest,
                customerSpace);
        long endTime = System.currentTimeMillis();
        long oneBatchTime = endTime - startTime;
        log.info("Sending this batch of score requests takes " + (oneBatchTime * 1.66667e-5) + " mins");

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

    private void getLeadEnrichmentAttributes(CustomerSpace customerSpace, Map<String, Schema.Type> attributeMap,
            Map<String, String> attributeDisplayNameMap) {
        List<LeadEnrichmentAttribute> leadEnrichmentAttributeList = internalResourceRestApiProxy
                .getLeadEnrichmentAttributes(customerSpace, null, null, Boolean.TRUE);
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
        }
        log.info("The attributeMap is: " + attributeMap);
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
        while (reader.hasNext() && recordCount <= BulkRecordScoreRequest.MAX_ALLOWED_RECORDS) {
            GenericRecord avroRecord = reader.next();
            Schema schema = avroRecord.getSchema();
            List<Schema.Field> fields = schema.getFields();

            Record record = new Record();
            record.setPerformEnrichment(rtsBulkScoringConfig.isEnableLeadEnrichment());
            record.setIdType(DEFAULT_ID_TYPE);

            String idStr = null;
            if (!useInternalId) {
                idStr = avroRecord.get(InterfaceName.Id.toString()).toString();
            } else {
                idStr = avroRecord.get(InterfaceName.InternalId.toString()).toString();
            }
            log.info("idStr is " + idStr);

            Map<String, Object> attributeValues = new HashMap<>();
            for (Schema.Field field : fields) {
                String fieldName = field.name();
                Object fieldValue = avroRecord.get(fieldName) == null ? null : avroRecord.get(fieldName).toString();
                attributeValues.put(fieldName, fieldValue);
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

        log.info(String.format("score request contains %d records; is: %s", records.size(), scoreRequest));

        return scoreRequest;
    }

    @VisibleForTesting
    void checkForInternalId(String path) throws IOException {
        try (FileReader<GenericRecord> prereader = instantiateReaderForBulkScoreRequest(path);) {
            GenericRecord avroRecord = prereader.next();
            Object idObj = avroRecord.get(InterfaceName.Id.toString());
            if (idObj == null) {
                idObj = avroRecord.get(InterfaceName.InternalId.toString());
                useInternalId = true;
            }
            if (idObj == null) {
                throw new LedpException(LedpCode.LEDP_20034);
            }
            log.info("useInternalId is " + useInternalId);
        }
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
        modelIdAttr.setName("ModelId");
        modelIdAttr.setDisplayName("ModelId");
        modelIdAttr.setSourceLogicalDataType("");
        modelIdAttr.setPhysicalDataType(Type.STRING.name());
        Attribute scoreAttr = new Attribute();
        scoreAttr.setName("Score");
        scoreAttr.setDisplayName("Score");
        scoreAttr.setSourceLogicalDataType("");
        scoreAttr.setPhysicalDataType(Type.DOUBLE.name());
        outputTable.addAttribute(idAttr);
        outputTable.addAttribute(modelIdAttr);
        outputTable.addAttribute(scoreAttr);

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

    @VisibleForTesting
    void appendScoreResponseToAvro(List<RecordScoreResponse> recordScoreResponseList,
            DataFileWriter<GenericRecord> dataFileWriter, GenericRecordBuilder builder,
            Map<String, Schema.Type> leadEnrichmentAttributeMap,
            Map<String, String> leadEnrichmentAttributeDisplayNameMap) throws IOException {
        for (RecordScoreResponse scoreResponse : recordScoreResponseList) {
            List<ScoreModelTuple> scoreModelTupleList = scoreResponse.getScores();
            String id = scoreResponse.getId();
            if (StringUtils.isBlank(id)) {
                throw new LedpException(LedpCode.LEDP_20035);
            }
            for (ScoreModelTuple tuple : scoreModelTupleList) {
                if (!useInternalId) {
                    builder.set(InterfaceName.Id.toString(), id);
                } else {
                    builder.set(InterfaceName.InternalId.toString(), Long.valueOf(id));
                }
                String modelId = tuple.getModelId();
                if (StringUtils.isBlank(modelId)) {
                    throw new LedpException(LedpCode.LEDP_20036);
                }
                Double score = tuple.getScore();
                if (score != null && (score > 99 || score < 5)) {
                    throw new LedpException(LedpCode.LEDP_20037);
                }
                builder.set("ModelId", modelId);
                builder.set("Score", score);

                Map<String, Object> enrichmentAttributeValues = scoreResponse.getEnrichmentAttributeValues();
                if (leadEnrichmentAttributeMap != null) {
                    if (enrichmentAttributeValues == null) {
                        throw new LedpException(LedpCode.LEDP_20038);
                    }
                    Iterator<Entry<String, Object>> it = enrichmentAttributeValues.entrySet().iterator();
                    while (it.hasNext()) {
                        Entry<String, Object> entry = it.next();
                        if (!leadEnrichmentAttributeMap.containsKey(entry.getKey())) {
                            throw new LedpException(LedpCode.LEDP_20039, new String[] { entry.getKey() });
                        }
                        builder.set(entry.getKey(), entry.getValue());
                    }
                }

                dataFileWriter.append(builder.build());
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
            Map<String, Type> leadEnrichmentAttributeMap, Map<String, String> leadEnrichmentAttributeDisplayNameMap)
            throws Exception {
        List<Future<Integer>> futures = new ArrayList<>();
        ExecutorService scoreExecutorService = Executors.newFixedThreadPool(threadpoolSize);
        for (int i = 0; i < threadpoolSize; i++) {
            futures.add(scoreExecutorService.submit(new BulkScoreApiCallable(rtsBulkScoringConfig, reader,
                    dataFileWriter, builder, leadEnrichmentAttributeMap, leadEnrichmentAttributeDisplayNameMap)));
        }

        for (Future<Integer> future : futures) {
            try {
                future.get(threadPoolTimeoutMin, TimeUnit.MINUTES);
            } catch (Exception e) {
                log.fatal(e.getMessage(), e);
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
        private Map<String, String> leadEnrichmentAttributeDisplayNameMap;

        BulkScoreApiCallable(RTSBulkScoringConfiguration rtsBulkScoringConfig, FileReader<GenericRecord> reader,
                DataFileWriter<GenericRecord> dataFileWriter, GenericRecordBuilder builder,
                Map<String, Type> leadEnrichmentAttributeMap, Map<String, String> leadEnrichmentAttributeDisplayNameMap) {
            super();
            this.rtsBulkScoringConfig = rtsBulkScoringConfig;
            this.reader = reader;
            this.dataFileWriter = dataFileWriter;
            this.builder = builder;
            this.leadEnrichmentAttributeMap = leadEnrichmentAttributeMap;
            this.leadEnrichmentAttributeDisplayNameMap = leadEnrichmentAttributeDisplayNameMap;
        }

        @Override
        public Integer call() throws Exception {
            while (true) {
                BulkRecordScoreRequest scoreRequest = null;
                synchronized (reader) {
                    scoreRequest = ScoringProcessor.this.getBulkScoreRequest(reader, rtsBulkScoringConfig);
                    if (scoreRequest == null) {
                        break;
                    }
                }
                List<RecordScoreResponse> scoreResponseList = ScoringProcessor.this.bulkScore(scoreRequest,
                        rtsBulkScoringConfig.getCustomerSpace().toString());
                synchronized (dataFileWriter) {
                    ScoringProcessor.this.appendScoreResponseToAvro(scoreResponseList, dataFileWriter, builder,
                            leadEnrichmentAttributeMap, leadEnrichmentAttributeDisplayNameMap);
                }
            }
            return 0;
        }
    }

}
