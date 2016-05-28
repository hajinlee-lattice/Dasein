package com.latticeengines.scoring.yarn.runtime;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.apache.avro.Schema;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.reflect.ReflectData;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.beans.BeansException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;

import com.google.common.annotations.VisibleForTesting;
import com.latticeengines.common.exposed.util.AvroUtils;
import com.latticeengines.common.exposed.util.HdfsUtils;
import com.latticeengines.dataplatform.exposed.yarn.runtime.SingleContainerYarnProcessor;
import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.domain.exposed.metadata.InterfaceName;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.scoring.RTSBulkScoringConfiguration;
import com.latticeengines.domain.exposed.scoringapi.BulkRecordScoreRequest;
import com.latticeengines.domain.exposed.scoringapi.Record;
import com.latticeengines.domain.exposed.scoringapi.RecordScoreResponse;
import com.latticeengines.domain.exposed.scoringapi.RecordScoreResponse.ScoreModelTuple;
import com.latticeengines.domain.exposed.scoringapi.ScoreResult;
import com.latticeengines.domain.exposed.util.ExtractUtils;
import com.latticeengines.proxy.exposed.scoringapi.ScoringApiProxy;
import com.latticeengines.scoring.orchestration.service.ScoringDaemonService;

public class ScoringProcessor extends SingleContainerYarnProcessor<RTSBulkScoringConfiguration> implements
        ItemProcessor<RTSBulkScoringConfiguration, String>, ApplicationContextAware {

    private static final Log log = LogFactory.getLog(ScoringProcessor.class);

    public static final String RECORD_RULE = "manual";
    public static final String RECORD_SOURCE = "file";
    public static final String DEFAULT_ID_TYPE = "internal";
    public static final boolean DEFAULT_ENRICHMENT = false;

    private ApplicationContext applicationContext;

    @Autowired
    private Configuration yarnConfiguration;

    @Autowired
    private ScoringApiProxy scoringApiProxy;

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
        String path = getExtractPath(rtsBulkScoringConfig);
        List<BulkRecordScoreRequest> bulkScoreRequestList = convertAvroToBulkScoreRequest(path, rtsBulkScoringConfig);
        List<RecordScoreResponse> recordScoreResponseList = new ArrayList<RecordScoreResponse>();
        for (BulkRecordScoreRequest scoreRequest : bulkScoreRequestList) {
            List<RecordScoreResponse> recordScoreResponse = scoringApiProxy.bulkScore(scoreRequest);
            recordScoreResponseList.addAll(recordScoreResponse);
        }
        convertBulkScoreResponseToAvro(recordScoreResponseList, rtsBulkScoringConfig.getTargetResultDir());
        return "In side the rts bulk scoring processor.";
    }

    private String getExtractPath(RTSBulkScoringConfiguration rtsBulkScoringConfig) {
        Table metadataTable = rtsBulkScoringConfig.getMetadataTable();
        if (metadataTable == null) {
            throw new LedpException(LedpCode.LEDP_20028, new String[] { rtsBulkScoringConfig.toString() });
        }
        String path = ExtractUtils.getSingleExtractPath(yarnConfiguration, metadataTable);
        return path;
    }

    @VisibleForTesting
    List<BulkRecordScoreRequest> convertAvroToBulkScoreRequest(String path,
            RTSBulkScoringConfiguration rtsBulkScoringConfig) throws IOException {
        if (path == null) {
            throw new IllegalArgumentException("The path is null.");
        }
        List<String> modelGuids = rtsBulkScoringConfig.getModelGuids();
        if (modelGuids.isEmpty()) {
            throw new LedpException(LedpCode.LEDP_20033);
        }

        List<BulkRecordScoreRequest> requestList = new ArrayList<BulkRecordScoreRequest>();

        String fileName = getAvroFileName(path);
        List<GenericRecord> avroRecords = AvroUtils.getDataFromGlob(yarnConfiguration, fileName);
        Schema schema = avroRecords.get(0).getSchema();
        List<Schema.Field> fields = schema.getFields();

        BulkRecordScoreRequest scoreRequest = new BulkRecordScoreRequest();
        scoreRequest.setRule(RECORD_RULE);
        scoreRequest.setSource(RECORD_SOURCE);
        List<Record> records = new ArrayList<Record>();
        scoreRequest.setRecords(records);

        int i = 0;
        for (GenericRecord avroRecord : avroRecords) {
            i++;
            Record record = new Record();
            record.setPerformEnrichment(DEFAULT_ENRICHMENT);
            record.setIdType(DEFAULT_ID_TYPE);

            Object recordIdObj = avroRecord.get(InterfaceName.Id.toString());
            if (StringUtils.isBlank(recordIdObj.toString())) {
                throw new LedpException(LedpCode.LEDP_20034);
            }
            record.setModelIds(modelGuids);
            record.setRecordId(recordIdObj.toString());
            Map<String, Object> attributeValues = new HashMap<>();
            record.setAttributeValues(attributeValues);
            for (Schema.Field field : fields) {
                String fieldName = field.name();
                Object fieldValue = avroRecord.get(fieldName);
                attributeValues.put(fieldName, fieldValue);
            }
            records.add(record);

            if (i % BulkRecordScoreRequest.MAX_ALLOWED_RECORDS == 0) {
                requestList.add(scoreRequest);
                scoreRequest = new BulkRecordScoreRequest();
                scoreRequest.setRule(RECORD_RULE);
                scoreRequest.setSource(RECORD_SOURCE);
                records = new ArrayList<Record>();
                scoreRequest.setRecords(records);
            }
        }

        if (records.size() > 0) {
            requestList.add(scoreRequest);
        }

        return requestList;
    }

    @VisibleForTesting
    void convertBulkScoreResponseToAvro(List<RecordScoreResponse> recordScoreResponseList, String targetPath)
            throws IOException {
        String fileName = UUID.randomUUID() + ScoringDaemonService.AVRO_FILE_SUFFIX;
        String scorePath = String.format(targetPath + "/%s", fileName);
        log.info("The output score path is " + scorePath);

        Schema schema = ReflectData.get().getSchema(ScoreResult.class);

        DatumWriter<GenericRecord> userDatumWriter = new GenericDatumWriter<>();

        try (DataFileWriter<GenericRecord> dataFileWriter = new DataFileWriter<>(userDatumWriter)) {

            dataFileWriter.create(schema, new File(fileName));
            GenericRecordBuilder builder = new GenericRecordBuilder(schema);
            for (RecordScoreResponse scoreResponse : recordScoreResponseList) {
                List<ScoreModelTuple> scoreModelTupleList = scoreResponse.getScores();
                String id = scoreResponse.getId();
                if (StringUtils.isBlank(id)) {
                    throw new LedpException(LedpCode.LEDP_20035);
                }
                for (ScoreModelTuple tuple : scoreModelTupleList) {
                    builder.set("id", id);
                    String modelId = tuple.getModelId();
                    if (StringUtils.isBlank(modelId)) {
                        throw new LedpException(LedpCode.LEDP_20036);
                    }
                    double score = tuple.getScore();
                    if (score > 99 || score < 5) {
                        throw new LedpException(LedpCode.LEDP_20037);
                    }
                    builder.set("modelId", modelId);
                    builder.set("score", score);
                    dataFileWriter.append(builder.build());
                }
            }
        }
        HdfsUtils.copyLocalToHdfs(new Configuration(), fileName, scorePath);
    }

    private String getAvroFileName(String path) throws IOException {
        List<String> files = HdfsUtils.getFilesForDir(yarnConfiguration, path, "*.avro");
        String fileName = files.size() > 0 ? files.get(0) : null;
        if (fileName == null) {
            throw new LedpException(LedpCode.LEDP_12003, new String[] { path });
        }
        return fileName;
    }

}
