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

import org.apache.avro.Schema;
import org.apache.avro.Schema.Type;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;
import org.apache.avro.io.DatumWriter;
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

    private ApplicationContext applicationContext;

    @Autowired
    private Configuration yarnConfiguration;

    @Autowired
    private InternalScoringApiProxy internalScoringApiProxy;

    private InternalResourceRestApiProxy internalResourceRestApiProxy;

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
        String customerSpace = rtsBulkScoringConfig.getCustomerSpace().toString();
        Map<String, Schema.Type> leadEnrichmentAttributeMap = null;
        if (rtsBulkScoringConfig.isEnableLeadEnrichment()) {
            leadEnrichmentAttributeMap = getLeadEnrichmentAttributes(rtsBulkScoringConfig.getCustomerSpace());
        }

        long startTime = System.currentTimeMillis();
        List<BulkRecordScoreRequest> bulkScoreRequestList = convertAvroToBulkScoreRequest(path, rtsBulkScoringConfig);
        long endTime = System.currentTimeMillis();
        long transformationTotalTime = endTime - startTime;
        log.info("The transformation from avro to bulks score request takes " + (transformationTotalTime * 1.66667e-5)
                + " mins");
        long scoringStartTime = System.currentTimeMillis();
        List<RecordScoreResponse> recordScoreResponseList = new ArrayList<RecordScoreResponse>();
        for (BulkRecordScoreRequest scoreRequest : bulkScoreRequestList) {
            startTime = System.currentTimeMillis();
            log.info(String.format("Sending internal scoring api with %d records to for tenant %s", scoreRequest
                    .getRecords().size(), customerSpace));
            List<RecordScoreResponse> recordScoreResponse = internalScoringApiProxy.scorePercentileRecords(
                    scoreRequest, customerSpace);
            endTime = System.currentTimeMillis();
            long oneBatchTime = endTime - startTime;
            log.info("Sending this batch of score requests takes " + (oneBatchTime * 1.66667e-5) + " mins");
            recordScoreResponseList.addAll(recordScoreResponse);
        }
        long scoringEndTime = System.currentTimeMillis();
        long scoringApiRequestTotalTime = scoringEndTime - scoringStartTime;
        log.info("The sending scoring requests takes " + (scoringApiRequestTotalTime * 1.66667e-5) + " mins");
        startTime = System.currentTimeMillis();
        convertBulkScoreResponseToAvro(recordScoreResponseList, rtsBulkScoringConfig.getTargetResultDir(),
                leadEnrichmentAttributeMap);
        endTime = System.currentTimeMillis();
        transformationTotalTime = endTime - startTime;
        log.info("The transformation from bulks score response to avro takes " + (transformationTotalTime * 1.66667e-5)
                + " mins");
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

    private Map<String, Schema.Type> getLeadEnrichmentAttributes(CustomerSpace customerSpace) {
        List<LeadEnrichmentAttribute> leadEnrichmentAttributeList = internalResourceRestApiProxy
                .getLeadEnrichmentAttributes(customerSpace, null, null, Boolean.TRUE);
        Map<String, Schema.Type> attributeMap = new HashMap<String, Schema.Type>();
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
        }
        log.info("The attributeMap is: " + attributeMap);
        return attributeMap;
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
        log.info(String.format("There are %d records in total,", avroRecords.size()));
        Schema schema = avroRecords.get(0).getSchema();
        List<Schema.Field> fields = schema.getFields();

        BulkRecordScoreRequest scoreRequest = new BulkRecordScoreRequest();
        scoreRequest.setSource(RECORD_SOURCE);
        List<Record> records = new ArrayList<Record>();
        scoreRequest.setRecords(records);

        int i = 0;
        for (GenericRecord avroRecord : avroRecords) {
            i++;
            Record record = new Record();
            record.setPerformEnrichment(rtsBulkScoringConfig.isEnableLeadEnrichment());
            record.setIdType(DEFAULT_ID_TYPE);

            Object recordIdObj = avroRecord.get(InterfaceName.Id.toString());
            if (StringUtils.isBlank(recordIdObj.toString())) {
                throw new LedpException(LedpCode.LEDP_20034);
            }

            Map<String, Object> attributeValues = new HashMap<>();
            for (Schema.Field field : fields) {
                String fieldName = field.name();
                Object fieldValue = avroRecord.get(fieldName) == null ? null : avroRecord.get(fieldName).toString();
                attributeValues.put(fieldName, fieldValue);
            }

            Map<String, Map<String, Object>> modelAttributeValuesMap = new HashMap<>();
            for (String modelguid : modelGuids) {
                modelAttributeValuesMap.put(modelguid, attributeValues);
            }

            record.setModelAttributeValuesMap(modelAttributeValuesMap);
            record.setRecordId(recordIdObj.toString());
            record.setRule(RECORD_RULE);

            records.add(record);

            if (i % BulkRecordScoreRequest.MAX_ALLOWED_RECORDS == 0) {
                requestList.add(scoreRequest);
                scoreRequest = new BulkRecordScoreRequest();
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
    void convertBulkScoreResponseToAvro(List<RecordScoreResponse> recordScoreResponseList, String targetPath,
            Map<String, Schema.Type> leadEnrichmentAttributeMap) throws IOException {
        String fileName = UUID.randomUUID() + ScoringDaemonService.AVRO_FILE_SUFFIX;
        String scorePath = String.format(targetPath + "/%s", fileName);
        log.info("The output score path is " + scorePath);

        Table outputTable = new Table();
        outputTable.setName("scoreOutput");
        Attribute idAttr = new Attribute();
        idAttr.setName("Id");
        idAttr.setDisplayName("Id");
        idAttr.setSourceLogicalDataType("");
        idAttr.setPhysicalDataType(Type.STRING.name());
        Attribute modelIdAttr = new Attribute();
        modelIdAttr.setName("modelId");
        modelIdAttr.setDisplayName("modelId");
        modelIdAttr.setSourceLogicalDataType("");
        modelIdAttr.setPhysicalDataType(Type.STRING.name());
        Attribute scoreAttr = new Attribute();
        scoreAttr.setName("score");
        scoreAttr.setDisplayName("score");
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
                attr.setDisplayName(leadEnrichmentAttrName);
                attr.setSourceLogicalDataType("");
                attr.setPhysicalDataType(avroType.name());
                outputTable.addAttribute(attr);
            }
        }
        Schema schema = TableUtils.createSchema(outputTable.getName(), outputTable);

        DatumWriter<GenericRecord> userDatumWriter = new GenericDatumWriter<>();
        try (DataFileWriter<GenericRecord> dataFileWriter = new DataFileWriter<>(userDatumWriter)) {

            dataFileWriter.create(schema, new File(fileName));
            GenericRecordBuilder builder = new GenericRecordBuilder(schema);
            for (RecordScoreResponse scoreResponse : recordScoreResponseList) {
                log.info("the score response is: " + scoreResponse);
                List<ScoreModelTuple> scoreModelTupleList = scoreResponse.getScores();
                String id = scoreResponse.getId();
                if (StringUtils.isBlank(id)) {
                    throw new LedpException(LedpCode.LEDP_20035);
                }
                for (ScoreModelTuple tuple : scoreModelTupleList) {
                    builder.set("Id", id);
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

        HdfsUtils.copyLocalToHdfs(new Configuration(), fileName, scorePath);
    }

    private String getAvroFileName(String path) throws IOException {
        List<String> files = HdfsUtils.getFilesForDir(yarnConfiguration, path, ".*.avro");
        String fileName = files.size() > 0 ? files.get(0) : null;
        if (fileName == null) {
            throw new LedpException(LedpCode.LEDP_12003, new String[] { path });
        }
        return fileName;
    }

}
