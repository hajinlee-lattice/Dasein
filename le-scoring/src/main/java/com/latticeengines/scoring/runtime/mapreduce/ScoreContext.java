package com.latticeengines.scoring.runtime.mapreduce;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericData.Record;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.mapred.AvroKey;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Mapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.latticeengines.common.exposed.util.UuidUtils;
import com.latticeengines.domain.exposed.scoring.ScoringConfiguration.ScoringInputType;
import com.latticeengines.scoring.orchestration.service.ScoringDaemonService;
import com.latticeengines.scoring.util.ModelAndRecordInfo;
import com.latticeengines.scoring.util.ScoringMapperValidateUtil;

public class ScoreContext {

    private static final Logger log = LoggerFactory.getLogger(ScoreContext.class);
    private static final long DEFAULT_LEAD_FILE_THRESHOLD = 10000L;

    public OutputStream out = null;
    public DataFileWriter<GenericRecord> writer = null;
    public DataFileWriter<GenericRecord> creator = null;
    public Map<String, ModelAndRecordInfo.ModelInfo> modelInfoMap = new HashMap<String, ModelAndRecordInfo.ModelInfo>();
    public Map<String, BufferedWriter> recordFileBufferMap = new HashMap<String, BufferedWriter>();
    public String type;
    public boolean readModelIdFromRecord;
    public int recordNumber = 0;
    public Collection<String> modelGuids;
    public ObjectMapper mapper;
    public String uniqueKeyColumn;
    public long recordFileThreshold;
    public ModelAndRecordInfo modelAndRecordInfo = new ModelAndRecordInfo();

    public Map<String, String> uuidToModeId = new HashMap<>();

    public ScoreContext() {
    }

    public ScoreContext(Mapper<AvroKey<Record>, NullWritable, NullWritable, NullWritable>.Context context) {
        Configuration config = context.getConfiguration();
        type = config.get(ScoringProperty.SCORE_INPUT_TYPE.name(), ScoringInputType.Json.name());
        readModelIdFromRecord = config.getBoolean(ScoringProperty.READ_MODEL_ID_FROM_RECORD.name(), true);
        modelGuids = config.getStringCollection(ScoringProperty.MODEL_GUID.name());
        uniqueKeyColumn = config.get(ScoringProperty.UNIQUE_KEY_COLUMN.name());
        recordFileThreshold = config.getLong(ScoringProperty.RECORD_FILE_THRESHOLD.name(), DEFAULT_LEAD_FILE_THRESHOLD);
        mapper = new ObjectMapper();
        modelAndRecordInfo = new ModelAndRecordInfo();
        modelAndRecordInfo.setModelInfoMap(modelInfoMap);
    }

    public ModelAndRecordInfo verify() {
        modelAndRecordInfo.setTotalRecordCountr(recordNumber);
        ScoringMapperValidateUtil.validateTransformation(modelAndRecordInfo);
        return modelAndRecordInfo;
    }

    public void closeFiles(String uuid) throws IOException, InterruptedException {
        if (out != null) {
            creator.close();
            writer.close();
            creator = null;
            writer = null;
            out = null;
        }
        for (String key : recordFileBufferMap.keySet()) {
            if (key.startsWith(uuid)) {
                recordFileBufferMap.get(key).close();
                recordFileBufferMap.remove(key);
            }
        }
    }

    public Map<String, List<Record>> buildRecords(
            Mapper<AvroKey<Record>, NullWritable, NullWritable, NullWritable>.Context context)
            throws IOException, InterruptedException {
        Map<String, List<Record>> res = new HashMap<>();
        int total = 0;
        if (readModelIdFromRecord) {
            while (context.nextKeyValue()) {
                Record origRecord = context.getCurrentKey().datum();
                Record record = new Record(origRecord, true);
                String modelGuid = (String) record.get(ScoringDaemonService.MODEL_GUID);
                String uuid = UuidUtils.extractUuid(modelGuid);
                res.putIfAbsent(uuid, new ArrayList<>());
                res.get(uuid).add(record);
                uuidToModeId.put(uuid, modelGuid);
                total++;
            }
        } else {
            List<Record> records = new ArrayList<>();
            while (context.nextKeyValue()) {
                Record origRecord = context.getCurrentKey().datum();
                Record record = new Record(origRecord, true);
                records.add(record);
                total++;
            }
            for (String modelGuid : modelGuids) {
                String uuid = UuidUtils.extractUuid(modelGuid);
                res.put(uuid, records);
                uuidToModeId.put(uuid, modelGuid);
            }
        }
        log.info("readModelIdFromRecord=" + readModelIdFromRecord + " total records=" + total);
        res.forEach((key, value) -> {
            log.info("UUID=" + key + " records=" + value.size());
        });
        return res;
    }
}
