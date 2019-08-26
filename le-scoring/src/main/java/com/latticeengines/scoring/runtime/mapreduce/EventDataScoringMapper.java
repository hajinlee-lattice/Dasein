package com.latticeengines.scoring.runtime.mapreduce;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.nio.charset.Charset;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData.Record;
import org.apache.avro.mapred.AvroKey;
import org.apache.avro.mapreduce.AvroJob;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Mapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.JsonNode;
import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.domain.exposed.scoringapi.ScoreDerivation;
import com.latticeengines.scoring.util.ModelAndRecordInfo;
import com.latticeengines.scoring.util.ScoringJobUtil;
import com.latticeengines.scoring.util.ScoringMapperPredictUtil;
import com.latticeengines.scoring.util.ScoringMapperTransformUtil;

public class EventDataScoringMapper extends Mapper<AvroKey<Record>, NullWritable, NullWritable, NullWritable> {

    private static final Logger log = LoggerFactory.getLogger(EventDataScoringMapper.class);

    @Override
    public void run(Context context) throws IOException, InterruptedException {
        int taskId = context.getTaskAttemptID().getTaskID().getId();
        Configuration config = context.getConfiguration();
        Schema schema = AvroJob.getInputKeySchema(config);
        URI[] uris = context.getCacheFiles();

        try {
            JsonNode dataType = ScoringJobUtil.generateDataTypeSchema(schema);
            log.info("DataType :" + dataType.asText() + " task Id :" + taskId);
            ScoreContext scoreContext = new ScoreContext(context);
            Map<String, List<Record>> recordsMap = scoreContext.buildRecords(context);
            Map<String, URI> uuidURIMap = ScoringMapperTransformUtil.getModelUris(uris);
            long scoringStartTime = System.currentTimeMillis();
            for (String uuid : scoreContext.uuidToModeId.keySet()) {
                URI uri = uuidURIMap.get(uuid);
                if (CollectionUtils.isEmpty(recordsMap.get(uuid))) {
                    log.warn("There's no records for model uuid=" + uuid);
                    continue;
                }
                Map<String, JsonNode> models = transformRecords(dataType, scoreContext, recordsMap, uuid, uri);
                ScoringMapperPredictUtil.evaluate(uuid, scoreContext, context);
                if (config.getBoolean(ScoringProperty.USE_SCOREDERIVATION.name(), false)) {
                    log.info("Using score derivation to generate percentile score.");
                    Map<String, ScoreDerivation> scoreDerivationMap = ScoringMapperTransformUtil
                            .deserializeLocalScoreDerivationFiles(uuid, uris);
                    ScoringMapperPredictUtil.processScoreFilesUsingScoreDerivation(uuid, config,
                            scoreContext.modelAndRecordInfo, scoreDerivationMap, scoreContext.recordFileThreshold,
                            taskId);
                } else {
                    ScoringMapperPredictUtil.processScoreFiles(uuid, config, scoreContext.modelAndRecordInfo, models,
                            scoreContext.recordFileThreshold, taskId);
                }
            }

            ModelAndRecordInfo modelAndRecordInfo = scoreContext.verify();
            long totalRecordCount = modelAndRecordInfo.getTotalRecordCount();
            log.info("The mapper has scored: " + totalRecordCount + " records.");

            long scoringEndTime = System.currentTimeMillis();
            long scoringTotalTime = scoringEndTime - scoringStartTime;
            log.info("The scoring takes " + (scoringTotalTime * 1.66667e-5) + " mins");

        } catch (Exception e) {
            String errorMessage = String.format(
                    "TenantId=%s leadnputQueueId=%s Failure Step=Scoring Mapper Failure Message=%s Failure StackTrace=%s", //
                    config.get(ScoringProperty.TENANT_ID.name()),
                    config.get(ScoringProperty.LEAD_INPUT_QUEUE_ID.name()), e.getMessage(),
                    ExceptionUtils.getStackTrace(e));
            log.error(errorMessage);
            File logFile = new File(config.get(ScoringProperty.LOG_DIR.name()) + "/" + UUID.randomUUID() + ".err");
            FileUtils.write(logFile, errorMessage, Charset.forName("UTF-8"));
            throw new LedpException(LedpCode.LEDP_20014, e);
        }
    }

    private Map<String, JsonNode> transformRecords(JsonNode dataType, ScoreContext scoreContext,
            Map<String, List<Record>> recordsMap, String uuid, URI uri) throws IOException, InterruptedException {
        long transformStartTime = System.currentTimeMillis();
        Map<String, JsonNode> models = ScoringMapperTransformUtil.processLocalizedFiles(uri);
        ScoringMapperTransformUtil.prepareRecordsForScoring(uuid, scoreContext, dataType, models, recordsMap.get(uuid));
        scoreContext.closeFiles(uuid);
        long transformEndTime = System.currentTimeMillis();
        long transformationTotalTime = transformEndTime - transformStartTime;
        log.info("The transformation takes " + (transformationTotalTime * 1.66667e-5) + " mins for model Id:"
                + scoreContext.uuidToModeId.get(uuid));
        return models;
    }

}
