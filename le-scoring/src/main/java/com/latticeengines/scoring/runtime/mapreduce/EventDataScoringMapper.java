package com.latticeengines.scoring.runtime.mapreduce;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.UUID;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData.Record;
import org.apache.avro.mapred.AvroKey;
import org.apache.avro.mapreduce.AvroJob;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Mapper;

import com.fasterxml.jackson.databind.JsonNode;
import com.latticeengines.dataplatform.exposed.mapreduce.MapReduceProperty;
import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.domain.exposed.scoring.ScoreOutput;
import com.latticeengines.scoring.util.ModelAndRecordInfo;
import com.latticeengines.scoring.util.ScoringJobUtil;
import com.latticeengines.scoring.util.ScoringMapperPredictUtil;
import com.latticeengines.scoring.util.ScoringMapperTransformUtil;

public class EventDataScoringMapper extends Mapper<AvroKey<Record>, NullWritable, NullWritable, NullWritable> {

    private static final Log log = LogFactory.getLog(EventDataScoringMapper.class);
    private static final long DEFAULT_LEAD_FILE_THRESHOLD = 10000L;

    @Override
    public void run(Context context) throws IOException, InterruptedException {

        Configuration config = context.getConfiguration();
        Schema schema = AvroJob.getInputKeySchema(config);
        @SuppressWarnings("deprecation")
        Path[] paths = context.getLocalCacheFiles();
        long recordFileThreshold = context.getConfiguration().getLong(ScoringProperty.RECORD_FILE_THRESHOLD.name(),
                DEFAULT_LEAD_FILE_THRESHOLD);

        try {
            // Store localized files
            HashMap<String, JsonNode> models = ScoringMapperTransformUtil.processLocalizedFiles(paths);
            long transformStartTime = System.currentTimeMillis();
            JsonNode dataType = ScoringJobUtil.generateDataTypeSchema(schema);
            log.info("DataType :" + dataType.asText());
            ModelAndRecordInfo modelAndRecordInfo = ScoringMapperTransformUtil.prepareRecordsForScoring(context, dataType,
                    models, recordFileThreshold);

            if (modelAndRecordInfo.getTotalRecordCount() == 0) {
                return;
            }
            long transformEndTime = System.currentTimeMillis();
            long transformationTotalTime = transformEndTime - transformStartTime;
            log.info("The transformation takes " + (transformationTotalTime * 1.66667e-5) + " mins");

            long totalRecordCount = modelAndRecordInfo.getTotalRecordCount();
            log.info("The mapper has transformed: " + totalRecordCount + " records.");

            ScoringMapperPredictUtil.evaluate(context, modelAndRecordInfo.getModelInfoMap().keySet());
            List<ScoreOutput> resultList = ScoringMapperPredictUtil.processScoreFiles(modelAndRecordInfo, models,
                    recordFileThreshold);
            log.info("The mapper has scored: " + resultList.size() + " records.");
            if (totalRecordCount != resultList.size()) {
                throw new LedpException(LedpCode.LEDP_20009, new String[] { String.valueOf(totalRecordCount),
                        String.valueOf(resultList.size()) });
            }

            String outputPath = context.getConfiguration().get(MapReduceProperty.OUTPUT.name());
            log.info("outputDir: " + outputPath);
            ScoringMapperPredictUtil.writeToOutputFile(resultList, context.getConfiguration(), outputPath);

            long scoringEndTime = System.currentTimeMillis();
            long scoringTotalTime = scoringEndTime - transformEndTime;
            log.info("The scoring takes " + (scoringTotalTime * 1.66667e-5) + " mins");

        } catch (Exception e) {
            String errorMessage = String
                    .format("TenantId=%s leadnputQueueId=%s Failure Step=Scoring Mapper Failure Message=%s Failure StackTrace=%s", //
                            config.get(ScoringProperty.TENANT_ID.name()),
                            config.get(ScoringProperty.LEAD_INPUT_QUEUE_ID.name()), e.getMessage(),
                            ExceptionUtils.getStackTrace(e));
            log.error(errorMessage);
            File logFile = new File(config.get(ScoringProperty.LOG_DIR.name()) + "/" + UUID.randomUUID() + ".err");
            FileUtils.writeStringToFile(logFile, errorMessage);
            throw new LedpException(LedpCode.LEDP_20014, e);
        }
    }
}
