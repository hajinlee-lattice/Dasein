package com.latticeengines.scoring.runtime.mapreduce;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.UUID;

import org.apache.avro.generic.GenericData.Record;
import org.apache.avro.mapred.AvroKey;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Mapper;

import com.latticeengines.dataplatform.exposed.mapreduce.MapReduceProperty;
import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.domain.exposed.scoring.ScoreOutput;
import com.latticeengines.scoring.util.LocalizedFiles;
import com.latticeengines.scoring.util.ModelAndLeadInfo;
import com.latticeengines.scoring.util.ScoringMapperPredictUtil;
import com.latticeengines.scoring.util.ScoringMapperTransformUtil;

public class EventDataScoringMapper extends Mapper<AvroKey<Record>, NullWritable, NullWritable, NullWritable> {

    private static final Log log = LogFactory.getLog(EventDataScoringMapper.class);
    private static final long DEFAULT_LEAD_FILE_THRESHOLD = 10000L;

    @Override
    public void run(Context context) throws IOException, InterruptedException {

        String leadInputQueueId = context.getConfiguration().get(ScoringProperty.LEAD_INPUT_QUEUE_ID.name());
        String logDir = context.getConfiguration().get(ScoringProperty.LOG_DIR.name());
        String tenantId = context.getConfiguration().get(ScoringProperty.TENANT_ID.name());

        @SuppressWarnings("deprecation")
        Path[] paths = context.getLocalCacheFiles();
        long leadFileThreshold = context.getConfiguration().getLong(ScoringProperty.LEAD_FILE_THRESHOLD.name(),
                DEFAULT_LEAD_FILE_THRESHOLD);

        try {
            // Store localized files
            LocalizedFiles localizedFiles = ScoringMapperTransformUtil.processLocalizedFiles(paths);

            long transformStartTime = System.currentTimeMillis();

            ModelAndLeadInfo modelAndLeadInfo = ScoringMapperTransformUtil.prepareLeadsForScoring(context,
                    localizedFiles, leadFileThreshold);

            long transformEndTime = System.currentTimeMillis();
            long transformationTotalTime = transformEndTime - transformStartTime;
            log.info("The transformation takes " + (transformationTotalTime * 1.66667e-5) + " mins");
            long totalLeadNumber = modelAndLeadInfo.getTotalleadNumber();
            if (totalLeadNumber == 0) {
                log.error("The mapper gets zero leads.");
                throw new LedpException(LedpCode.LEDP_20015);
            } else {
                log.info("The mapper has transformed: " + totalLeadNumber + " leads.");
                ScoringMapperPredictUtil.evaluate(context, modelAndLeadInfo.getModelInfoMap().keySet());
                List<ScoreOutput> resultList = ScoringMapperPredictUtil.processScoreFiles(modelAndLeadInfo,
                        localizedFiles.getModels(), leadFileThreshold);
                log.info("The mapper has scored: " + resultList.size() + " leads.");
                if (totalLeadNumber != resultList.size()) {
                    throw new LedpException(LedpCode.LEDP_20009, new String[] { String.valueOf(totalLeadNumber),
                            String.valueOf(resultList.size()) });
                }

                String outputPath = context.getConfiguration().get(MapReduceProperty.OUTPUT.name());
                log.info("outputDir: " + outputPath);
                ScoringMapperPredictUtil.writeToOutputFile(resultList, context.getConfiguration(), outputPath);

                long scoringEndTime = System.currentTimeMillis();
                long scoringTotalTime = scoringEndTime - transformEndTime;
                log.info("The scoring takes " + (scoringTotalTime * 1.66667e-5) + " mins");
            }
        } catch (Exception e) {
            String errorMessage = String
                    .format("TenantId=%s leadnputQueueId=%s Failure Step=Scoring Mapper Failure Message=%s Failure StackTrace=%s", //
                            tenantId, leadInputQueueId, e.getMessage(), ExceptionUtils.getStackTrace(e));
            log.error(errorMessage);
            File logFile = new File(logDir + "/" + UUID.randomUUID() + ".err");
            FileUtils.writeStringToFile(logFile, errorMessage);
            throw new LedpException(LedpCode.LEDP_20014, e);
        }
    }
}
