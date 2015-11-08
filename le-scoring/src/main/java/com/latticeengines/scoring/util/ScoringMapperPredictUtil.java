package com.latticeengines.scoring.util;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericData.Record;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.mapred.AvroKey;
import org.apache.avro.specific.SpecificDatumWriter;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.io.FileUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.MapContext;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.google.common.annotations.VisibleForTesting;
import com.latticeengines.common.exposed.util.AvroUtils;
import com.latticeengines.common.exposed.util.HdfsUtils;
import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.domain.exposed.scoring.ScoreOutput;
import com.latticeengines.scoring.orchestration.service.ScoringDaemonService;
import com.latticeengines.scoring.runtime.mapreduce.EventDataScoringMapper;

public class ScoringMapperPredictUtil {

    private static final Log log = LogFactory.getLog(EventDataScoringMapper.class);

    public static String evaluate(MapContext<AvroKey<Record>, NullWritable, NullWritable, NullWritable> context,
            Set<String> uuidSet) throws IOException, InterruptedException {

        StringBuilder strs = new StringBuilder();
        // spawn python
        StringBuilder sb = new StringBuilder();
        for (String modelGuid : uuidSet) {
            sb.append(modelGuid + " ");
        }

        log.info("/usr/local/bin/python2.7 " + "scoring.py " + sb.toString());
        File pyFile = new File("scoring.py");
        if (!pyFile.exists()) {
            throw new LedpException(LedpCode.LEDP_20002);
        }

        Process p = Runtime.getRuntime().exec("/usr/local/bin/python2.7 " + "scoring.py " + sb.toString());

        try (BufferedReader err = new BufferedReader(new InputStreamReader(p.getErrorStream()))) {
            String line = "";
            StringBuilder errors = new StringBuilder();
            try (BufferedReader in = new BufferedReader(new InputStreamReader(p.getInputStream()))) {

                while ((line = in.readLine()) != null) {
                    strs.append(line);
                    log.info("This is python info: " + line);
                    context.progress();
                }
            }
            while ((line = err.readLine()) != null) {
                errors.append(line);
                strs.append(line);
                log.error("This is python error: " + line);
                context.progress();
            }

            int exitCode = p.waitFor();
            log.info("The exit code for python is " + exitCode);

            if (errors.length() != 0) {
                throw new LedpException(LedpCode.LEDP_20011, new String[] { errors.toString() });
            }
        }
        return strs.toString();
    }

    public static void writeToOutputFile(List<ScoreOutput> resultList, Configuration yarnConfiguration,
            String outputPath) throws Exception {

        String fileName = UUID.randomUUID() + ScoringDaemonService.AVRO_FILE_SUFFIX;
        File outputFile = new File(fileName);
        DatumWriter<ScoreOutput> userDatumWriter = new SpecificDatumWriter<ScoreOutput>();
        DataFileWriter<ScoreOutput> dataFileWriter = new DataFileWriter<ScoreOutput>(userDatumWriter);

        for (int i = 0; i < resultList.size(); i++) {
            ScoreOutput result = resultList.get(i);
            if (i == 0) {
                dataFileWriter.create(result.getSchema(), outputFile);
            }
            dataFileWriter.append(result);
        }

        dataFileWriter.close();
        HdfsUtils.copyLocalToHdfs(yarnConfiguration, fileName, outputPath + "/" + fileName);
    }

    public static List<ScoreOutput> processScoreFiles(ModelAndRecordInfo modelAndRecordInfo,
            Map<String, JsonNode> models, long recordFileThreshold) throws IOException {
        Map<String, ModelAndRecordInfo.ModelInfo> modelInfoMap = modelAndRecordInfo.getModelInfoMap();
        Set<String> uuidSet = modelInfoMap.keySet();
        // list of HashMap<leadId: score>
        List<ScoreOutput> resultList = new ArrayList<ScoreOutput>();
        for (String uuid : uuidSet) {
            log.info("uuid is " + uuid);
            // key: leadID, value: list of raw scores for that lead
            Map<String, List<Double>> scores = new HashMap<String, List<Double>>();
            long value = modelInfoMap.get(uuid).getRecordCount();
            JsonNode model = models.get(uuid);
            long remain = value / recordFileThreshold;
            int totalRawScoreNumber = 0;
            for (int i = 0; i <= remain; i++) {
                totalRawScoreNumber += readScoreFile(uuid, i, scores);
            }
            List<String> duplicateLeadIdList = checkForDuplicateLeads(scores, totalRawScoreNumber, uuid);
            if (!CollectionUtils.isEmpty(duplicateLeadIdList)) {
                String message = String.format("The duplicate leads for model %s are: %s\n", uuid,
                        Arrays.toString(duplicateLeadIdList.toArray()));
                log.warn(message);
            }
            Set<String> keySet = scores.keySet();
            for (String key : keySet) {
                List<Double> rawScoreList = scores.get(key);
                for (Double rawScore : rawScoreList) {
                    ScoreOutput result = getResult(modelInfoMap, uuid, key, model, rawScore);
                    resultList.add(result);
                }
            }
        }
        return resultList;
    }

    @VisibleForTesting
    static List<String> checkForDuplicateLeads(Map<String, List<Double>> scoreMap, int totalRawScoreNumber,
            String modelGuid) {
        List<String> duplicateLeadsList = new ArrayList<String>();
        if (totalRawScoreNumber != scoreMap.size()) {
            int duplicateLeadNumber = totalRawScoreNumber - scoreMap.size();
            String message = String.format("There are %d duplicate leads for model %s\n", duplicateLeadNumber,
                    modelGuid);
            log.warn(message);
            Set<String> leadIdSet = scoreMap.keySet();
            for (String leadId : leadIdSet) {
                if (scoreMap.get(leadId).size() > 1) {
                    duplicateLeadsList.add(leadId);
                }
            }
        }
        return duplicateLeadsList;
    }

    private static int readScoreFile(String uuid, int index, Map<String, List<Double>> scores) throws IOException {

        int rawScoreNum = 0;
        String fileName = uuid + ScoringDaemonService.SCORING_OUTPUT_PREFIX + index + ".txt";
        File f = new File(fileName);
        if (!f.exists()) {
            throw new LedpException(LedpCode.LEDP_20012, new String[] { fileName });
        }

        List<String> lines = FileUtils.readLines(f);
        for (String line : lines) {
            String[] splitLine = line.split(",");
            if (splitLine.length != 2) {
                throw new LedpException(LedpCode.LEDP_20013);
            }
            String recordId = splitLine[0];
            Double rawScore = Double.parseDouble(splitLine[1]);
            if (scores.containsKey(recordId)) {
                scores.get(recordId).add(rawScore);
            } else {
                List<Double> scoreListWithRecordId = new ArrayList<Double>();
                scoreListWithRecordId.add(rawScore);
                scores.put(recordId, scoreListWithRecordId);
            }
            rawScoreNum++;
        }
        return rawScoreNum;
    }

    private static ScoreOutput getResult(Map<String, ModelAndRecordInfo.ModelInfo> modelInfoMap, String uuid,
            String leadId, JsonNode model, double score) {
        Double probability = null;

        // perform calibration
        ArrayNode calibrationRanges = (ArrayNode) model.get(ScoringDaemonService.CALIBRATION);
        if (calibrationRanges != null) {
            for (int i = 0; i < calibrationRanges.size(); i++) {
                JsonNode range = (JsonNode) calibrationRanges.get(i);
                JsonNode lowerBoundObj = range.get(ScoringDaemonService.CALIBRATION_MINIMUMSCORE);
                JsonNode upperBoundObj = range.get(ScoringDaemonService.CALIBRATION_MAXIMUMSCORE);
                Double lowerBound = lowerBoundObj.isNull() ? null : lowerBoundObj.asDouble();
                Double upperBound = upperBoundObj.isNull() ? null : upperBoundObj.asDouble();
                if (betweenBounds(score, lowerBound, upperBound)) {
                    JsonNode probabilityObj = range.get(ScoringDaemonService.CALIBRATION_PROBABILITY);
                    probability = probabilityObj.isNull() ? null : probabilityObj.asDouble();
                    break;
                }
            }
        }

        Object averageProbabilityObj = model.get(ScoringDaemonService.AVERAGE_PROBABILITY);
        Double averageProbability = averageProbabilityObj == null ? null : Double.parseDouble(averageProbabilityObj
                .toString());
        Double lift = averageProbability != null && averageProbability != 0 ? (double) probability / averageProbability
                : null;

        // perform bucketing
        String bucket = null;
        ArrayNode bucketRanges = (ArrayNode) model.get(ScoringDaemonService.BUCKETS);
        if (bucketRanges != null) {
            for (int i = 0; i < bucketRanges.size(); i++) {
                Double value = probability;
                JsonNode range = bucketRanges.get(i);
                if (value == null) {
                    value = score;
                }
                // "0 - Probability, 1 - Lift"
                if (range.get(ScoringDaemonService.BUCKETS_TYPE).asInt() == 1) {
                    value = lift;
                }
                JsonNode lowerBoundObj = range.get(ScoringDaemonService.BUCKETS_MINIMUMSCORE);
                JsonNode upperBoundObj = range.get(ScoringDaemonService.BUCKETS_MAXIMUMSCORE);
                Double lowerBound = lowerBoundObj.isNull() ? null : lowerBoundObj.asDouble();
                Double upperBound = upperBoundObj.isNull() ? null : upperBoundObj.asDouble();
                if (value != null && betweenBounds(value, lowerBound, upperBound)) {
                    bucket = range.get(ScoringDaemonService.BUCKETS_NAME).asText();
                    break;
                }
            }
        }

        // bucket into percentiles
        Integer percentile = null;
        ArrayNode percentileRanges = (ArrayNode) model.get(ScoringDaemonService.PERCENTILE_BUCKETS);
        if (percentileRanges != null) {
            Double topPercentileMaxScore = (Double) 0.0;
            Double bottomPercentileMinScore = (Double) 1.0;
            Integer topPercentile = 100;
            Integer bottomPercentile = 1;
            boolean foundTopPercentileMaxScore = false;
            boolean foundbottomPercentileMinScore = false;
            for (int i = 0; i < percentileRanges.size(); i++) {
                JsonNode range = percentileRanges.get(i);
                JsonNode minObject = range.get(ScoringDaemonService.PERCENTILE_BUCKETS_MINIMUMSCORE);
                JsonNode maxObject = range.get(ScoringDaemonService.PERCENTILE_BUCKETS_MAXIMUMSCORE);
                Double min = minObject.isNull() ? null : minObject.asDouble();
                Double max = maxObject.isNull() ? null : maxObject.asDouble();
                JsonNode percentObj = range.get(ScoringDaemonService.PERCENTILE_BUCKETS_PERCENTILE);
                Integer percent = percentObj.isNull() ? null : percentObj.asInt();
                if (max > topPercentileMaxScore) {
                    topPercentileMaxScore = max;
                    topPercentile = percent;
                    foundTopPercentileMaxScore = true;
                }
                if (min < bottomPercentileMinScore) {
                    bottomPercentileMinScore = min;
                    bottomPercentile = percent;
                    foundbottomPercentileMinScore = true;
                }
            }

            if (foundTopPercentileMaxScore && score >= topPercentileMaxScore) {
                percentile = topPercentile;
            } else if (foundbottomPercentileMinScore && score <= bottomPercentileMinScore) {
                percentile = bottomPercentile;
            } else {
                for (int i = 0; i < percentileRanges.size(); i++) {
                    JsonNode range = percentileRanges.get(i);
                    JsonNode minObject = range.get(ScoringDaemonService.PERCENTILE_BUCKETS_MINIMUMSCORE);
                    JsonNode maxObject = range.get(ScoringDaemonService.PERCENTILE_BUCKETS_MAXIMUMSCORE);
                    Double min = minObject.isNull() ? null : minObject.asDouble();
                    Double max = maxObject.isNull() ? null : maxObject.asDouble();
                    JsonNode percentObj = range.get(ScoringDaemonService.PERCENTILE_BUCKETS_PERCENTILE);
                    Integer percent = percentObj.isNull() ? null : percentObj.asInt();
                    if (betweenBounds(score, min, max)) {
                        percentile = percent;
                        break;
                    }
                }
            }

        }

        Integer integerScore = (int) (probability != null ? Math.round(probability * 100) : Math.round(score * 100));
        String modelGuid = modelInfoMap.get(uuid).getModelGuid();
        ScoreOutput result = new ScoreOutput(leadId, bucket, lift, modelGuid, percentile, probability, score,
                integerScore);
        return result;

    }

    private static boolean betweenBounds(double value, Double lowerInclusive, Double upperExclusive) {
        return (lowerInclusive == null || value >= lowerInclusive)
                && (upperExclusive == null || value < upperExclusive);
    }

    public static void main(String[] args) throws IllegalArgumentException, Exception {

        String hdfs = "/user/s-analytics/customers/Nutanix/scoring/data/part-m-00000.avro";
        List<GenericRecord> list = AvroUtils.getData(new Configuration(), new Path(hdfs));
        for (GenericRecord ele : list) {
            System.out.println(ele.toString());
        }
    }
}
