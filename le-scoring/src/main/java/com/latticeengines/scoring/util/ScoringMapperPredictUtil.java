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
import org.apache.commons.io.FileUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.MapContext;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;

import com.google.common.annotations.VisibleForTesting;
import com.latticeengines.common.exposed.util.AvroUtils;
import com.latticeengines.common.exposed.util.HdfsUtils;
import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.domain.exposed.scoring.ScoreOutput;
import com.latticeengines.scoring.runtime.mapreduce.EventDataScoringMapper;
import com.latticeengines.scoring.service.ScoringDaemonService;

public class ScoringMapperPredictUtil {

    private static final Log log = LogFactory.getLog(EventDataScoringMapper.class);

    public static String evaluate(MapContext<AvroKey<Record>, NullWritable, NullWritable, NullWritable> context,
            Set<String> modelGuidSet) throws IOException, InterruptedException {

        StringBuilder strs = new StringBuilder();
        // spawn python
        StringBuilder sb = new StringBuilder();
        for (String modelGuid : modelGuidSet) {
            sb.append(modelGuid + " ");
        }

        log.info("/usr/local/bin/python2.7 " + "scoring.py " + sb.toString());
        File pyFile = new File("scoring.py");
        if (!pyFile.exists()) {
            throw new LedpException(LedpCode.LEDP_20002);
        }

        Process p = Runtime.getRuntime().exec("/usr/local/bin/python2.7 " + "scoring.py " + sb.toString());

        BufferedReader in = new BufferedReader(new InputStreamReader(p.getInputStream()));
        BufferedReader err = new BufferedReader(new InputStreamReader(p.getErrorStream()));
        String line = "";
        StringBuilder errors = new StringBuilder();

        while ((line = in.readLine()) != null) {
            strs.append(line);
            log.info("This is python info: " + line);
            context.progress();
        }
        in.close();
        while ((line = err.readLine()) != null) {
            errors.append(line);
            strs.append(line);
            log.error("This is python error: " + line);
            context.progress();
        }
        err.close();
        int exitCode = p.waitFor();
        log.info("The exit code for python is " + exitCode);

        if (errors.length() != 0) {
            throw new LedpException(LedpCode.LEDP_20011, new String[] { errors.toString() });
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

    public static List<ScoreOutput> processScoreFiles(ModelAndLeadInfo modelAndLeadInfo,
            Map<String, JSONObject> models, long leadFileThreshold) throws IOException {
        Map<String, ModelAndLeadInfo.ModelInfo> modelInfoMap = modelAndLeadInfo.getModelInfoMap();
        Set<String> modelGuidSet = modelInfoMap.keySet();
        // list of HashMap<leadId: score>
        List<ScoreOutput> resultList = new ArrayList<ScoreOutput>();
        for (String modelGuid : modelGuidSet) {
            log.info("modelGuid is " + modelGuid);
            // key: leadID, value: list of raw scores for that lead
            Map<String, List<Double>> scores = new HashMap<String, List<Double>>();
            long value = modelInfoMap.get(modelGuid).getLeadNumber();
            JSONObject model = models.get(modelGuid);
            long remain = value / leadFileThreshold;
            int totalRawScoreNumber = 0;
            for (int i = 0; i <= remain; i++) {
                totalRawScoreNumber += readScoreFile(modelGuid, i, scores);
            }
            List<String> duplicateLeadIdList = checkForDuplicateLeads(scores, totalRawScoreNumber, modelGuid);
            if (duplicateLeadIdList != null && duplicateLeadIdList.size() > 0) {
                String message = String.format("The duplicate leads for model %s are: %s\n", modelGuid,
                        Arrays.toString(duplicateLeadIdList.toArray()));
                log.warn(message);
            }
            Set<String> keySet = scores.keySet();
            for (String key : keySet) {
                List<Double> rawScoreList = scores.get(key);
                for (Double rawScore : rawScoreList) {
                    ScoreOutput result = getResult(modelInfoMap, modelGuid, key, model, rawScore);
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

    private static int readScoreFile(String modelGuid, int index, Map<String, List<Double>> scores) throws IOException {

        int rawScoreNum = 0;
        String fileName = modelGuid + ScoringDaemonService.SCORING_OUTPUT_PREFIX + index + ".txt";
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
            String leadId = splitLine[0];
            Double rawScore = Double.parseDouble(splitLine[1]);
            if (scores.containsKey(leadId)) {
                scores.get(leadId).add(rawScore);
                rawScoreNum++;
            } else {
                List<Double> scoreListWithSameLeadId = new ArrayList<Double>();
                scoreListWithSameLeadId.add(rawScore);
                scores.put(leadId, scoreListWithSameLeadId);
                rawScoreNum++;
            }
        }
        return rawScoreNum;
    }

    private static ScoreOutput getResult(Map<String, ModelAndLeadInfo.ModelInfo> modelInfoMap, String modelGuid,
            String leadId, JSONObject model, double score) {
        Double probability = null;

        // perform calibration
        JSONArray calibrationRanges = (JSONArray) model.get(ScoringDaemonService.CALIBRATION);
        if (calibrationRanges != null) {
            for (int i = 0; i < calibrationRanges.size(); i++) {
                JSONObject range = (JSONObject) calibrationRanges.get(i);
                Object lowerBoundObj = range.get(ScoringDaemonService.CALIBRATION_MINIMUMSCORE);
                Object upperBoundObj = range.get(ScoringDaemonService.CALIBRATION_MAXIMUMSCORE);
                Double lowerBound = lowerBoundObj == null ? null : Double.parseDouble(lowerBoundObj.toString());
                Double upperBound = upperBoundObj == null ? null : Double.parseDouble(upperBoundObj.toString());
                if (betweenBounds(score, lowerBound, upperBound)) {
                    Object probabilityObj = range.get(ScoringDaemonService.CALIBRATION_PROBABILITY);
                    probability = probabilityObj == null ? null : Double.parseDouble(probabilityObj.toString());
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
        JSONArray bucketRanges = (JSONArray) model.get(ScoringDaemonService.BUCKETS);
        if (bucketRanges != null) {
            for (int i = 0; i < bucketRanges.size(); i++) {
                Double value = probability;
                JSONObject range = (JSONObject) bucketRanges.get(i);
                if (value == null) {
                    value = score;
                }
                // "0 - Probability, 1 - Lift"
                if (Integer.parseInt(range.get(ScoringDaemonService.BUCKETS_TYPE).toString()) == 1) {
                    value = lift;
                }
                Object lowerBoundObj = range.get(ScoringDaemonService.BUCKETS_MINIMUMSCORE);
                Object upperBoundObj = range.get(ScoringDaemonService.BUCKETS_MAXIMUMSCORE);
                Double lowerBound = lowerBoundObj == null ? null : Double.parseDouble(lowerBoundObj.toString());
                Double upperBound = upperBoundObj == null ? null : Double.parseDouble(upperBoundObj.toString());
                if (value != null && betweenBounds(value, lowerBound, upperBound)) {
                    bucket = String.valueOf(range.get(ScoringDaemonService.BUCKETS_NAME));
                    break;
                }
            }
        }

        // bucket into percentiles
        Integer percentile = null;
        JSONArray percentileRanges = (JSONArray) model.get(ScoringDaemonService.PERCENTILE_BUCKETS);
        if (percentileRanges != null) {
            Double topPercentileMaxScore = (Double) 0.0;
            Double bottomPercentileMinScore = (Double) 1.0;
            Integer topPercentile = 100;
            Integer bottomPercentile = 1;
            boolean foundTopPercentileMaxScore = false;
            boolean foundbottomPercentileMinScore = false;
            for (int i = 0; i < percentileRanges.size(); i++) {
                JSONObject range = (JSONObject) percentileRanges.get(i);
                Object minObject = range.get(ScoringDaemonService.PERCENTILE_BUCKETS_MINIMUMSCORE);
                Object maxObject = range.get(ScoringDaemonService.PERCENTILE_BUCKETS_MAXIMUMSCORE);
                Double min = minObject == null ? null : Double.parseDouble(minObject.toString());
                Double max = maxObject == null ? null : Double.parseDouble(maxObject.toString());
                Object percentObj = range.get(ScoringDaemonService.PERCENTILE_BUCKETS_PERCENTILE);
                Integer percent = percentObj == null ? null : Integer.parseInt(percentObj.toString());
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
                    JSONObject range = (JSONObject) percentileRanges.get(i);
                    Object minObject = range.get(ScoringDaemonService.PERCENTILE_BUCKETS_MINIMUMSCORE);
                    Object maxObject = range.get(ScoringDaemonService.PERCENTILE_BUCKETS_MAXIMUMSCORE);
                    Double min = minObject == null ? null : Double.parseDouble(minObject.toString());
                    Double max = maxObject == null ? null : Double.parseDouble(maxObject.toString());
                    Object percentObj = range.get(ScoringDaemonService.PERCENTILE_BUCKETS_PERCENTILE);
                    Integer percent = percentObj == null ? null : Integer.parseInt(percentObj.toString());
                    if (betweenBounds(score, min, max)) {
                        percentile = percent;
                        break;
                    }
                }
            }

        }

        Integer integerScore = (int) (probability != null ? Math.round(probability * 100) : Math.round(score * 100));
        String modelName = modelInfoMap.get(modelGuid).getModelId();
        ScoreOutput result = new ScoreOutput(leadId, bucket, lift, modelName, percentile, probability, score,
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
