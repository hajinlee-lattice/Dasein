package com.latticeengines.scoring.util;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
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

import com.latticeengines.common.exposed.util.AvroUtils;
import com.latticeengines.common.exposed.util.HdfsUtils;
import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.domain.exposed.scoring.ScoreOutput;
import com.latticeengines.scoring.runtime.mapreduce.EventDataScoringMapper;

public class ScoringMapperPredictUtil {

    private static final Log log = LogFactory.getLog(EventDataScoringMapper.class);

    public static final String CALIBRATION = "Calibration";
    public static final String CALIBRATION_MAXIMUMSCORE = "MaximumScore";
    public static final String CALIBRATION_MINIMUMSCORE = "MinimumScore";
    public static final String CALIBRATION_PROBABILITY = "Probability";
    public static final String AVERAGE_PROBABILITY = "AverageProbability";
    public static final String BUCKETS = "Buckets";
    public static final String BUCKETS_TYPE = "Type";
    public static final String BUCKETS_MAXIMUMSCORE = "Maximum";
    public static final String BUCKETS_MINIMUMSCORE = "Minimum";
    public static final String BUCKETS_NAME = "Name";
    public static final String PERCENTILE_BUCKETS = "PercentileBuckets";
    public static final String PERCENTILE_BUCKETS_PERCENTILE = "Percentile";
    public static final String PERCENTILE_BUCKETS_MINIMUMSCORE = "MinimumScore";
    public static final String PERCENTILE_BUCKETS_MAXIMUMSCORE = "MaximumScore";
    private static final String SCORING_OUTPUT_PREFIX = "scoringoutputfile-";
    private static final String AVRO_FILE_SUFFIX = ".avro";

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

        String fileName = UUID.randomUUID() + AVRO_FILE_SUFFIX;
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
            // key: leadID, value: raw score
            Map<String, Double> scores = new HashMap<String, Double>();
            long value = modelInfoMap.get(modelGuid).getLeadNumber();
            JSONObject model = models.get(modelGuid);
            long remain = value / leadFileThreshold;
            for (int i = 0; i <= remain; i++) {
                readScoreFile(modelGuid, i, scores);
            }
            Set<String> keySet = scores.keySet();
            for (String key : keySet) {
                ScoreOutput result = getResult(modelInfoMap, modelGuid, key, model, scores.get(key));
                resultList.add(result);
            }
        }
        return resultList;
    }

    private static void readScoreFile(String modelGuid, int index, Map<String, Double> scores) throws IOException {

        String fileName = modelGuid + SCORING_OUTPUT_PREFIX + index + ".txt";
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
            scores.put(splitLine[0], Double.parseDouble(splitLine[1]));
        }
    }

    private static ScoreOutput getResult(Map<String, ModelAndLeadInfo.ModelInfo> modelInfoMap, String modelGuid,
            String leadId, JSONObject model, double score) {
        Double probability = null;

        // perform calibration
        JSONArray calibrationRanges = (JSONArray) model.get(CALIBRATION);
        if (calibrationRanges != null) {
            for (int i = 0; i < calibrationRanges.size(); i++) {
                JSONObject range = (JSONObject) calibrationRanges.get(i);
                Object lowerBoundObj = range.get(CALIBRATION_MINIMUMSCORE);
                Object upperBoundObj = range.get(CALIBRATION_MAXIMUMSCORE);
                Double lowerBound = lowerBoundObj == null ? null : Double.parseDouble(lowerBoundObj.toString());
                Double upperBound = upperBoundObj == null ? null : Double.parseDouble(upperBoundObj.toString());
                if (betweenBounds(score, lowerBound, upperBound)) {
                    Object probabilityObj = range.get(CALIBRATION_PROBABILITY);
                    probability = probabilityObj == null ? null : Double.parseDouble(probabilityObj.toString());
                    break;
                }
            }
        }

        Object averageProbabilityObj = model.get(AVERAGE_PROBABILITY);
        Double averageProbability = averageProbabilityObj == null ? null : Double.parseDouble(averageProbabilityObj
                .toString());
        Double lift = averageProbability != null && averageProbability != 0 ? (double) probability / averageProbability
                : null;

        // perform bucketing
        String bucket = null;
        JSONArray bucketRanges = (JSONArray) model.get(BUCKETS);
        if (bucketRanges != null) {
            for (int i = 0; i < bucketRanges.size(); i++) {
                Double value = probability;
                JSONObject range = (JSONObject) bucketRanges.get(i);
                if (value == null) {
                    value = score;
                }
                // "0 - Probability, 1 - Lift"
                if (Integer.parseInt(range.get(BUCKETS_TYPE).toString()) == 1) {
                    value = lift;
                }
                Object lowerBoundObj = range.get(BUCKETS_MINIMUMSCORE);
                Object upperBoundObj = range.get(BUCKETS_MAXIMUMSCORE);
                Double lowerBound = lowerBoundObj == null ? null : Double.parseDouble(lowerBoundObj.toString());
                Double upperBound = upperBoundObj == null ? null : Double.parseDouble(upperBoundObj.toString());
                if (value != null && betweenBounds(value, lowerBound, upperBound)) {
                    bucket = String.valueOf(range.get(BUCKETS_NAME));
                    break;
                }
            }
        }

        // bucket into percentiles
        Integer percentile = null;
        JSONArray percentileRanges = (JSONArray) model.get(PERCENTILE_BUCKETS);
        if (percentileRanges != null) {
            Double topPercentileMaxScore = (Double) 0.0;
            Double bottomPercentileMinScore = (Double) 1.0;
            Integer topPercentile = 100;
            Integer bottomPercentile = 1;
            boolean foundTopPercentileMaxScore = false;
            boolean foundbottomPercentileMinScore = false;
            for (int i = 0; i < percentileRanges.size(); i++) {
                JSONObject range = (JSONObject) percentileRanges.get(i);
                Object minObject = range.get(PERCENTILE_BUCKETS_MINIMUMSCORE);
                Object maxObject = range.get(PERCENTILE_BUCKETS_MAXIMUMSCORE);
                Double min = minObject == null ? null : Double.parseDouble(minObject.toString());
                Double max = maxObject == null ? null : Double.parseDouble(maxObject.toString());
                Object percentObj = range.get(PERCENTILE_BUCKETS_PERCENTILE);
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
                    Object minObject = range.get(PERCENTILE_BUCKETS_MINIMUMSCORE);
                    Object maxObject = range.get(PERCENTILE_BUCKETS_MAXIMUMSCORE);
                    Double min = minObject == null ? null : Double.parseDouble(minObject.toString());
                    Double max = maxObject == null ? null : Double.parseDouble(maxObject.toString());
                    Object percentObj = range.get(PERCENTILE_BUCKETS_PERCENTILE);
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
