package com.latticeengines.scoring.util;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Set;
import java.util.UUID;

import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.specific.SpecificDatumWriter;
import org.apache.commons.io.FileUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;

import com.latticeengines.common.exposed.util.HdfsUtils;
import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.domain.exposed.scoring.ScoreOutput;
import com.latticeengines.scoring.runtime.mapreduce.EventDataScoringMapper;

public class ScoringMapperPredictUtil {

    private static final Log log = LogFactory.getLog(EventDataScoringMapper.class);

    private static final String CALIBRATION = "Calibration";
    private static final String CALIBRATION_MAXIMUMSCORE = "MaximumScore";
    private static final String CALIBRATION_MINIMUMSCORE = "MinimumScore";
    private static final String CALIBRATION_PROBABILITY = "Probability";
    private static final String AVERAGE_PROBABILITY = "AverageProbability";
    private static final String BUCKETS = "Buckets";
    private static final String BUCKETS_TYPE = "Type";
    private static final String BUCKETS_MAXIMUMSCORE = "Maximum";
    private static final String BUCKETS_MINIMUMSCORE = "Minimum";
    private static final String BUCKETS_NAME = "Name";
    private static final String PERCENTILE_BUCKETS = "PercentileBuckets";
    private static final String PERCENTILE_BUCKETS_PERCENTILE = "Percentile";
    private static final String PERCENTILE_BUCKETS_MINIMUMSCORE = "MinimumScore";
    private static final String PERCENTILE_BUCKETS_MAXIMUMSCORE = "MaximumScore";
    private static final String SCORING_OUTPUT_PREFIX = "scoringoutputfile-";

    public static String evaluate(HashMap<String, JSONObject> models) throws IOException, InterruptedException {

        StringBuilder strs = new StringBuilder();
        // spawn python
        Set<String> modelGuidSet = models.keySet();
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
            log.info(line);
        }
        in.close();
        while ((line = err.readLine()) != null) {
            errors.append(line);
            strs.append(line);
            log.error(line);
        }
        err.close();
        p.waitFor();

        if (errors.length() != 0) {
            throw new LedpException(LedpCode.LEDP_200011, new String[] { errors.toString() });
        }

        return strs.toString();
    }

    public static void writeToOutputFile(ArrayList<ScoreOutput> resultList, Configuration yarnConfiguration,
            String outputPath) throws Exception {

        String fileName = UUID.randomUUID() + ".avro";
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

    public static ArrayList<ScoreOutput> processScoreFiles(HashMap<String, ArrayList<String>> leadInputRecordMap,
            HashMap<String, JSONObject> models, HashMap<String, String> modelIdMap, long threshold) throws IOException {

        Set<String> modelGuidSet = leadInputRecordMap.keySet();
        // list of HashMap<leadID: score>
        ArrayList<ScoreOutput> resultList = new ArrayList<ScoreOutput>();
        for (String modelGuid : modelGuidSet) {
            log.info("modelGuid is " + modelGuid);
            // key: leadID, value: raw score
            HashMap<String, Double> scores = new HashMap<String, Double>();
            int value = leadInputRecordMap.get(modelGuid).size();
            JSONObject model = models.get(modelGuid);
            int remain = (int) (value / threshold);
            for (int i = 0; i <= remain; i++) {
                readScoreFile(modelGuid, i, scores);
            }
            Set<String> keySet = scores.keySet();
            for (String key : keySet) {
                ScoreOutput result = getResult(modelIdMap, modelGuid, key, model, scores.get(key));
                resultList.add(result);
            }
        }
        return resultList;
    }

    private static void readScoreFile(String modelGuid, int index, HashMap<String, Double> scores) throws IOException {

        String fileName = modelGuid + SCORING_OUTPUT_PREFIX + index + ".txt";
        File f = new File(fileName);
        if (!f.exists()) {
            throw new LedpException(LedpCode.LEDP_200012, new String[] { fileName });
        }

        List<String> lines = FileUtils.readLines(f);
        for (String line : lines) {
            String[] splitLine = line.split(",");
            if (splitLine.length != 2) {
                throw new LedpException(LedpCode.LEDP_200013);
            }
            scores.put(splitLine[0], Double.parseDouble(splitLine[1]));
        }
    }

    private static ScoreOutput getResult(HashMap<String, String> modelIdMap, String modelGuid, String leadId,
            JSONObject model, double score) {
        Double probability = null;

        // perform calibration
        JSONArray calibrationRanges = (JSONArray) model.get(CALIBRATION);
        if (calibrationRanges != null) {
            for (int i = 0; i < calibrationRanges.size(); i++) {
                JSONObject range = (JSONObject) calibrationRanges.get(i);
                Double lowerBound = (Double) range.get(CALIBRATION_MINIMUMSCORE);
                Double upperBound = (Double) range.get(CALIBRATION_MAXIMUMSCORE);
                if (betweenBounds(score, lowerBound, upperBound)) {
                    Double probabilityObj = (Double) range.get(CALIBRATION_PROBABILITY);
                    probability = probabilityObj != null ? (Double) probabilityObj.doubleValue() : null;
                    break;
                }
            }
        }

        Double averageProbability = (Double) model.get(AVERAGE_PROBABILITY);
        Double lift = averageProbability != null && averageProbability != 0 ? (double) probability / averageProbability
                : null;

        // perform bucketing
        String bucket = null;
        JSONArray bucketRanges = (JSONArray) model.get(BUCKETS);
        if (bucketRanges != null) {
            for (int i = 0; i < bucketRanges.size(); i++) {
                Double value = probability;
                JSONObject range = (JSONObject) bucketRanges.get(i);
                // TODO need to Double check with Haitao/Ron about uncalibration
                if (value == null) {
                    value = score;
                }
                // "0 - Probability, 1 - Lift"
                if (((Long) range.get(BUCKETS_TYPE)).intValue() == 1) {
                    value = lift;
                }
                Double lowerBound = (Double) range.get(BUCKETS_MINIMUMSCORE);
                Double upperBound = (Double) range.get(BUCKETS_MAXIMUMSCORE);
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
                Double min = (Double) range.get(PERCENTILE_BUCKETS_MINIMUMSCORE);
                Double max = (Double) range.get(PERCENTILE_BUCKETS_MAXIMUMSCORE);
                Integer percent = ((Long) range.get(PERCENTILE_BUCKETS_PERCENTILE)).intValue();
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
                    Double min = (Double) range.get(PERCENTILE_BUCKETS_MINIMUMSCORE);
                    Double max = (Double) range.get(PERCENTILE_BUCKETS_MAXIMUMSCORE);
                    Integer percent = ((Long) range.get(PERCENTILE_BUCKETS_PERCENTILE)).intValue();
                    if (betweenBounds(score, min, max)) {
                        percentile = percent;
                        break;
                    }
                }
            }

        }

        Integer integerScore = (int) (probability != null ? Math.round(probability * 100) : Math.round(score * 100));
        String modelName = modelIdMap.get(modelGuid);
        ScoreOutput result = new ScoreOutput(leadId, bucket, lift, modelName, percentile, probability, score,
                integerScore);
        return result;

    }

    private static boolean betweenBounds(double value, Double lowerInclusive, Double upperExclusive) {
        return (lowerInclusive == null || value >= lowerInclusive)
                && (upperExclusive == null || value < upperExclusive);
    }

    public static void main(String[] args) {

        // String hdfs =
        // "/user/s-analytics/customers/Nutanix/scoring/ScoringCommandProcessorTestNG_LeadsTable/scores/15e01328-9e73-4977-9298-259e68632bce.avro";
        // List<GenericRecord> list = AvroUtils.getData(new Configuration(), new
        // Path(hdfs));
        // for (GenericRecord ele : list) {
        // System.out.println(ele.toString());
        // }

        ScoreOutput result1 = new ScoreOutput("18f446f1-747b-461e-9160-c995c3876ed4", "Highest",
                4.88519256666, "modelID", 100, 0.05822784810126582, 0.0777755757027, 6);
        ArrayList<ScoreOutput> resultList = new ArrayList<>();
        resultList.add(result1);
        String fileName = "/Users/ygao/test/test.avro";
        File outputFile = new File(fileName);
        DatumWriter<ScoreOutput> userDatumWriter = new SpecificDatumWriter<>();
        DataFileWriter<ScoreOutput> dataFileWriter = new DataFileWriter<>(
                userDatumWriter);

        for (int i = 0; i < resultList.size(); i++) {
            ScoreOutput result = resultList.get(i);
            try {
                if (i == 0) {
                    dataFileWriter.create(result.getSchema(), outputFile);
                }
                dataFileWriter.append(result);
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        try {
            dataFileWriter.close();
        } catch (IOException e) {
            e.printStackTrace();
        }

        SpecificDatumReader<ScoreOutput> reader = new SpecificDatumReader<ScoreOutput>(
                ScoreOutput.class);
        DataFileReader<ScoreOutput> dataFileReader = null;
        try {
            dataFileReader = new DataFileReader<ScoreOutput>(outputFile, reader);
        } catch (IOException e) {
            e.printStackTrace();
        }
        ScoreOutput result = null;
        while (dataFileReader.hasNext()) {
            result = dataFileReader.next();
            System.out.println(result);
        }

        // delete the temp folder and the temp file
        try {
            dataFileReader.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
