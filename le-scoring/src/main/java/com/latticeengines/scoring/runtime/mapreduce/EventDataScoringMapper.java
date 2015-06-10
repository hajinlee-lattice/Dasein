package com.latticeengines.scoring.runtime.mapreduce;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

import org.apache.avro.generic.GenericData.Record;
import org.apache.avro.mapred.AvroKey;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Mapper;
import org.json.simple.JSONObject;
import org.json.simple.parser.ParseException;

import com.latticeengines.dataplatform.exposed.mapreduce.MapReduceProperty;
import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.scoring.util.ModelEvaluationResult;
import com.latticeengines.scoring.util.ScoringMapperPredictUtil;
import com.latticeengines.scoring.util.ScoringMapperTransformUtil;
import com.latticeengines.scoring.util.ScoringMapperValidateUtil;
import com.latticeengines.scoring.util.DatatypeValidationResult;

public class EventDataScoringMapper extends Mapper<AvroKey<Record>, NullWritable, NullWritable, NullWritable> {

    private static final Log log = LogFactory.getLog(EventDataScoringMapper.class);
    private static final long THRESHOLD = 10000L;

    private JSONObject processLocalizedFiles(Path[] paths, HashSet<String> modelIDs, HashMap<String, String> modelIdMap, HashMap<String, JSONObject> models) throws IOException, ParseException {
        
        JSONObject datatype = null;
        boolean scoringScriptProvided = false;
        boolean datatypeFileProvided = false;
        
        for (Path p : paths) {
            log.info("files" + p);
            log.info(p.getName());
            if (p.getName().equals("datatype.avsc")) {
                datatypeFileProvided = true;
                datatype = ScoringMapperTransformUtil.parseDatatypeFile(p);
            } else if (p.getName().equals("scoring.py")) {
                scoringScriptProvided = true;
            } else {
                String modelGuid = p.getName();
                // if the model is selected by this request, parse that particular model
                for (Iterator<String> i = modelIDs.iterator(); i.hasNext();) {
                    String modelId = i.next();
                    if (modelId.contains(modelGuid)) {
                        ScoringMapperTransformUtil.parseModelFiles(models, p);
                        modelIdMap.put(modelGuid, modelId);
                        modelIDs.remove(modelId);
                        break;
                    }
                }
            }
        }
        
        // print out the modelIdMap
        for (String modelID : modelIdMap.keySet()) {
            log.info(modelID + ": " + modelIdMap.get(modelID));
        }

        log.info("the size of the models is " + models.size());
        
        ScoringMapperValidateUtil.validateLocalizedFiles(scoringScriptProvided, datatypeFileProvided, modelIDs);
        
        return datatype;
    }
    
    
    @Override
    public void run(Context context) throws IOException, InterruptedException {
        @SuppressWarnings("deprecation")
        Path[] paths = context.getLocalCacheFiles();
        // key: modelGuid, value: modelId
        HashMap<String, String> modelIdMap = new HashMap<String, String>();
        // key: modelGuid, value: model contents
        HashMap<String, JSONObject> models = new HashMap<String, JSONObject>();
        ArrayList<ModelEvaluationResult> resultList = null;
        JSONObject datatype = null;
        boolean scoringScriptProvided = false;
        boolean datatypeFileProvided = false;

        try {

            ArrayList<String> leadList = new ArrayList<String>();
            while (context.nextKeyValue()) {
                String lead = context.getCurrentKey().datum().toString();
                leadList.add(lead);
            }

            // Preprocess the leads
            HashSet<String> modelIDs = ScoringMapperTransformUtil.preprocessLeads(leadList);
            for (String str : modelIDs) {
                log.info("the current str is:");
                log.info(str);
            }

            // key: modelGuid, value: list of lead records
            HashMap<String, ArrayList<String>> leadInputRecordMap = new HashMap<String, ArrayList<String>>();

            for (Path p : paths) {
                log.info("files" + p);
                log.info(p.getName());
                if (p.getName().equals("datatype.avsc")) {
                    datatypeFileProvided = true;
                    datatype = ScoringMapperTransformUtil.parseDatatypeFile(p);
                } else if (p.getName().equals("scoring.py")) {
                    scoringScriptProvided = true;
                } else {
                    String modelGuid = p.getName();
                    // if the model is selected by this request, parse that particular model
                    for (Iterator<String> i = modelIDs.iterator(); i.hasNext();) {
                        String modelId = i.next();
                        if (modelId.contains(modelGuid)) {
                            ScoringMapperTransformUtil.parseModelFiles(models, p);
                            modelIdMap.put(modelGuid, modelId);
                            modelIDs.remove(modelId);
                            break;
                        }
                    }
                }
            }

            // print out the modelIdMap
            for (String modelID : modelIdMap.keySet()) {
                log.info(modelID + ": " + modelIdMap.get(modelID));
            }

            log.info("the size of the models is " + models.size());
            
            ScoringMapperValidateUtil.validateLocalizedFiles(scoringScriptProvided, datatypeFileProvided, modelIDs);
            
            ScoringMapperValidateUtil.validateDatatype(datatype, models);

            int numberOfRecords = 0;
            for (String record : leadList) {
                numberOfRecords++;
                ScoringMapperTransformUtil.manipulateLeadFile(leadInputRecordMap, models, modelIdMap, record);
            }
            log.info("number of lead records is " + numberOfRecords);
            
            Set<String> keys = leadInputRecordMap.keySet();
            int totalRecordSize = 0;
            for (String key : keys) {
                totalRecordSize += leadInputRecordMap.get(key).size();
            }
            log.info("The number of transformed lead record is: " + totalRecordSize);

            if (numberOfRecords != totalRecordSize) {
                throw new LedpException(LedpCode.LEDP_200010, new String[] { String.valueOf(numberOfRecords), String.valueOf(totalRecordSize) });
            }

            Long leadFileThreshold = context.getConfiguration().getLong(ScoringProperty.LEAD_FILE_THRESHOLD.name(),
                    THRESHOLD);
            ScoringMapperTransformUtil.writeToLeadInputFiles(leadInputRecordMap, leadFileThreshold);

            ScoringMapperPredictUtil.evaluate(models);
            resultList = ScoringMapperPredictUtil.processScoreFiles(leadInputRecordMap, models, modelIdMap,
                    leadFileThreshold);
            log.info("The size of resultList is: " + resultList.size());
            if (numberOfRecords != resultList.size()) {
                throw new LedpException(LedpCode.LEDP_20009, new String[] { String.valueOf(totalRecordSize), String.valueOf(resultList.size()) });
            }
            
            String outputPath = context.getConfiguration().get(MapReduceProperty.OUTPUT.name());
            log.info("outputDir: " + outputPath);
            ScoringMapperPredictUtil.writeToOutputFile(resultList, context.getConfiguration(), outputPath);
            
        } catch (Exception e) {
            log.info(String.format(
                    "Failure Step=Scoring Mapper Failure Message=%s Failure Cause=%s Failure StackTrace=%s", //
                    e.getMessage(), e.getCause(), e.getStackTrace()));
        }
    }
}
