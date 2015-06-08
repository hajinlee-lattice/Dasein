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

import com.latticeengines.dataplatform.exposed.mapreduce.MapReduceProperty;
import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.scoring.util.ModelEvaluationResult;
import com.latticeengines.scoring.util.ScoringMapperPredictUtil;
import com.latticeengines.scoring.util.ScoringMapperTransformUtil;
import com.latticeengines.scoring.util.ScoringMapperValidateUtil;
import com.latticeengines.scoring.util.ValidationResult;

public class EventDataScoringMapper extends Mapper<AvroKey<Record>, NullWritable, NullWritable, NullWritable> {

    private static final Log log = LogFactory.getLog(EventDataScoringMapper.class);
    private static final long THRESHOLD = 10000L;

    @Override
    public void run(Context context) throws IOException, InterruptedException {
        @SuppressWarnings("deprecation")
        Path[] paths = context.getLocalCacheFiles();

        // key: modelGuid, value: modelName
        HashMap<String, String> modelIdMap = new HashMap<String, String>();

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
            HashSet<String> modelNames = ScoringMapperTransformUtil.preprocessLeads(leadList);
            for (String str : modelNames) {
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
                    // this is a model.json file
                    String modelID = p.getName();
                    // if the model is selected by this request, parse that
                    // particular model
                    for (Iterator<String> i = modelNames.iterator(); i.hasNext();) {
                        String modelName = i.next();
                        if (modelName.contains(modelID)) {
                            ScoringMapperTransformUtil.parseModelFiles(models, p);
                            modelIdMap.put(modelID, modelName);
                            modelNames.remove(modelName);
                            break;
                        }
                    }
                }
            }

            // print out the modelIdMap
            for (String modelID : modelIdMap.keySet()) {
                log.info(modelID + ": " + modelIdMap.get(modelID));
            }

            // check whether if there is any required model that is not
            // localized
            if (!modelNames.isEmpty()) {
                ArrayList<String> missingModelsNames = new ArrayList<String>();
                for (String modelName : modelNames) {
                    missingModelsNames.add(modelName + " ");
                }
                throw new LedpException(LedpCode.LEDP_20007, missingModelsNames.toArray(new String[missingModelsNames
                        .size()]));
            }
            log.info("the size of the models is " + models.size());

            if (!scoringScriptProvided) {
                throw new LedpException(LedpCode.LEDP_20002);
            }

            if (!datatypeFileProvided) {
                throw new LedpException(LedpCode.LEDP_20006);
            }

            ValidationResult vr = ScoringMapperValidateUtil.validate(datatype, models);
            if (!vr.passValidation()) {
                log.error("ValidationResult is: " + vr);
                throw new LedpException(LedpCode.LEDP_20001, new String[] { vr.toString() });
            }

            int n = 0;
            for (String record : leadList) {
                n++;
                log.info("the record is " + record);
                ScoringMapperTransformUtil.manipulateLeadFile(leadInputRecordMap, models, modelIdMap, record);
            }

            log.info("number of records is " + n);
            Set<String> keys = leadInputRecordMap.keySet();
            int i = 0;
            for (String key : keys) {
                i += leadInputRecordMap.get(key).size();
            }
            log.info("The number of leads is: " + i);

            // TODO verify the number of leads??

            Long leadFileThreshold = context.getConfiguration().getLong(ScoringProperty.LEAD_FILE_THRESHOLD.name(),
                    THRESHOLD);
            ScoringMapperTransformUtil.writeToLeadInputFiles(leadInputRecordMap, leadFileThreshold);

            ScoringMapperPredictUtil.evaluate(models);
            resultList = ScoringMapperPredictUtil.processScoreFiles(leadInputRecordMap, models, modelIdMap,
                    leadFileThreshold);
            log.info("The size of resultList is: " + resultList.size());
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
