package com.latticeengines.scoring.runtime.mapreduce;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
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
import com.latticeengines.scoring.util.ModelEvaluationResult;
import com.latticeengines.scoring.util.ScoringMapperPredictUtil;
import com.latticeengines.scoring.util.ScoringMapperTransformUtil;
import com.latticeengines.scoring.util.ScoringMapperValidateUtil;
import com.latticeengines.scoring.util.ValidationResult;

public class EventDataScoringMapper extends Mapper<AvroKey<Record>, NullWritable, NullWritable, NullWritable> {

    private static final Log log = LogFactory.getLog(EventDataScoringMapper.class);
    private static final int THRESHOLD = 10000;
    
    @Override
    public void run(Context context) throws IOException, InterruptedException {
        @SuppressWarnings("deprecation")
        Path[] paths = context.getLocalCacheFiles();
        HashMap<String, JSONObject> models = new HashMap<String, JSONObject>();
        // key: modelID, value: list of lead records
    	HashMap<String, ArrayList<String>> leadInputRecordMap = new HashMap<String, ArrayList<String>>();
    	ArrayList<ModelEvaluationResult> resultList = null;
    	// key: modelGuid, value: modelName
    	HashMap<String, String> modelIdMap = new HashMap<String, String>();
        JSONObject datatype = null;
        
        for (Path p : paths) {
            log.info("files" + p);
            log.info(p.getName());
            if (p.getName().equals("datatype.avsc")) {
            	datatype = ScoringMapperTransformUtil.parseDatatypeFile(p);
            } else if (!p.getName().equals("scoring.py")) {
            	ScoringMapperTransformUtil.parseModelFiles(models, p);
            } else {
                log.info("Localize 'scoring.py'");
            }
        }
    	log.info("the size of the models is " + models.size());
        
    	ValidationResult vr= ScoringMapperValidateUtil.validate(datatype, models);
    	if (!vr.passValidation()) {
    		log.error("ValidationResult is: " + vr);
    		new Exception("validation fails");
    	}
    	
        int n = 0;
        while (context.nextKeyValue()) {
        	n++;
            String record = context.getCurrentKey().datum().toString();
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
        
        ScoringMapperTransformUtil.writeToLeadInputFiles(leadInputRecordMap, THRESHOLD);
        
        String outputPath = context.getConfiguration().get(MapReduceProperty.OUTPUT.name());
        log.info("outputDir: " + outputPath);
        ScoringMapperPredictUtil.evaluate(models);
        resultList = ScoringMapperPredictUtil.processScoreFiles(leadInputRecordMap, models, modelIdMap, THRESHOLD);
        log.info("The size of resultList is: " + resultList.size());
        ScoringMapperPredictUtil.writeToOutputFile(resultList, context.getConfiguration(), outputPath);
    }
}
