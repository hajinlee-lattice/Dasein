package com.latticeengines.scoring.util;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Hashtable;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;

import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.scoring.runtime.mapreduce.EventDataScoringMapper;

public class ScoringMapperValidateUtil {

    private static final Log log = LogFactory.getLog(EventDataScoringMapper.class);
    
    private static final String INPUT_COLUMN_METADATA = "InputColumnMetadata";
    private static final String INPUT_COLUMN_METADATA_NAME = "Name";
    private static final String INPUT_COLUMN_METADATA_PURPOSE = "Purpose";
    private static final String INPUT_COLUMN_METADATA_VALUETYPE = "ValueType";

    @SuppressWarnings("unchecked")
    public static void validateDatatype(JSONObject datatype, HashMap<String, JSONObject> models) {

        ArrayList<String> datatypeFailures = new ArrayList<String>();

        Hashtable<String, ArrayList<String>> modelFailures = new Hashtable<String, ArrayList<String>>();

        DatatypeValidationResult vf = new DatatypeValidationResult(datatypeFailures, modelFailures);

        // datatype validation
        if (datatype == null) {
            datatypeFailures.add("Datatype file is not provided. ");
            log.error("ValidationResult is: " + vf);
            throw new LedpException(LedpCode.LEDP_20001, new String[] { vf.toString() });
        }

        if (models == null) {
            datatypeFailures.add("Models are not provided. ");
            log.error("ValidationResult is: " + vf);
            throw new LedpException(LedpCode.LEDP_20001, new String[] { vf.toString() });
        }

        Set<String> keySet = datatype.keySet();
        for (String key : keySet) {
            Long datatypeVal = (Long) datatype.get(key);
            if (datatypeVal != 0 && datatypeVal != 1) {
                String msg = String.format("Column %s contains unknown datatype: %d ", key, datatypeVal);
                datatypeFailures.add(msg);
            }
        }

        // validate the datatype file with the model.json
        Set<String> modelIDs = models.keySet();
        for (String modelID : modelIDs) {
            JSONArray metadata = (JSONArray) models.get(modelID).get(INPUT_COLUMN_METADATA);
            ArrayList<String> msgs = validate(datatype, modelID, metadata);
            if (msgs.size() != 0) {
                modelFailures.put(modelID, msgs);
            }
        }

        if (!vf.passDatatypeValidation()) {
            log.error("ValidationResult is: " + vf);
            throw new LedpException(LedpCode.LEDP_20001, new String[] { vf.toString() });
        }
    }

    public static void validateLocalizedFiles(boolean scoringScriptProvided, boolean datatypeFileProvided, HashSet<String> modelIDs) {

        if (!scoringScriptProvided) {
            throw new LedpException(LedpCode.LEDP_20002);
        }

        if (!datatypeFileProvided) {
            throw new LedpException(LedpCode.LEDP_20006);
        }
        
        // check whether if there is any required model that is not localized
        if (!modelIDs.isEmpty()) {
            ArrayList<String> missingModelsNames = new ArrayList<String>();
            for (String modelId : modelIDs) {
                missingModelsNames.add(modelId + " ");
            }
            throw new LedpException(LedpCode.LEDP_20007, missingModelsNames.toArray(new String[missingModelsNames
                    .size()]));
        }
    }
    
    private static ArrayList<String> validate(JSONObject datatype, String modelID, JSONArray metadata) {
        ArrayList<String> toReturn = new ArrayList<String>();
        if (metadata != null) {
            for (int i = 0; i < metadata.size(); i++) {
                JSONObject obj = (JSONObject) metadata.get(i);
                String name = (String) obj.get(INPUT_COLUMN_METADATA_NAME);
                Long purpose = (Long) obj.get(INPUT_COLUMN_METADATA_PURPOSE);
                Long type = (Long) obj.get(INPUT_COLUMN_METADATA_VALUETYPE);
                // need to verify with Ron
                if (purpose != 3) {
                    continue;
                }
                if (!datatype.containsKey(name)) {
                    String msg = String.format("Missing required column: %s ", name);
                    toReturn.add(msg);
                    continue;
                }
                if (datatype.get(name) != type) {
                    String msg = String.format("%d does not match with %d ", type, datatype.get(name));
                    toReturn.add(msg);
                }
            }
        } else {
            String msg = String.format("%s does not contain %s. ", modelID, INPUT_COLUMN_METADATA);
            toReturn.add(msg);
        }
        return toReturn;
    }
}
