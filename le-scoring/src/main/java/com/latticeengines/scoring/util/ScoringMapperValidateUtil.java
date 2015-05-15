package com.latticeengines.scoring.util;

import java.util.HashMap;
import java.util.Set;

import org.json.simple.JSONArray;
import org.json.simple.JSONObject;

public class ScoringMapperValidateUtil {
	
    private static final String INPUT_COLUMN_METADATA = "InputColumnMetadata";
    private static final String INPUT_COLUMN_METADATA_NAME = "Name";
    private static final String INPUT_COLUMN_METADATA_PURPOSE = "Purpose";
    private static final String INPUT_COLUMN_METADATA_VALUETYPE = "ValueType";
	
	public static void validate(JSONObject datatype, boolean datatypeFileProvided, HashMap<String, JSONObject> models)
	{
		if (!datatypeFileProvided) {
			new Exception("datatype file is not provided");
		}
		// datatype validation
		Set<String> keySet = datatype.keySet();
		for (String key : keySet) {
			Long datatypeVal = (Long) datatype.get(key);
			if (datatypeVal != 0 && datatypeVal != 1) {
				new Exception("datatype is not supported");
			}
		}
		
		Set<String> modelIDs = models.keySet();
		for (String modelID : modelIDs) {
			JSONArray metadata = (JSONArray) models.get(modelID).get(INPUT_COLUMN_METADATA);
			validate(datatype, modelID, metadata);
		}
	}
	
	public static void validate(JSONObject datatype, String modelID, JSONArray metadata)
	{
		if (metadata != null) {
			for (int i = 0; i < metadata.size(); i++) {
				JSONObject obj = (JSONObject) metadata.get(i);
				String name = (String) obj.get(INPUT_COLUMN_METADATA_NAME);
				Long purpose = (Long) obj.get(INPUT_COLUMN_METADATA_PURPOSE);
				Long type = (Long) obj.get(INPUT_COLUMN_METADATA_VALUETYPE);
				//need to verify with Haitao
				if (purpose == 3) {
					continue;
				}
				Long valueType = (Long) obj.get(INPUT_COLUMN_METADATA_VALUETYPE);
				if (!datatype.containsKey(name)) {
					new Exception("missing required column " + name);
				}
				if (datatype.get(name) != type) {
					new Exception("Tye dataype does not match " + name);
				}
			}
		}
	}
}
