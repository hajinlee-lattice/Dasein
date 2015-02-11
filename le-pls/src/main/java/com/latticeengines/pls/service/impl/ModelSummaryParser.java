package com.latticeengines.pls.service.impl;

import java.io.IOException;

import org.springframework.stereotype.Component;

import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.latticeengines.common.exposed.util.CompressionUtils;
import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.domain.exposed.pls.KeyValue;
import com.latticeengines.domain.exposed.pls.ModelSummary;

@Component("modelSummaryParser")
public class ModelSummaryParser {

    public ModelSummary parse(String hdfsPath, String fileContents) {
        JsonElement root = new JsonParser().parse(fileContents);
        JsonObject rootJsonObject = root.getAsJsonObject();
        
        ModelSummary summary = new ModelSummary();
        
        JsonElement modelDetails = rootJsonObject.get("ModelDetails");
        
        if (modelDetails != null) {
            JsonObject modelDetailsJsonObject = modelDetails.getAsJsonObject();
            String lookupId = modelDetailsJsonObject.get("LookupID").getAsString();
            summary.setName(modelDetailsJsonObject.get("Name").getAsString());
            summary.setLookupId(lookupId);
            summary.setTrainingRowCount(modelDetailsJsonObject.get("TrainingLeads").getAsLong());
            summary.setTestRowCount(modelDetailsJsonObject.get("TestingLeads").getAsLong());
            summary.setTotalRowCount(modelDetailsJsonObject.get("TotalLeads").getAsLong());
            summary.setTrainingConversionCount(modelDetailsJsonObject.get("TrainingConversions").getAsLong());
            summary.setTestConversionCount(modelDetailsJsonObject.get("TestingConversions").getAsLong());
            summary.setTotalConversionCount(modelDetailsJsonObject.get("TotalConversions").getAsLong());
            summary.setRocScore(modelDetailsJsonObject.get("RocScore").getAsDouble());
            summary.setConstructionTime(modelDetailsJsonObject.get("ConstructionTime").getAsLong() * 1000);
            summary.setId("ms-" + lookupId.split("\\|")[2]);
        }
        
        KeyValue kv = new KeyValue();
        try {
            kv.setData(CompressionUtils.compressByteArray(fileContents.getBytes()));
            summary.setDetails(kv);
        } catch (IOException e) {
            throw new LedpException(LedpCode.LEDP_18020, new String[] { hdfsPath });
        }
        
        return summary;
    }
    
}
