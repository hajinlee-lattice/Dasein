package com.latticeengines.scoringharness.marketoharness;

import java.io.IOException;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.latticeengines.scoringharness.OutputFileWriter;
import com.latticeengines.scoringharness.cloudmodel.BaseCloudResult;
import com.latticeengines.scoringharness.cloudmodel.BaseCloudUpdate;
import com.latticeengines.scoringharness.operationmodel.Operation;
import com.latticeengines.scoringharness.operationmodel.WriteLeadOperationSpec;

@Component
public class WriteLeadToMarketoOperation extends Operation<WriteLeadOperationSpec> {
    @Autowired
    private MarketoHarness harness;

    @Autowired
    private MarketoProperties properties;

    @Override
    public void execute() {
        ObjectNode json = spec.object;
        json.put("email", spec.externalId);
        json.put(properties.getScoreField(), (String) null);

        BaseCloudUpdate update = new BaseCloudUpdate(MarketoHarness.OBJECT_TYPE_LEAD,
                MarketoHarness.OBJECT_ACTION_CREATE_OR_UPDATE);
        update.addRow(json.toString());

        OutputFileWriter.Result outputResult = new OutputFileWriter.Result();
        outputResult.offsetMilliseconds = spec.offsetMilliseconds;
        outputResult.operation = getName();
        outputResult.additionalFields.add(spec.externalId);

        try {
            String accessToken = harness.getAccessToken();
            BaseCloudResult result = harness.updateObjects(accessToken, update);
            outputResult.isSuccess = result.isSuccess;
            addToCache(result);
            if (!result.isSuccess) {
                throw new RuntimeException(String.format("Failed to write the lead %s to Marketo: %s", spec.object,
                        result.errorMessage));
            }
        } catch (Exception e) {
            outputResult.additionalFields.add(e.getMessage());
        }

        output.write(outputResult);
    }

    private void addToCache(BaseCloudResult result) throws JsonProcessingException, IOException {
        String jsonString = result.jsonObjectResults.get(0);
        ObjectNode json = (ObjectNode) new ObjectMapper().readTree(jsonString);
        MarketoLeadCache.instance().put(spec.externalId, json.get("id").asText());
    }

    @Override
    public String getName() {
        return "write";
    }
}
