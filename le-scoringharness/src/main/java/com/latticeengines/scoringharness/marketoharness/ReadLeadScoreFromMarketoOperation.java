package com.latticeengines.scoringharness.marketoharness;

import java.util.ArrayList;
import java.util.List;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.latticeengines.scoringharness.OutputFileWriter;
import com.latticeengines.scoringharness.cloudmodel.BaseCloudRead;
import com.latticeengines.scoringharness.cloudmodel.BaseCloudResult;
import com.latticeengines.scoringharness.operationmodel.Operation;
import com.latticeengines.scoringharness.operationmodel.ReadLeadScoreOperationSpec;
import com.latticeengines.scoringharness.util.JsonUtil;

@Component
public class ReadLeadScoreFromMarketoOperation extends Operation<ReadLeadScoreOperationSpec> {
    @Autowired
    private MarketoHarness harness;

    @Autowired
    private MarketoProperties properties;

    @Override
    public void execute() {
        OutputFileWriter.Result outputResult = new OutputFileWriter.Result();
        outputResult.offsetMilliseconds = spec.offsetMilliseconds;
        outputResult.operation = getName();
        outputResult.additionalFields.add(spec.externalId);

        try {
            String id = MarketoLeadCache.instance().get(spec.externalId);
            if (id == null) {
                throw new RuntimeException("Previously failed, or never, wrote out a lead with external id "
                        + spec.externalId);
            }

            BaseCloudRead read = new BaseCloudRead(MarketoHarness.OBJECT_TYPE_LEAD, id);
            read.fields.add(properties.getScoreField());
            if (spec.additionalFields != null) {
                read.fields.addAll(spec.additionalFields);
            }

            String accessToken = harness.getAccessToken();
            BaseCloudResult result = harness.getObjects(accessToken, read);
            outputResult.isSuccess = result.isSuccess;
            if (!result.isSuccess) {
                throw new RuntimeException(String.format("Failed to read the lead %s from Marketo: %s",
                        spec.externalId, result.errorMessage));
            }

            outputResult.additionalFields.add(getScoreFields(result.results).toString());
        } catch (Exception e) {
            outputResult.additionalFields.add(e.getMessage());
        }

        output.write(outputResult);
    }

    /**
     * Peel out only the fields that we requested
     */
    private JsonNode getScoreFields(ArrayNode results) {
        if (results.size() == 0) {
            throw new RuntimeException(String.format("Invalid output returned from Marketo '%s'", results));
        }

        // Collect requested fields
        List<String> requestedFields = new ArrayList<String>();
        requestedFields.add(properties.getScoreField());
        if (spec.additionalFields != null) {
            requestedFields.addAll(spec.additionalFields);
        }

        // Create new object node with just those fields
        ObjectNode toReturn = JsonUtil.createObject();
        for (String field : requestedFields) {
            JsonNode jsonField = results.get(0).get(field);
            if (jsonField == null) {
                throw new RuntimeException(String.format("Requested field %s was not provided in output from Marketo",
                        field));
            }
            toReturn.put(field, jsonField.asText());
        }
        return toReturn;
    }

    @Override
    public String getName() {
        return "readscore";
    }
}
