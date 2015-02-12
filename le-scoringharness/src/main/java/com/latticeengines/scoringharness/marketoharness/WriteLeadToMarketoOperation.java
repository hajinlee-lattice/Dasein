package com.latticeengines.scoringharness.marketoharness;

import java.util.UUID;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.latticeengines.scoringharness.OutputFileWriter;
import com.latticeengines.scoringharness.cloudmodel.BaseCloudResult;
import com.latticeengines.scoringharness.cloudmodel.BaseCloudUpdate;
import com.latticeengines.scoringharness.operationmodel.Operation;
import com.latticeengines.scoringharness.operationmodel.WriteLeadOperationSpec;

@Component
public class WriteLeadToMarketoOperation extends Operation<WriteLeadOperationSpec> {
    private static final Log log = LogFactory.getLog(new Object() {
    }.getClass().getEnclosingClass());

    @Autowired
    private MarketoHarness harness;

    @Autowired
    private MarketoProperties properties;

    @Override
    public void execute() {
        ObjectNode json = spec.object;
        json.put("email", spec.externalId);
        json.put(properties.getScoreField(), (String) null);
        json.put(properties.getGuidField(), UUID.randomUUID().toString());

        BaseCloudUpdate update = new BaseCloudUpdate(MarketoHarness.OBJECT_TYPE_LEAD,
                MarketoHarness.OBJECT_ACTION_CREATE_OR_UPDATE);
        update.addRow(json);

        OutputFileWriter.Result outputResult = new OutputFileWriter.Result();
        outputResult.offsetMilliseconds = spec.offsetMilliseconds;
        outputResult.operation = getName();
        outputResult.additionalFields.add(spec.externalId);

        try {
            BaseCloudResult result = harness.updateObjects(update);
            outputResult.isSuccess = result.isSuccess;
            addToCache(result);
            if (!result.isSuccess) {
                throw new RuntimeException(String.format("Failed to write the lead %s to Marketo: %s", spec.object,
                        result.errorMessage));
            }
        } catch (Exception e) {
            log.error(String.format("Failed to write lead %s to Marketo", spec.externalId), e);
            outputResult.additionalFields.add(e.getMessage());
        }

        output.write(outputResult);
    }

    private void addToCache(BaseCloudResult result) {
        JsonNode json = result.results.get(0);
        MarketoLeadCache.instance().put(spec.externalId, json.get("id").asText());
    }

    @Override
    public String getName() {
        return "write";
    }
}
