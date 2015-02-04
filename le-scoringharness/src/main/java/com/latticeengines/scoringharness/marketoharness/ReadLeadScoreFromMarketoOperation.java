package com.latticeengines.scoringharness.marketoharness;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.latticeengines.scoringharness.OutputFileWriter;
import com.latticeengines.scoringharness.cloudmodel.BaseCloudRead;
import com.latticeengines.scoringharness.cloudmodel.BaseCloudResult;
import com.latticeengines.scoringharness.operationmodel.Operation;
import com.latticeengines.scoringharness.operationmodel.ReadLeadScoreOperationSpec;

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

            outputResult.additionalFields.add(result.jsonObjectResults.toString());
        } catch (Exception e) {
            outputResult.additionalFields.add(e.getMessage());
        }

        output.write(outputResult);
    }

    @Override
    public String getName() {
        return "readscore";
    }
}
