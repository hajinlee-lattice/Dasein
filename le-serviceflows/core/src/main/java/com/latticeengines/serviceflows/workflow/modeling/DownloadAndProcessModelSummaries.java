package com.latticeengines.serviceflows.workflow.modeling;

import java.util.Map;

import com.latticeengines.domain.exposed.serviceflows.core.steps.ModelStepConfiguration;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.domain.exposed.pls.AttributeMap;
import com.latticeengines.domain.exposed.pls.ModelSummary;
import com.latticeengines.domain.exposed.pls.ModelSummaryStatus;
import com.latticeengines.domain.exposed.workflow.WorkflowContextConstants;
import com.latticeengines.serviceflows.workflow.core.BaseWorkflowStep;
import com.latticeengines.serviceflows.workflow.core.InternalResourceRestApiProxy;

@Component("downloadAndProcessModelSummaries")
public class DownloadAndProcessModelSummaries extends BaseWorkflowStep<ModelStepConfiguration> {

    @Autowired
    private WaitForDownloadedModelSummaries waitForDownloadedModelSummaries;

    private InternalResourceRestApiProxy proxy = null;

    @SuppressWarnings("unchecked")
    @Override
    public void execute() {
        if (proxy == null) {
            proxy = new InternalResourceRestApiProxy(configuration.getInternalResourceHostPort());
        }

        Map<String, String> modelApplicationIdToEventColumn = getObjectFromContext(MODEL_APP_IDS, Map.class);
        if (modelApplicationIdToEventColumn == null || modelApplicationIdToEventColumn.isEmpty()) {
            throw new LedpException(LedpCode.LEDP_28012);
        }
        Map<String, ModelSummary> eventToModelSummary = waitForDownloadedModelSummaries.wait(configuration,
                modelApplicationIdToEventColumn);
        Map<String, String> eventToModelId = retrieveModelIds(eventToModelSummary);

        AttributeMap attrMap = new AttributeMap();
        attrMap.put("Status", ModelSummaryStatus.INACTIVE.getStatusCode());
        for (String modelId : eventToModelId.values()) {
            proxy.updateModelSummary(modelId, attrMap);

            saveOutputValue(WorkflowContextConstants.Inputs.MODEL_ID.toString(), modelId);
            putStringValueInContext(SCORING_MODEL_ID, modelId);
        }

        putObjectInContext(EVENT_TO_MODELID, eventToModelId);

        for (ModelSummary modelSummary : eventToModelSummary.values()) {
            if (modelSummary.getTotalRowCount() != 0) {
                double avgProbability = (double) modelSummary.getTotalConversionCount()
                        / (double) modelSummary.getTotalRowCount();
                putDoubleValueInContext(MODEL_AVG_PROBABILITY, avgProbability);
            } else {
                log.info("TotalRowCount is 0!");
            }
        }
    }
}
