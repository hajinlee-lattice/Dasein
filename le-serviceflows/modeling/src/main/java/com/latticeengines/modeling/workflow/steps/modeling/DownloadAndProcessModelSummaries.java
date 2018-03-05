package com.latticeengines.modeling.workflow.steps.modeling;

import java.util.Map;

import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.domain.exposed.pls.AIModel;
import com.latticeengines.domain.exposed.pls.AttributeMap;
import com.latticeengines.domain.exposed.pls.ModelSummary;
import com.latticeengines.domain.exposed.pls.ModelSummaryStatus;
import com.latticeengines.domain.exposed.pls.RatingModel;
import com.latticeengines.domain.exposed.serviceflows.modeling.steps.ModelStepConfiguration;
import com.latticeengines.domain.exposed.workflow.WorkflowContextConstants;
import com.latticeengines.proxy.exposed.cdl.RatingEngineProxy;
import com.latticeengines.workflow.exposed.build.BaseWorkflowStep;
import com.latticeengines.workflow.exposed.build.InternalResourceRestApiProxy;

@Component("downloadAndProcessModelSummaries")
public class DownloadAndProcessModelSummaries extends BaseWorkflowStep<ModelStepConfiguration> {

    @Autowired
    private WaitForDownloadedModelSummaries waitForDownloadedModelSummaries;

    private InternalResourceRestApiProxy proxy = null;

    @Autowired
    private RatingEngineProxy ratingEngineProxy;

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
        if (configuration.getActivateModelSummaryByDefault()) {
            attrMap.put("Status", ModelSummaryStatus.ACTIVE.getStatusCode());
        } else {
            attrMap.put("Status", ModelSummaryStatus.INACTIVE.getStatusCode());
        }
        for (String event : eventToModelId.keySet()) {
            String modelId = eventToModelId.get(event);
            proxy.updateModelSummary(modelId, attrMap);

            saveOutputValue(WorkflowContextConstants.Inputs.MODEL_ID, modelId);
            putStringValueInContext(SCORING_MODEL_ID, modelId);
            putStringValueInContext(SCORING_MODEL_TYPE, eventToModelSummary.get(event).getModelType());
        }

        if (StringUtils.isNotBlank(configuration.getRatingEngineId())) {
            RatingModel ratingModel = ratingEngineProxy.getRatingModel(configuration.getCustomerSpace().toString(),
                    configuration.getRatingEngineId(), configuration.getAiModelId());
            if (ratingModel != null && ratingModel instanceof AIModel) {
                AIModel aiModel = (AIModel) ratingModel;
                for (String event : eventToModelId.keySet()) {
                    aiModel.setModelSummary(eventToModelSummary.get(event));
                }
                ratingEngineProxy.updateRatingModel(configuration.getCustomerSpace().toString(),
                        configuration.getRatingEngineId(), configuration.getAiModelId(), aiModel);
                log.info("Attaching model summary: " + aiModel.getModelSummary().getId() + " to RatingEngine: "
                        + configuration.getRatingEngineId() + ", AIModel: " + configuration.getAiModelId());
            } else {
                log.info("No model found for RatingEngine: " + configuration.getRatingEngineId() + ", AIModel: "
                        + configuration.getAiModelId());
            }
        }

        putObjectInContext(EVENT_TO_MODELID, eventToModelId);

        for (ModelSummary modelSummary : eventToModelSummary.values()) {
            if (modelSummary.getTotalRowCount() != 0) {
                double avgProbability = (double) modelSummary.getTotalConversionCount()
                        / (double) modelSummary.getTotalRowCount();
                putDoubleValueInContext(MODEL_AVG_PROBABILITY, avgProbability);
                putDoubleValueInContext(SCORING_AVG_SCORE, avgProbability);
            } else {
                log.info("TotalRowCount is 0!");
            }
        }
    }
}
