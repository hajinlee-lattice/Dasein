package com.latticeengines.modeling.workflow.steps.modeling;

import java.util.Collection;
import java.util.List;
import java.util.Map;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.domain.exposed.pls.AttributeMap;
import com.latticeengines.domain.exposed.pls.ModelSummary;
import com.latticeengines.domain.exposed.pls.ModelSummaryStatus;
import com.latticeengines.domain.exposed.serviceflows.modeling.steps.ModelStepConfiguration;
import com.latticeengines.workflow.exposed.build.BaseWorkflowStep;
import com.latticeengines.workflow.exposed.build.InternalResourceRestApiProxy;

@Component("activateModel")
public class ActivateModel extends BaseWorkflowStep<ModelStepConfiguration> {

    @Autowired
    private WaitForDownloadedModelSummaries waitForDownloadedModelSummaries;

    private InternalResourceRestApiProxy proxy = null;

    @SuppressWarnings("unchecked")
    @Override
    public void execute() {
        if (proxy == null) {
            proxy = new InternalResourceRestApiProxy(configuration.getInternalResourceHostPort());
        }
        Collection<String> modelIds;
        if (executionContext.getString(ACTIVATE_MODEL_IDS) == null) {
            Map<String, String> modelApplicationIdToEventColumn = getObjectFromContext(MODEL_APP_IDS, Map.class);
            if (modelApplicationIdToEventColumn == null || modelApplicationIdToEventColumn.isEmpty()) {
                throw new LedpException(LedpCode.LEDP_28012);
            }
            Map<String, ModelSummary> eventToModelSummary = waitForDownloadedModelSummaries.wait(configuration, modelApplicationIdToEventColumn);
            modelIds = retrieveModelIds(eventToModelSummary).values();
        } else {
            modelIds = getObjectFromContext(ACTIVATE_MODEL_IDS, List.class);
        }

        AttributeMap attrMap = new AttributeMap();
        attrMap.put("Status", ModelSummaryStatus.ACTIVE.getStatusCode());
        for (String modelId : modelIds) {
            proxy.updateModelSummary(modelId, attrMap);
        }
    }
}
