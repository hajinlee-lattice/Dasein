package com.latticeengines.serviceflows.workflow.modeling;

import java.util.List;
import java.util.Map;

import com.latticeengines.serviceflows.workflow.core.BaseWorkflowStep;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.domain.exposed.pls.AttributeMap;
import com.latticeengines.serviceflows.workflow.core.InternalResourceRestApiProxy;

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
        List<String> modelIds;
        if (executionContext.get(ACTIVATE_MODEL_IDS) == null) {
            Map<String, String> modelApplicationIdToEventColumn = JsonUtils.deserialize(
                    executionContext.getString(MODEL_APP_IDS), Map.class);
            if (modelApplicationIdToEventColumn == null || modelApplicationIdToEventColumn.isEmpty()) {
                throw new LedpException(LedpCode.LEDP_28012);
            }
            modelIds = waitForDownloadedModelSummaries.retrieveModelIds(configuration,
                    modelApplicationIdToEventColumn.keySet());
        } else {
            modelIds = JsonUtils.deserialize(executionContext.getString(ACTIVATE_MODEL_IDS), List.class);
        }

        AttributeMap attrMap = new AttributeMap();
        attrMap.put("Status", "UpdateAsActive");
        for (String modelId : modelIds) {
            proxy.updateModelSummary(modelId, attrMap);
        }
    }
}
