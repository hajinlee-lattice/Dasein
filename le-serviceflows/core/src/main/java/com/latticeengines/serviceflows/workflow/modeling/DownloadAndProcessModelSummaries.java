package com.latticeengines.serviceflows.workflow.modeling;

import java.util.Map;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.domain.exposed.pls.AttributeMap;
import com.latticeengines.domain.exposed.pls.ModelSummaryStatus;
import com.latticeengines.domain.exposed.workflow.WorkflowContextConstants;
import com.latticeengines.proxy.exposed.metadata.MetadataProxy;
import com.latticeengines.serviceflows.workflow.core.BaseWorkflowStep;
import com.latticeengines.serviceflows.workflow.core.InternalResourceRestApiProxy;

@Component("downloadAndProcessModelSummaries")
public class DownloadAndProcessModelSummaries extends BaseWorkflowStep<ModelStepConfiguration> {

    @Autowired
    private MetadataProxy metadataProxy;

    @Autowired
    private WaitForDownloadedModelSummaries waitForDownloadedModelSummaries;

    private InternalResourceRestApiProxy proxy = null;

    @SuppressWarnings("unchecked")
    @Override
    public void execute() {
        if (proxy == null) {
            proxy = new InternalResourceRestApiProxy(configuration.getInternalResourceHostPort());
        }

        Map<String, String> modelApplicationIdToEventColumn = JsonUtils
                .deserialize(executionContext.getString(MODEL_APP_IDS), Map.class);
        if (modelApplicationIdToEventColumn == null || modelApplicationIdToEventColumn.isEmpty()) {
            throw new LedpException(LedpCode.LEDP_28012);
        }
        Map<String, String> eventToModelId = waitForDownloadedModelSummaries
                .retrieveModelIds(configuration, modelApplicationIdToEventColumn);

        AttributeMap attrMap = new AttributeMap();
        attrMap.put("Status", ModelSummaryStatus.INACTIVE.getStatusCode());
        for (String modelId : eventToModelId.values()) {
            proxy.updateModelSummary(modelId, attrMap);
            putOutputValue(WorkflowContextConstants.Outputs.MODEL_ID, modelId);
        }

        putObjectInContext(EVENT_TO_MODELID, eventToModelId);
    }
}
