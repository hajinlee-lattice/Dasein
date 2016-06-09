package com.latticeengines.serviceflows.workflow.modeling;

import java.util.List;
import java.util.Map;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.domain.exposed.pls.AttributeMap;
import com.latticeengines.domain.exposed.pls.ModelSummaryStatus;
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
        List<String> modelIds;

        Map<String, String> modelApplicationIdToEventColumn = JsonUtils.deserialize(
                executionContext.getString(MODEL_APP_IDS), Map.class);
        if (modelApplicationIdToEventColumn == null || modelApplicationIdToEventColumn.isEmpty()) {
            throw new LedpException(LedpCode.LEDP_28012);
        }
        modelIds = waitForDownloadedModelSummaries.retrieveModelIds(configuration,
                modelApplicationIdToEventColumn.keySet());

        AttributeMap attrMap = new AttributeMap();
        attrMap.put("Status", ModelSummaryStatus.INACTIVE.getStatusCode());
        for (String modelId : modelIds) {
            proxy.updateModelSummary(modelId, attrMap);
        }
    }
}
