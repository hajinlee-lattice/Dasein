package com.latticeengines.modeling.workflow.steps.modeling;

import java.util.Collection;
import java.util.List;
import java.util.Map;

import javax.inject.Inject;

import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.domain.exposed.pls.AttributeMap;
import com.latticeengines.domain.exposed.pls.ModelSummary;
import com.latticeengines.domain.exposed.pls.ModelSummaryStatus;
import com.latticeengines.domain.exposed.serviceflows.modeling.steps.ModelStepConfiguration;
import com.latticeengines.proxy.exposed.lp.ModelSummaryProxy;
import com.latticeengines.workflow.exposed.build.BaseWorkflowStep;

@Component("activateModel")
@Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
public class ActivateModel extends BaseWorkflowStep<ModelStepConfiguration> {

    @Inject
    private ModelSummaryProxy modelSummaryProxy;

    @SuppressWarnings("unchecked")
    @Override
    public void execute() {
        Collection<String> modelIds;
        if (executionContext.getString(ACTIVATE_MODEL_IDS) == null) {
            Map<String, String> modelApplicationIdToEventColumn = getObjectFromContext(MODEL_APP_IDS, Map.class);
            if (modelApplicationIdToEventColumn == null || modelApplicationIdToEventColumn.isEmpty()) {
                throw new LedpException(LedpCode.LEDP_28012);
            }

            Map<String, ModelSummary> eventToModelSummary = modelSummaryProxy.getEventToModelSummary(
                    configuration.getCustomerSpace().getTenantId(), modelApplicationIdToEventColumn);

            modelIds = retrieveModelIds(eventToModelSummary).values();
        } else {
            modelIds = getObjectFromContext(ACTIVATE_MODEL_IDS, List.class);
        }

        AttributeMap attrMap = new AttributeMap();
        attrMap.put("Status", ModelSummaryStatus.ACTIVE.getStatusCode());
        for (String modelId : modelIds) {
            modelSummaryProxy.update(configuration.getCustomerSpace().toString(), modelId, attrMap);
        }
    }
}
