package com.latticeengines.serviceflows.workflow.modeling;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.stereotype.Component;

import com.google.common.base.Joiner;
import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.domain.exposed.pls.ModelSummary;
import com.latticeengines.serviceflows.workflow.core.InternalResourceRestApiProxy;
import com.latticeengines.domain.exposed.serviceflows.core.steps.MicroserviceStepConfiguration;

@Component("waitForDownloadedModelSummaries")
public class WaitForDownloadedModelSummaries {

    private static final Log log = LogFactory.getLog(WaitForDownloadedModelSummaries.class);

    private static final int MAX_TEN_SECOND_ITERATIONS_TO_WAIT_FOR_DOWNLOADED_MODELSUMMARIES = 60;

    public <T extends MicroserviceStepConfiguration> Map<String, ModelSummary> wait(T configuration,
            Map<String, String> modelApplicationIdToEventColumn) {
        InternalResourceRestApiProxy proxy = new InternalResourceRestApiProxy(
                configuration.getInternalResourceHostPort());
        Map<String, ModelSummary> eventToModelSummary = new HashMap<>();
        Set<String> foundModels = new HashSet<>();

        int maxTries = MAX_TEN_SECOND_ITERATIONS_TO_WAIT_FOR_DOWNLOADED_MODELSUMMARIES;
        int i = 0;

        log.info("Expecting to retrieve models with these application ids:"
                + Joiner.on(", ").join(modelApplicationIdToEventColumn.keySet()));

        do {
            for (String modelApplicationId : modelApplicationIdToEventColumn.keySet()) {
                if (!foundModels.contains(modelApplicationId)) {
                    ModelSummary model = proxy.getModelSummaryFromApplicationId(modelApplicationId, configuration
                            .getCustomerSpace().toString());
                    if (model != null) {
                        eventToModelSummary.put(modelApplicationIdToEventColumn.get(modelApplicationId), model);
                        foundModels.add(modelApplicationId);
                    } else {
                        log.info("Still waiting for model:" + modelApplicationId);
                    }
                }
            }

            try {
                Thread.sleep(10000L);
            } catch (InterruptedException e) {
                // do nothing
            }
            i++;

            if (i == maxTries) {
                break;
            }
        } while (eventToModelSummary.size() < modelApplicationIdToEventColumn.size());

        if (eventToModelSummary.size() < modelApplicationIdToEventColumn.size()) {
            Joiner joiner = Joiner.on(",").skipNulls();
            throw new LedpException(LedpCode.LEDP_28013, new String[] {
                    joiner.join(modelApplicationIdToEventColumn.keySet()), joiner.join(foundModels) });
        }

        return eventToModelSummary;
    }

}
