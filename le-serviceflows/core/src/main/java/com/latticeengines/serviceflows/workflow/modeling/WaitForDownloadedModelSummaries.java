package com.latticeengines.serviceflows.workflow.modeling;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.stereotype.Component;

import com.google.common.base.Joiner;
import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.domain.exposed.pls.ModelSummary;
import com.latticeengines.serviceflows.workflow.core.InternalResourceRestApiProxy;
import com.latticeengines.serviceflows.workflow.core.MicroserviceStepConfiguration;

@Component("waitForDownloadedModelSummaries")
public class WaitForDownloadedModelSummaries {

    private static final Log log = LogFactory.getLog(WaitForDownloadedModelSummaries.class);

    private static final int MAX_TEN_SECOND_ITERATIONS_TO_WAIT_FOR_DOWNLOADED_MODELSUMMARIES = 6 * 60;

    public <T extends MicroserviceStepConfiguration> List<ModelSummary> wait(T configuration,
            Collection<String> modelApplicationIds) {
        InternalResourceRestApiProxy proxy = new InternalResourceRestApiProxy(
                configuration.getInternalResourceHostPort());
        List<ModelSummary> modelSummaries = new ArrayList<>();
        Set<String> foundModels = new HashSet<>();

        int maxTries = MAX_TEN_SECOND_ITERATIONS_TO_WAIT_FOR_DOWNLOADED_MODELSUMMARIES;
        int i = 0;

        log.info("Expecting to retrieve models with these application ids:" + Joiner.on(", ").join(modelApplicationIds));

        do {
            for (String modelApplicationId : modelApplicationIds) {
                if (!foundModels.contains(modelApplicationId)) {
                    ModelSummary model = proxy.getModelSummaryFromApplicationId(modelApplicationId, configuration
                            .getCustomerSpace().toString());
                    if (model != null) {
                        modelSummaries.add(model);
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
        } while (modelSummaries.size() < modelApplicationIds.size());

        if (modelSummaries.size() < modelApplicationIds.size()) {
            Joiner joiner = Joiner.on(",").skipNulls();
            throw new LedpException(LedpCode.LEDP_28013, new String[] { joiner.join(modelApplicationIds),
                    joiner.join(foundModels) });
        }

        return modelSummaries;
    }

    public <T extends MicroserviceStepConfiguration> List<String> retrieveModelIds(T configuration,
            Collection<String> modelApplicationIds) {
        List<String> modelIds = new ArrayList<>();
        List<ModelSummary> modelSummaries = wait(configuration, modelApplicationIds);
        for (ModelSummary modelSummary : modelSummaries) {
            modelIds.add(modelSummary.getId());
        }
        return modelIds;
    }

}
