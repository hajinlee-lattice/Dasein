package com.latticeengines.serviceflows.workflow.modeling;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.stereotype.Component;

import com.google.common.base.Joiner;
import com.google.common.base.Strings;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.domain.exposed.pls.ModelSummary;
import com.latticeengines.domain.exposed.pls.TargetMarket;
import com.latticeengines.serviceflows.workflow.core.BaseWorkflowStep;
import com.latticeengines.serviceflows.workflow.core.InternalResourceRestApiProxy;

@Component("chooseModel")
public class ChooseModel extends BaseWorkflowStep<ChooseModelStepConfiguration> {

    private static final Log log = LogFactory.getLog(ChooseModel.class);

    private static final int MAX_TEN_SECOND_ITERATIONS_TO_WAIT_FOR_DOWNLOADED_MODELSUMMARIES = 6 * 60;
    private static final int MINIMUM_POSITIVE_EVENTS = 300;
    private static final double MINIMUM_ROC = 0.7;

    private InternalResourceRestApiProxy proxy = null;

    @Override
    public void execute() {
        log.info("Inside ChooseModel execute()");

        @SuppressWarnings("unchecked")
        List<String> modelApplicationIds = JsonUtils.deserialize(executionContext.getString(MODEL_APP_IDS), List.class);
        if (modelApplicationIds == null || modelApplicationIds.isEmpty()) {
            throw new LedpException(LedpCode.LEDP_28012);
        }

        proxy = new InternalResourceRestApiProxy(configuration.getInternalResourceHostPort());
        List<ModelSummary> modelSummaries = waitForDownloadedModelSummaries(modelApplicationIds);
        String bestModelId = chooseBestModelId(modelSummaries);

        TargetMarket targetMarket = configuration.getTargetMarket();
        targetMarket.setModelId(bestModelId);
        proxy.updateTargetMarket(targetMarket, configuration.getCustomerSpace().toString());
    }

    private String chooseBestModelId(List<ModelSummary> models) {
        String chosenModelId = null;
        List<ModelSummary> validModels = new ArrayList<>();
        StringBuilder sb = new StringBuilder();

        for (ModelSummary model : models) {
            sb.append(model.getId()).append(" ");
            boolean isValid = true;

            if (model.getTotalConversionCount() < MINIMUM_POSITIVE_EVENTS) {
                isValid = false;
                log.info(String.format(
                        "Model %s discarded; contains %s positive events which is fewer than minimum %d.",
                        model.getId(), model.getTotalConversionCount(), MINIMUM_POSITIVE_EVENTS));
            }
            if (model.getRocScore() < MINIMUM_ROC) {
                isValid = false;
                log.info(String.format("Model %s discarded; its ROC score %s is less than minimum %d.", model.getId(),
                        model.getRocScore(), MINIMUM_ROC));
            }
            if (isValid) {
                validModels.add(model);
            }
        }

        if (validModels.size() == 0) {
            log.warn("None of the models met the minimum criteria");

            if (!configuration.getTargetMarket().getIsDefault()) {
                TargetMarket defaultTargetMarket = proxy.findTargetMarketByName(TargetMarket.DEFAULT_NAME,
                        configuration.getCustomerSpace().toString());
                if (Strings.isNullOrEmpty(defaultTargetMarket.getModelId())) {
                    log.warn("No existing global model available so falling back to choosing the model with highest lift");
                    chosenModelId = chooseModelWithHighestLift(models).getId();
                } else {
                    log.info("Using the global model from the default target market");
                    chosenModelId = defaultTargetMarket.getModelId();
                }
            } else {
                chosenModelId = chooseModelWithHighestLift(models).getId();
            }
        } else {
            chosenModelId = chooseModelWithHighestLift(validModels).getId();
        }

        log.info(String.format("Chose best model %s from among:%s", chosenModelId, sb.toString()));
        return chosenModelId;
    }

    private ModelSummary chooseModelWithHighestLift(List<ModelSummary> models) {
        ModelSummary chosenModel = null;
        double highestLift = Double.MIN_VALUE;
        for (ModelSummary model : models) {
            if (model.getTop20PercentLift() > highestLift) {
                highestLift = model.getTop20PercentLift();
                chosenModel = model;
            }
        }

        return chosenModel;
    }

    private List<ModelSummary> waitForDownloadedModelSummaries(List<String> modelApplicationIds) {
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

}