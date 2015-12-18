package com.latticeengines.serviceflows.workflow.modeling;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.stereotype.Component;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Joiner;
import com.google.common.base.Strings;
import com.google.common.collect.Maps;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.domain.exposed.pls.ModelSummary;
import com.latticeengines.domain.exposed.pls.TargetMarket;
import com.latticeengines.serviceflows.workflow.core.BaseWorkflowStep;
import com.latticeengines.serviceflows.workflow.core.InternalResourceRestApiProxy;

@Component("chooseModel")
public class ChooseModel extends BaseWorkflowStep<ChooseModelStepConfiguration> {

    public static final long MINIMUM_POSITIVE_EVENTS = 300;
    public static final double MINIMUM_ROC = 0.7;

    private static final Log log = LogFactory.getLog(ChooseModel.class);
    private static final int MAX_TEN_SECOND_ITERATIONS_TO_WAIT_FOR_DOWNLOADED_MODELSUMMARIES = 6 * 60;

    private InternalResourceRestApiProxy proxy = null;

    @Override
    public void execute() {
        log.info("Inside ChooseModel execute()");

        @SuppressWarnings("unchecked")
        Map<String, String> modelApplicationIdToEventColumn = JsonUtils.deserialize(
                executionContext.getString(MODEL_APP_IDS), Map.class);
        if (modelApplicationIdToEventColumn == null || modelApplicationIdToEventColumn.isEmpty()) {
            throw new LedpException(LedpCode.LEDP_28012);
        }

        if (proxy == null) {
            proxy = new InternalResourceRestApiProxy(configuration.getInternalResourceHostPort());
        }
        List<ModelSummary> modelSummaries = waitForDownloadedModelSummaries(modelApplicationIdToEventColumn.keySet());
        Entry<String, String> bestModelIdAndEventColumn = chooseBestModelIdAndEventColumn(modelSummaries,
                modelApplicationIdToEventColumn);
        executionContext.putString(SCORING_MODEL_ID, bestModelIdAndEventColumn.getKey());

        TargetMarket targetMarket = proxy.findTargetMarketByName(configuration.getTargetMarket().getName(),
                configuration.getCustomerSpace().toString());
        targetMarket.setModelId(bestModelIdAndEventColumn.getKey());
        targetMarket.setEventColumnName(bestModelIdAndEventColumn.getValue());
        proxy.updateTargetMarket(targetMarket, configuration.getCustomerSpace().toString());
    }

    @VisibleForTesting
    Entry<String, String> chooseBestModelIdAndEventColumn(List<ModelSummary> models,
            Map<String, String> modelApplicationIdToEventColumn) {
        Entry<String, String> chosenModelIdAndEventColumn = null;
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
                log.info(String.format("Model %s discarded; its ROC score %f is less than minimum %f.", model.getId(),
                        model.getRocScore(), MINIMUM_ROC));
            }
            if (isValid) {
                validModels.add(model);
            }
        }

        ModelSummary chosenModel = null;
        if (validModels.size() == 0) {
            log.warn("None of the models met the minimum criteria");

            if (!configuration.getTargetMarket().getIsDefault()) {
                TargetMarket defaultTargetMarket = proxy.findTargetMarketByName(TargetMarket.DEFAULT_NAME,
                        configuration.getCustomerSpace().toString());
                if (Strings.isNullOrEmpty(defaultTargetMarket.getModelId())) {
                    log.warn("No existing global model available so falling back to choosing the model with highest lift");
                    chosenModel = chooseModelWithHighestLift(models);
                } else {
                    log.info("Using the global model from the default target market");
                    chosenModelIdAndEventColumn = Maps.immutableEntry(defaultTargetMarket.getModelId(),
                            defaultTargetMarket.getEventColumnName());
                }
            } else {
                chosenModel = chooseModelWithHighestLift(models);
            }
        } else {
            chosenModel = chooseModelWithHighestLift(validModels);
        }

        if (chosenModelIdAndEventColumn == null) {
            chosenModelIdAndEventColumn = Maps.immutableEntry(chosenModel.getId(),
                    modelApplicationIdToEventColumn.get(chosenModel.getApplicationId()));
        }
        log.info(String.format("Chose best model:%s eventColumn: from among:%s", chosenModelIdAndEventColumn.getKey(),
                chosenModelIdAndEventColumn.getValue(), sb.toString()));
        return chosenModelIdAndEventColumn;
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

    private List<ModelSummary> waitForDownloadedModelSummaries(Set<String> modelApplicationIds) {
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

    @VisibleForTesting
    void setProxy(InternalResourceRestApiProxy proxy) {
        this.proxy = proxy;
    }

    @VisibleForTesting
    void setConfiguration(ChooseModelStepConfiguration configuration) {
        this.configuration = configuration;
    }

}