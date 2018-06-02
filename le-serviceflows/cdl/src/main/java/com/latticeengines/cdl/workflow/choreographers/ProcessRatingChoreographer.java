package com.latticeengines.cdl.workflow.choreographers;

import static com.latticeengines.workflow.exposed.build.BaseWorkflowStep.CHOREOGRAPHER_CONTEXT_KEY;
import static com.latticeengines.workflow.exposed.build.BaseWorkflowStep.RATING_MODELS;

import java.util.Collection;
import java.util.List;

import javax.inject.Inject;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import com.latticeengines.cdl.workflow.GenerateAIRatingWorkflow;
import com.latticeengines.cdl.workflow.steps.process.CombineStatistics;
import com.latticeengines.cdl.workflow.steps.rating.CloneInactiveServingStores;
import com.latticeengines.cdl.workflow.steps.rating.IngestRuleBasedRating;
import com.latticeengines.cdl.workflow.steps.rating.PrepareForRating;
import com.latticeengines.cdl.workflow.steps.reset.ResetRating;
import com.latticeengines.domain.exposed.cdl.ChoreographerContext;
import com.latticeengines.domain.exposed.pls.RatingEngineType;
import com.latticeengines.domain.exposed.pls.RatingModelContainer;
import com.latticeengines.domain.exposed.serviceflows.cdl.steps.process.BaseProcessEntityStepConfiguration;
import com.latticeengines.domain.exposed.workflow.BaseStepConfiguration;
import com.latticeengines.workflow.exposed.build.AbstractStep;
import com.latticeengines.workflow.exposed.build.BaseChoreographer;
import com.latticeengines.workflow.exposed.build.Choreographer;

@Component("processRatingChoreographer")
public class ProcessRatingChoreographer extends BaseChoreographer implements Choreographer {

    private static final Logger log = LoggerFactory.getLogger(ProcessRatingChoreographer.class);

    @Inject
    private PrepareForRating prepareForRating;

    @Inject
    private CloneInactiveServingStores cloneInactiveServingStores;

    @Inject
    private GenerateAIRatingWorkflow generateAIRatingWorkflow;

    @Inject
    private IngestRuleBasedRating ingestRuleBasedRating;

    @Inject
    private ProcessAccountChoreographer accountChoreographer;

    @Inject
    private ProcessContactChoreographer contactChoreographer;

    @Inject
    private ProcessProductChoreographer productChoreographer;

    @Inject
    private ProcessTransactionChoreographer transactionChoreographer;

    @Inject
    private CombineStatistics combineStatistics;

    @Inject
    private ResetRating resetRating;

    private boolean initialized;
    private boolean enforceRebuild = false;
    private boolean hasDataChange = false;
    private boolean hasActionImpactedAIModels = false;
    private boolean hasActionImpactedRuleModels = false;
    private boolean hasAIModels = false;
    private boolean hasCrossSellModels = false;
    private boolean hasCustomEventModels = false;
    private boolean hasRuleModels = false;

    private boolean shouldReset = false;
    private boolean shouldProcessAI = false;
    private boolean shouldProcessRuleBased = false;

    @Override
    public boolean skipStep(AbstractStep<? extends BaseStepConfiguration> step, int seq) {
        if (isPrepareStep(step)) {
            checkEnforcedRebuild(step);
            return false;
        }

        if (isCloneServingStoresStep(step)) {
            initialize(step);
        }

        log.info("Step namespace = " + getStepNamespace(seq) + " generateAIRatingWorkflow.name()="
                + generateAIRatingWorkflow.name());

        boolean skip;
        if (shouldReset) {
            skip = !isResetRatingStep(step) && !isCombineStatisticsStep(step);
        } else {
            skip = shouldProcessAI || shouldProcessRuleBased;
            if (isResetRatingStep(step)) {
                skip = true;
            } else if (isAIWorkflow(seq)) {
                skip = !shouldProcessAI;
            } else if (isIngestRuleRatingStep(step)) {
                skip = !shouldProcessRuleBased;
            }
        }
        return skip;
    }

    private void initialize(AbstractStep<? extends BaseStepConfiguration> step) {
        if (!initialized) {
            List<RatingModelContainer> containers = step.getListObjectFromContext(RATING_MODELS,
                    RatingModelContainer.class);
            hasDataChange = hasDataChange();
            checkActionImpactedEngines(step);
            hasCrossSellModels = hasCrossSellModels(containers);
            hasCustomEventModels = hasCustomEventModels(containers);
            hasAIModels = hasCrossSellModels || hasCustomEventModels;
            hasRuleModels = hasRuleModels(containers);
            shouldReset = shouldReset();
            shouldProcessAI = shouldProcessAI();
            shouldProcessRuleBased = shouldProcessRuleBased();
            String[] msgs = new String[] { //
                    " enforced=" + enforceRebuild, //
                    " hasDataChange=" + hasDataChange, //
                    " hasActionImpactedAIModels=" + hasActionImpactedAIModels, //
                    " hasActionImpactedRuleModels=" + hasActionImpactedRuleModels, //
                    " hasCrossSellModels=" + hasCrossSellModels, //
                    " hasCustomEventModels=" + hasCustomEventModels, //
                    " hasAIModels=" + hasAIModels, //
                    " hasRuleModels=" + hasRuleModels, //
                    " shouldReset=" + shouldReset, //
                    " shouldProcessAI=" + shouldProcessAI, //
                    " shouldProcessRuleBased=" + shouldProcessRuleBased, //
            };
            log.info(StringUtils.join(msgs, ", "));
            initialized = true;
        }
    }

    private boolean isPrepareStep(AbstractStep<? extends BaseStepConfiguration> step) {
        return step.name().equals(prepareForRating.name());
    }

    private boolean isCloneServingStoresStep(AbstractStep<? extends BaseStepConfiguration> step) {
        return step.name().equals(cloneInactiveServingStores.name());
    }

    private boolean isIngestRuleRatingStep(AbstractStep<? extends BaseStepConfiguration> step) {
        return step.name().endsWith(ingestRuleBasedRating.name());
    }

    private boolean isAIWorkflow(int seq) {
        String namespace = getStepNamespace(seq);
        return namespace.contains(generateAIRatingWorkflow.name());
    }

    private boolean isResetRatingStep(AbstractStep<? extends BaseStepConfiguration> step) {
        return step.name().equals(resetRating.name());
    }

    private boolean isCombineStatisticsStep(AbstractStep<? extends BaseStepConfiguration> step) {
        return step.name().equals(combineStatistics.name());
    }

    private boolean shouldReset() {
        boolean reset = false;
        if (!hasCustomEventModels && !hasCrossSellModels && !hasRuleModels) {
            log.info("Does not have any ready to score models, reset the Rating entity.");
            reset = true;
        }
        return reset;
    }

    private boolean shouldProcessAI() {
        return shouldProcessModelOfOneType(hasAIModels, "AI");
    }

    private boolean shouldProcessRuleBased() {
        return shouldProcessModelOfOneType(hasRuleModels, "rule based");
    }

    private boolean shouldProcessModelOfOneType(boolean hasModels, String modelType) {
        boolean shouldProcess = true;
        boolean hasActionImpactedEngines = "AI".equalsIgnoreCase(modelType) ? hasActionImpactedAIModels
                : hasActionImpactedRuleModels;
        if (!hasModels) {
            log.info("Has no " + modelType + " models, skip generating " + modelType + " ratings");
            shouldProcess = false;
        } else if (enforceRebuild) {
            log.info("enforced to rebuild ratings.");
            shouldProcess = true;
        } else if (!hasDataChange && !hasActionImpactedEngines) {
            log.warn("Has neither underlying data change nor related actions, should skip generating " + modelType
                    + " ratings");
            shouldProcess = false;
        }
        return shouldProcess;
    }

    private void checkEnforcedRebuild(AbstractStep<? extends BaseStepConfiguration> step) {
        BaseProcessEntityStepConfiguration configuration = (BaseProcessEntityStepConfiguration) step.getConfiguration();
        enforceRebuild = Boolean.TRUE.equals(configuration.getRebuild());
    }

    private boolean hasCrossSellModels(Collection<RatingModelContainer> containers) {
        return CollectionUtils.isNotEmpty(containers) && containers.stream()
                .anyMatch(container -> RatingEngineType.CROSS_SELL.equals(container.getEngineSummary().getType()));
    }

    private boolean hasCustomEventModels(Collection<RatingModelContainer> containers) {
        return CollectionUtils.isNotEmpty(containers) && containers.stream()
                .anyMatch(container -> RatingEngineType.CUSTOM_EVENT.equals(container.getEngineSummary().getType()));
    }

    private boolean hasRuleModels(Collection<RatingModelContainer> containers) {
        return CollectionUtils.isNotEmpty(containers) && containers.stream()
                .anyMatch(container -> RatingEngineType.RULE_BASED.equals(container.getEngineSummary().getType()));
    }

    private boolean hasDataChange() {
        return accountChoreographer.hasAnyChange() || contactChoreographer.hasAnyChange()
                || productChoreographer.hasAnyChange() || transactionChoreographer.hasAnyChange();
    }

    private void checkActionImpactedEngines(AbstractStep<? extends BaseStepConfiguration> step) {
        ChoreographerContext grapherContext = step.getObjectFromContext(CHOREOGRAPHER_CONTEXT_KEY,
                ChoreographerContext.class);

        // TODO: change to check by engine type after DP-6275
        hasActionImpactedAIModels = CollectionUtils.isNotEmpty(grapherContext.getActionImpactedAIRatingEngines());
        hasActionImpactedRuleModels = CollectionUtils.isNotEmpty(grapherContext.getActionImpactedRuleRatingEngines());

        hasActionImpactedAIModels = grapherContext.isHasRatingEngineChange();
        hasActionImpactedRuleModels = grapherContext.isHasRatingEngineChange();
    }

}
