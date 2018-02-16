package com.latticeengines.cdl.workflow.choreographers;

import static com.latticeengines.serviceflows.workflow.core.BaseWorkflowStep.RATING_MODELS;

import java.util.Collection;
import java.util.List;

import javax.inject.Inject;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import com.latticeengines.cdl.workflow.GenerateAIRatingWorkflow;
import com.latticeengines.cdl.workflow.steps.rating.CloneInactiveServingStores;
import com.latticeengines.cdl.workflow.steps.rating.IngestRuleBasedRating;
import com.latticeengines.cdl.workflow.steps.rating.PrepareForRating;
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

    private boolean initialized;
    private boolean enforceRebuild = false;
    private boolean hasAIModels = false;
    private boolean hasRuleModels = false;

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

        if (isAIWorkflow(seq)) {
            return !shouldProcessAI;
        }

        if (isIngestRuleRatingStep(step)) {
            return !shouldProcessRuleBased;
        }

        return !enforceRebuild;

        // TODO: resume following after rating workflow is complete
        // return !enforceRebuild || !hasModels;
    }

    private void initialize(AbstractStep<? extends BaseStepConfiguration> step) {
        if (!initialized) {
            List<RatingModelContainer> containers = step.getListObjectFromContext(RATING_MODELS,
                    RatingModelContainer.class);
            hasAIModels = hasAIModels(containers);
            hasRuleModels = hasRuleModels(containers);
            shouldProcessAI = shouldProcessAI();
            shouldProcessRuleBased = shouldProcessRuleBased();
            String[] msgs = new String[]{ //
                    "enforced=" + enforceRebuild, //
                    "hasAIModels=" + hasAIModels, //
                    "hasRuleModels=" + hasRuleModels, //
                    "shouldProcessAI=" + shouldProcessAI, //
                    "shouldProcessRuleBased=" + shouldProcessRuleBased, //
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

    private boolean shouldProcessAI() {
        if (!hasAIModels) {
            log.info("Has no AI models, skip generating AI ratings");
            return false;
        }
        return enforceRebuild;
    }

    private boolean shouldProcessRuleBased() {
        if (!hasRuleModels) {
            log.info("Has no rule based models, skip generating rule based ratings");
            return false;
        }
        return enforceRebuild;
    }

    private void checkEnforcedRebuild(AbstractStep<? extends BaseStepConfiguration> step) {
        BaseProcessEntityStepConfiguration configuration = (BaseProcessEntityStepConfiguration) step.getConfiguration();
        enforceRebuild = Boolean.TRUE.equals(configuration.getRebuild());
    }

    private boolean hasAIModels(Collection<RatingModelContainer> containers) {
        return CollectionUtils.isNotEmpty(containers) && containers.stream()
                .anyMatch(container -> RatingEngineType.AI_BASED.equals(container.getEngineSummary().getType()));
    }

    private boolean hasRuleModels(Collection<RatingModelContainer> containers) {
        return CollectionUtils.isNotEmpty(containers) && containers.stream()
                .anyMatch(container -> RatingEngineType.RULE_BASED.equals(container.getEngineSummary().getType()));
    }

}
