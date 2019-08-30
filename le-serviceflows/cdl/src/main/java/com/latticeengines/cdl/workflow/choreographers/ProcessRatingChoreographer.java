package com.latticeengines.cdl.workflow.choreographers;

import static com.latticeengines.workflow.exposed.build.BaseWorkflowStep.CDL_ACTIVE_VERSION;
import static com.latticeengines.workflow.exposed.build.BaseWorkflowStep.CHOREOGRAPHER_CONTEXT_KEY;
import static com.latticeengines.workflow.exposed.build.BaseWorkflowStep.CURRENT_RATING_ITERATION;
import static com.latticeengines.workflow.exposed.build.BaseWorkflowStep.CUSTOMER_SPACE;
import static com.latticeengines.workflow.exposed.build.BaseWorkflowStep.INACTIVE_ENGINES;
import static com.latticeengines.workflow.exposed.build.BaseWorkflowStep.ITERATION_RATING_MODELS;
import static com.latticeengines.workflow.exposed.build.BaseWorkflowStep.RATING_MODELS;
import static com.latticeengines.workflow.exposed.build.BaseWorkflowStep.RATING_MODELS_BY_ITERATION;
import static com.latticeengines.workflow.exposed.build.BaseWorkflowStep.RESCORE_ALL_RATINGS;

import java.util.Collection;
import java.util.List;
import java.util.Set;

import javax.inject.Inject;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import com.latticeengines.cdl.workflow.GenerateAIRatingWorkflow;
import com.latticeengines.cdl.workflow.steps.process.CombineStatistics;
import com.latticeengines.cdl.workflow.steps.rating.CloneInactiveServingStores;
import com.latticeengines.cdl.workflow.steps.rating.ExtractRuleBasedRatings;
import com.latticeengines.cdl.workflow.steps.rating.PostIterationInitialization;
import com.latticeengines.cdl.workflow.steps.rating.PrepareForRating;
import com.latticeengines.cdl.workflow.steps.rating.SplitRatingEngines;
import com.latticeengines.cdl.workflow.steps.rating.StartIteration;
import com.latticeengines.cdl.workflow.steps.reset.ResetRating;
import com.latticeengines.domain.exposed.cdl.ChoreographerContext;
import com.latticeengines.domain.exposed.metadata.DataCollection;
import com.latticeengines.domain.exposed.pls.RatingEngineType;
import com.latticeengines.domain.exposed.pls.RatingModelContainer;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.domain.exposed.serviceflows.cdl.steps.process.BaseProcessEntityStepConfiguration;
import com.latticeengines.domain.exposed.workflow.BaseStepConfiguration;
import com.latticeengines.proxy.exposed.cdl.DataCollectionProxy;
import com.latticeengines.serviceflows.workflow.export.ImportGeneratingRatingFromS3;
import com.latticeengines.workflow.exposed.build.AbstractStep;
import com.latticeengines.workflow.exposed.build.BaseChoreographer;
import com.latticeengines.workflow.exposed.build.BaseWorkflowStep;
import com.latticeengines.workflow.exposed.build.Choreographer;

@Component("processRatingChoreographer")
public class ProcessRatingChoreographer extends BaseChoreographer implements Choreographer {

    private static final Logger log = LoggerFactory.getLogger(ProcessRatingChoreographer.class);

    @Inject
    private PrepareForRating prepareForRating;

    @Inject
    private CloneInactiveServingStores cloneInactiveServingStores;

    @Inject
    private ImportGeneratingRatingFromS3 importGeneratingRatingFromS3;

    @Inject
    private SplitRatingEngines splitRatingEngines;

    @Inject
    private GenerateAIRatingWorkflow generateAIRatingWorkflow;

    @Inject
    private ExtractRuleBasedRatings extractRuleBasedRatings;

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

    @Inject
    private StartIteration startIteration;

    @Inject
    private PostIterationInitialization postIterationInitialization;

    @Inject
    protected DataCollectionProxy dataCollectionProxy;

    private boolean initialized;
    private boolean enforceRebuild = false;
    private boolean hasDataChange = false;
    private boolean hasActions = false;
    private boolean hasAIModels = false;
    private boolean hasRuleModels = false;
    private boolean hasAccount = false;

    private boolean shouldReset = false;
    private boolean shouldRebuildAll = false;
    private boolean shouldRebuildSome = false;

    private boolean shouldProcessAI = false;
    private boolean shouldProcessRuleBased = false;

    private int iteration = 0;
    private int effectiveIterations = 1;
    private boolean iterationFinished = false;

    @Override
    public boolean skipStep(AbstractStep<? extends BaseStepConfiguration> step, int seq) {
        Set<BusinessEntity> entities = step.getSetObjectFromContext(BaseWorkflowStep.PA_SKIP_ENTITIES,
                BusinessEntity.class);
        if (CollectionUtils.isNotEmpty(entities) && entities.contains(BusinessEntity.Rating)) {
            return true;
        }
        if (isPrepareStep(step)) {
            checkEnforcedRebuild(step);
            return false;
        }

        if (isCloneServingStoresStep(step)) {
            initialize(step);
        }

        if (isPostIterationInitialization(step)) {
            initializeIteration(step);
        }

        boolean skip;
        if (shouldReset) {
            skip = !isResetRatingStep(step) && !isCombineStatisticsStep(step);
        } else if (shouldRebuildAll || shouldRebuildSome) {
            if (!hasAccount) {
                skip = true;
            } else if (isResetRatingStep(step) || iterationFinished) {
                skip = true;
            } else if (isStartIterationStep(step) || isCloneServingStoresStep(step)
                    || isImportGeneratingRatingFromS3(step) || isSplitRatingStep(step)) {
                // always run these steps in rebuild mode
                skip = false;
            } else {
                if (isAIWorkflow(seq)) {
                    skip = !shouldProcessAI;
                } else if (isIngestRuleRatingStep(step)) {
                    skip = !shouldProcessRuleBased;
                } else {
                    skip = !shouldProcessAI && !shouldProcessRuleBased;
                }
            }
        } else {
            skip = true;
        }
        return skip;
    }

    private void initialize(AbstractStep<? extends BaseStepConfiguration> step) {
        if (!initialized) {
            List<RatingModelContainer> containers = step.getListObjectFromContext(RATING_MODELS,
                    RatingModelContainer.class);
            List<String> inactiveEngines = step.getListObjectFromContext(INACTIVE_ENGINES,
                    String.class);
            List<?> generations = step.getObjectFromContext(RATING_MODELS_BY_ITERATION, List.class);
            effectiveIterations = Math.max(1, CollectionUtils.size(generations));
            boolean hasEngines = CollectionUtils.isNotEmpty(containers) || CollectionUtils.isNotEmpty(inactiveEngines);
            hasDataChange = hasDataChange();
            checkActionImpactedEngines(step);
            shouldReset = shouldReset(hasEngines);
            shouldRebuildAll = shouldRebuildAll();
            shouldRebuildSome = shouldRebuildSome();
            hasAccount = hasAccount(step);
            String[] msgs = new String[] { //
                    "enforced=" + enforceRebuild, //
                    "hasEngines=" + hasEngines, //
                    "hasDataChange=" + hasDataChange, //
                    "hasActions=" + hasActions, //
                    "shouldReset=" + shouldReset, //
                    "shouldRebuildAll=" + shouldRebuildAll, //
                    "shouldRebuildSome=" + shouldRebuildSome, //
                    "hasAccount=" + hasAccount, //
            };
            log.info(StringUtils.join(msgs, ", "));
            if (shouldRebuildAll) {
                step.putObjectInContext(RESCORE_ALL_RATINGS, true);
            }
            initialized = true;
        }
    }

    private void initializeIteration(AbstractStep<? extends BaseStepConfiguration> step) {
        if (shouldRebuildAll || shouldRebuildSome) {
            if (!iterationFinished) {
                if (!step.hasKeyInContext(CURRENT_RATING_ITERATION)) {
                    iterationFinished = true;
                    log.info("Rating iteration skipped due to no iteration set.");
                    return;
                }
                iteration = step.getObjectFromContext(CURRENT_RATING_ITERATION, Integer.class);
                List<RatingModelContainer> containers = step.getListObjectFromContext(ITERATION_RATING_MODELS,
                        RatingModelContainer.class);
                boolean hasCrossSellModels = hasCrossSellModels(containers);
                boolean hasCustomEventModels = hasCustomEventModels(containers);
                hasAIModels = hasCrossSellModels || hasCustomEventModels;
                hasRuleModels = hasRuleModels(containers);
                shouldProcessAI = shouldProcessAI();
                shouldProcessRuleBased = shouldProcessRuleBased();
                String[] msgs = new String[] {
                        "iteration=" + iteration,
                        "hasCrossSellModels=" + hasCrossSellModels, //
                        "hasCustomEventModels=" + hasCustomEventModels, //
                        "hasAIModels=" + hasAIModels, //
                        "hasRuleModels=" + hasRuleModels, //
                        "shouldProcessAI=" + shouldProcessAI, //
                        "shouldProcessRuleBased=" + shouldProcessRuleBased, //
                };
                log.info(StringUtils.join(msgs, ", "));
                iterationFinished = iteration > effectiveIterations;
                if (iterationFinished) {
                    log.info(String.format("All %d effective iterations are finished", effectiveIterations));
                }
            } else {
                log.info("Skip dummy iteration " + (++iteration));
            }
        } else {
            shouldProcessAI = false;
            shouldProcessRuleBased = false;
        }
    }

    private boolean isPrepareStep(AbstractStep<? extends BaseStepConfiguration> step) {
        return step.name().equals(prepareForRating.name());
    }

    private boolean isSplitRatingStep(AbstractStep<? extends BaseStepConfiguration> step) {
        return step.name().equals(splitRatingEngines.name());
    }

    private boolean isCloneServingStoresStep(AbstractStep<? extends BaseStepConfiguration> step) {
        return step.name().equals(cloneInactiveServingStores.name());
    }

    private boolean isImportGeneratingRatingFromS3(AbstractStep<? extends BaseStepConfiguration> step) {
        return step.name().equals(importGeneratingRatingFromS3.name());
    }

    private boolean isIngestRuleRatingStep(AbstractStep<? extends BaseStepConfiguration> step) {
        return step.name().endsWith(extractRuleBasedRatings.name());
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

    private boolean isStartIterationStep(AbstractStep<? extends BaseStepConfiguration> step) {
        return step.name().equals(startIteration.name());
    }

    private boolean isPostIterationInitialization(AbstractStep<? extends BaseStepConfiguration> step) {
        return step.name().equals(postIterationInitialization.name());
    }

    private boolean shouldReset(boolean hasEngines) {
        boolean reset = false;
        if (!hasEngines) {
            log.info("Does not have any ready to score models, reset the Rating entity.");
            reset = true;
        }
        return reset;
    }

    private boolean shouldRebuildAll() {
        boolean rebuild = false;
        if (shouldReset) {
            log.info("Skip rebuild, because should reset.");
            rebuild = false;
        } else if (enforceRebuild) {
            log.info("enforced to rebuild all ratings.");
            rebuild = true;
        } else if (hasDataChange) {
            log.info("Going to rebuild all, because has data change.");
            rebuild = true;
        } else {
            log.info("No reason to rebuild all.");
        }
        return rebuild;
    }

    private boolean shouldRebuildSome() {
        boolean rebuild = false;
        if (!shouldReset && hasActions) {
            log.info("Going to rebuild some, because has related actions.");
            rebuild = true;
        } else {
            log.info("No reason to rebuild some.");
        }
        return rebuild;
    }

    private boolean shouldProcessAI() {
        return shouldProcessModelOfOneType(hasAIModels, "AI");
    }

    private boolean shouldProcessRuleBased() {
        return shouldProcessModelOfOneType(hasRuleModels, "rule based");
    }

    private boolean shouldProcessModelOfOneType(boolean hasModels, String modelType) {
        boolean shouldProcess = shouldRebuildAll || shouldRebuildSome;
        if (shouldProcess && !hasModels) {
            log.info("Has no " + modelType + " models, skip generating " + modelType + " ratings");
            shouldProcess = false;
        }
        return shouldProcess;
    }

    private void checkEnforcedRebuild(AbstractStep<? extends BaseStepConfiguration> step) {
        BaseProcessEntityStepConfiguration configuration = (BaseProcessEntityStepConfiguration) step.getConfiguration();
        enforceRebuild = Boolean.TRUE.equals(configuration.getRebuild());
    }

    private boolean hasAccount(AbstractStep<? extends BaseStepConfiguration> step) {
        DataCollection.Version active = step.getObjectFromContext(CDL_ACTIVE_VERSION, DataCollection.Version.class);
        String customerSpace = step.getObjectFromContext(CUSTOMER_SPACE, String.class);
        if (StringUtils.isNotBlank(
                dataCollectionProxy.getTableName(customerSpace, BusinessEntity.Account.getBatchStore(), active))) {
            return true;
        }
        if (StringUtils.isNotBlank(dataCollectionProxy.getTableName(customerSpace,
                BusinessEntity.Account.getBatchStore(), active.complement()))) {
            return true;
        }
        return false;
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
        hasActions = grapherContext.isHasRatingEngineChange();
    }

}
