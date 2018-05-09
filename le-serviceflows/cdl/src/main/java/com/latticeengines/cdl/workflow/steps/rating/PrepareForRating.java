package com.latticeengines.cdl.workflow.steps.rating;

import java.util.List;
import java.util.stream.Collectors;

import javax.inject.Inject;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import com.latticeengines.common.exposed.util.NamingUtils;
import com.latticeengines.domain.exposed.modeling.CustomEventModelingType;
import com.latticeengines.domain.exposed.pls.AIModel;
import com.latticeengines.domain.exposed.pls.BucketMetadata;
import com.latticeengines.domain.exposed.pls.RatingEngine;
import com.latticeengines.domain.exposed.pls.RatingEngineStatus;
import com.latticeengines.domain.exposed.pls.RatingEngineSummary;
import com.latticeengines.domain.exposed.pls.RatingEngineType;
import com.latticeengines.domain.exposed.pls.RatingModel;
import com.latticeengines.domain.exposed.pls.RatingModelContainer;
import com.latticeengines.domain.exposed.pls.RuleBasedModel;
import com.latticeengines.domain.exposed.pls.cdl.rating.model.CustomEventModelingConfig;
import com.latticeengines.domain.exposed.serviceflows.cdl.steps.process.ProcessRatingStepConfiguration;
import com.latticeengines.domain.exposed.util.BucketMetadataUtils;
import com.latticeengines.proxy.exposed.cdl.RatingEngineProxy;
import com.latticeengines.workflow.exposed.build.BaseWorkflowStep;

import reactor.core.publisher.Flux;
import reactor.core.scheduler.Schedulers;

@Component("prepareForRating")
@Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
public class PrepareForRating extends BaseWorkflowStep<ProcessRatingStepConfiguration> {

    private static final Logger log = LoggerFactory.getLogger(PrepareForRating.class);

    @Inject
    private RatingEngineProxy ratingEngineProxy;

    private String customerSpace;

    @Override
    public void execute() {
        customerSpace = configuration.getCustomerSpace().toString();
        String rawRatingTableName = NamingUtils.timestamp("RawRating");
        putStringValueInContext(RULE_RAW_RATING_TABLE_NAME, rawRatingTableName);
        readActiveRatingModels();
        removeObjectFromContext(TABLES_GOING_TO_DYNAMO);
        removeObjectFromContext(TABLES_GOING_TO_REDSHIFT);
        removeObjectFromContext(STATS_TABLE_NAMES);
    }

    private void readActiveRatingModels() {
        List<RatingEngineSummary> summaries = ratingEngineProxy.getRatingEngineSummaries(customerSpace);
        if (CollectionUtils.isNotEmpty(summaries)) {
            List<RatingModelContainer> activeModels = Flux.fromIterable(summaries) //
                    .parallel().runOn(Schedulers.parallel()) //
                    .map(this::getValidRatingModel) //
                    .filter(container -> container.getModel() != null) //
                    .sequential().collectList().block();
            if (CollectionUtils.isNotEmpty(activeModels)) {
                log.info("Found " + activeModels.size() + " active rating models.");
                putObjectInContext(RATING_MODELS, activeModels);
            }
            if (CollectionUtils.isNotEmpty(activeModels)) {
                List<String> modelGuids = activeModels.stream().filter(container -> {
                    RatingEngineType engineType = container.getEngineSummary().getType();
                    return RatingEngineType.CUSTOM_EVENT.equals(engineType)
                            || RatingEngineType.CROSS_SELL.equals(engineType);
                }).map(container -> {
                    AIModel aiModel = (AIModel) container.getModel();
                    return aiModel.getModelSummaryId();
                }).collect(Collectors.toList());
                putStringValueInContext(SCORING_MODEL_ID, StringUtils.join(modelGuids, "|"));
            }
        } else {
            log.info("There is no rating engine summaries");
        }
    }

    private RatingModelContainer getValidRatingModel(RatingEngineSummary summary) {
        RatingModelContainer container = new RatingModelContainer(null, summary);
        String engineId = summary.getId();
        String engineName = summary.getDisplayName();
        RatingEngine engine = ratingEngineProxy.getRatingEngine(customerSpace, engineId);
        if (isValidRatingEngine(engine)) {
            RatingModel ratingModel = engine.getActiveModel();
            boolean isValid = false;
            if (RatingEngineType.CROSS_SELL.equals(summary.getType())) {
                AIModel aiModel = (AIModel) ratingModel;
                isValid = isValidCrossSellModel(aiModel);
                if (!isValid) {
                    log.warn("Cross sell rating model " + aiModel.getId() + " of " + engineId + " : " + engineName
                            + " is not ready for scoring.");
                }
            } else if (RatingEngineType.CUSTOM_EVENT.equals(summary.getType())) {
                AIModel aiModel = (AIModel) ratingModel;
                isValid = isValidCustomEventModel(aiModel);
                if (!isValid) {
                    log.warn("Custom event rating model " + aiModel.getId() + " of " + engineId + " : " + engineName
                            + " is not ready for scoring.");
                }
            } else if (RatingEngineType.RULE_BASED.equals(summary.getType())) {
                isValid = isValidRuleBasedModel((RuleBasedModel) ratingModel);
                if (!isValid) {
                    log.warn("Rule based rating model " + ratingModel.getId() + " of " + engineId + " : " + engineName
                            + " is not ready for scoring.");
                }
            }
            if (CollectionUtils.isEmpty(summary.getBucketMetadata())) {
                List<BucketMetadata> bucketMetadata = BucketMetadataUtils.getDefaultMetadata();
                summary.setBucketMetadata(bucketMetadata);
            }
            if (isValid) {
                container = new RatingModelContainer(ratingModel, summary);
            }
        }
        return container;
    }

    private boolean isValidRatingEngine(RatingEngine engine) {
        String engineId = engine.getId();
        String engineName = engine.getDisplayName();
        boolean valid = true;
        if (Boolean.TRUE.equals(engine.getDeleted())) {
            log.info("Skip rating engine " + engineId + " : " + engineName + " because it is deleted.");
            valid = false;
        } else if (RatingEngineStatus.INACTIVE.equals(engine.getStatus())
                && Boolean.TRUE.equals(engine.getJustCreated())) {
            log.info("Skip rating engine " + engineId + " : " + engineName + " because it is just created.");
            valid = false;
        } else if (engine.getSegment() == null) {
            log.info(
                    "Skip rating engine " + engineId + " : " + engineName + " because it belongs to an invalid segment.");
            valid = false;
        } else if (engine.getActiveModel() == null) {
            log.info(
                    "Skip rating engine " + engineId + " : " + engineName + " because it does not have an active model.");
            valid = false;
        }
        return valid;
    }

    private boolean isValidCrossSellModel(AIModel model) {
        boolean valid = true;
        if (StringUtils.isBlank(model.getModelSummaryId())) {
            log.warn("AI model " + model.getId() + " does not have an associated model summary.");
            valid = false;
        }
        return valid;
    }

    private boolean isValidCustomEventModel(AIModel model) {
        boolean valid;
        if (StringUtils.isBlank(model.getModelSummaryId())) {
            log.warn("AI model " + model.getId() + " does not have an associated model summary.");
            valid = false;
        } else {
            CustomEventModelingConfig advancedConf = CustomEventModelingConfig.getAdvancedModelingConfig(model);
            valid = CustomEventModelingType.CDL.equals(advancedConf.getCustomEventModelingType());
        }
        return valid;
    }

    private boolean isValidRuleBasedModel(RuleBasedModel model) {
        if (model.getRatingRule() == null) {
            log.warn("Rule based model " + model.getId() + " has null rating rule.");
            return false;
        }
        return true;
    }

}
