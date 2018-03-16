package com.latticeengines.cdl.workflow.steps.rating;

import java.util.ArrayList;
import java.util.List;

import javax.inject.Inject;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.collections4.MapUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import com.latticeengines.common.exposed.util.NamingUtils;
import com.latticeengines.domain.exposed.cdl.PredictionType;
import com.latticeengines.domain.exposed.pls.AIModel;
import com.latticeengines.domain.exposed.pls.BucketMetadata;
import com.latticeengines.domain.exposed.pls.BucketName;
import com.latticeengines.domain.exposed.pls.ModelSummary;
import com.latticeengines.domain.exposed.pls.RatingEngine;
import com.latticeengines.domain.exposed.pls.RatingEngineSummary;
import com.latticeengines.domain.exposed.pls.RatingEngineType;
import com.latticeengines.domain.exposed.pls.RatingModel;
import com.latticeengines.domain.exposed.pls.RatingModelContainer;
import com.latticeengines.domain.exposed.pls.RatingRule;
import com.latticeengines.domain.exposed.pls.RuleBasedModel;
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
        putObjectInContext(RULE_RAW_RATING_TABLE_NAME, rawRatingTableName);
        readActiveRatingModels();
        removeObjectFromContext(TABLE_GOING_TO_REDSHIFT);
        removeObjectFromContext(APPEND_TO_REDSHIFT_TABLE);
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
        } else {
            log.info("There is no rating engine summaries");
        }
    }

    private RatingModelContainer getValidRatingModel(RatingEngineSummary summary) {
        RatingModelContainer container = new RatingModelContainer(null, summary);
        String engineId = summary.getId();
        RatingEngine engine = ratingEngineProxy.getRatingEngine(customerSpace, engineId);
        RatingModel ratingModel = engine.getActiveModel();
        if (ratingModel != null) {
            boolean isValid = false;
            if (RatingEngineType.CROSS_SELL.equals(summary.getType())) {
                AIModel aiModel = (AIModel) ratingModel;
                isValid = isValidAIModel(aiModel);
                if (!isValid) {
                    log.warn("AI rating model " + aiModel.getId() + " is not ready for scoring.");
                } else if (StringUtils.isBlank(aiModel.getModelSummaryId())) {
                    ModelSummary modelSummary = aiModel.getModelSummary();
                    aiModel.setModelSummaryId(modelSummary.getId());
                    aiModel.setModelSummary(null);
                    if (CollectionUtils.isEmpty(summary.getBucketMetadata())) {
                        List<BucketMetadata> bucketMetadata = BucketMetadataUtils.getDefaultMetadata();
                        summary.setBucketMetadata(bucketMetadata);
                    }
                }
            } else if (RatingEngineType.RULE_BASED.equals(summary.getType())) {
                isValid = isValidRuleBasedModel((RuleBasedModel) ratingModel);
                if (!isValid) {
                    log.warn("Rule based rating model " + ratingModel.getId() + " is not ready for scoring.");
                }
            }
            if (isValid) {
                container = new RatingModelContainer(ratingModel, summary);
            }
        }
        return container;
    }

    private boolean isValidAIModel(AIModel model) {
        boolean valid = true;
        if (model.getModelSummary() == null) {
            log.warn("AI model " + model.getId() + " has null model summary.");
            valid = false;
        } else if (StringUtils.isNotBlank(model.getModelSummaryId())) {
            valid = true;
        } else if (model.getModelSummary() == null || StringUtils.isBlank(model.getModelSummary().getId())) {
            log.warn("AI model " + model.getId() + " has blank model guid.");
            valid = false;
        }
        return valid;
    }

    private boolean isValidRuleBasedModel(RuleBasedModel model) {
        if (model.getRatingRule() == null) {
            log.warn("Rule based model " + model.getId() + " has null rating rule.");
            return false;
        }
        RatingRule ratingRule = model.getRatingRule();
        if (MapUtils.isEmpty(ratingRule.getBucketToRuleMap())) {
            log.warn("Rule based model " + model.getId() + " has empty bucket to rule map.");
            return false;
        }
        if (StringUtils.isBlank(ratingRule.getDefaultBucketName())) {
            log.warn("Rule based model " + model.getId() + " has blank default bucket name.");
            return false;
        }
        return true;
    }

    private List<BucketMetadata> getDefaultBucketMetadata(PredictionType predictionType) {
        List<BucketMetadata> buckets = new ArrayList<>();
        switch (predictionType) {
            case PROPENSITY:
                buckets.add(addBucket(10, 4, BucketName.A));
                buckets.add(addBucket(4, 2, BucketName.B));
                buckets.add(addBucket(2, 1, BucketName.C));
                buckets.add(addBucket(1, 0, BucketName.D));
                break;
            case EXPECTED_VALUE:
                buckets.add(addBucket(10, 4, BucketName.A));
                buckets.add(addBucket(4, 2, BucketName.B));
                buckets.add(addBucket(2, 1, BucketName.C));
                buckets.add(addBucket(1, 0, BucketName.D));
                break;
            default:
                throw new UnsupportedOperationException("Unknown prediction type: " + predictionType);
        }
        return buckets;
    }

    private BucketMetadata addBucket(int leftBoundScore, int rightBoundScore, BucketName bucketName) {
        BucketMetadata bucket = new BucketMetadata();
        bucket.setLeftBoundScore(leftBoundScore);
        bucket.setRightBoundScore(rightBoundScore);
        bucket.setBucket(bucketName);
        return bucket;
    }

}
