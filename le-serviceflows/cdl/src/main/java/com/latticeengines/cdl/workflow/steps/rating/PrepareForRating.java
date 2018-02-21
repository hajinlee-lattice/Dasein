package com.latticeengines.cdl.workflow.steps.rating;

import java.util.List;
import java.util.Objects;

import javax.inject.Inject;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.collections4.MapUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import com.latticeengines.common.exposed.util.NamingUtils;
import com.latticeengines.domain.exposed.pls.AIModel;
import com.latticeengines.domain.exposed.pls.RatingEngine;
import com.latticeengines.domain.exposed.pls.RatingEngineSummary;
import com.latticeengines.domain.exposed.pls.RatingEngineType;
import com.latticeengines.domain.exposed.pls.RatingModel;
import com.latticeengines.domain.exposed.pls.RatingModelContainer;
import com.latticeengines.domain.exposed.pls.RatingRule;
import com.latticeengines.domain.exposed.pls.RuleBasedModel;
import com.latticeengines.domain.exposed.serviceflows.cdl.steps.process.ProcessRatingStepConfiguration;
import com.latticeengines.proxy.exposed.cdl.RatingEngineProxy;
import com.latticeengines.serviceflows.workflow.core.BaseWorkflowStep;

import reactor.core.publisher.Flux;
import reactor.core.scheduler.Schedulers;

@Component("prepareForRating")
public class PrepareForRating extends BaseWorkflowStep<ProcessRatingStepConfiguration> {

    private static final Logger log = LoggerFactory.getLogger(PrepareForRating.class);

    @Inject
    private RatingEngineProxy ratingEngineProxy;

    private String customerSpace;

    @Override
    public void execute() {
        customerSpace = configuration.getCustomerSpace().toString();
        String rawRatingTableName = NamingUtils.timestamp("RawRating");
        putObjectInContext(RAW_RATING_TABLE_NAME, rawRatingTableName);
        readActiveRatingModels();
        putObjectInContext(TABLE_GOING_TO_REDSHIFT, null);
        putObjectInContext(APPEND_TO_REDSHIFT_TABLE, null);
    }

    private void readActiveRatingModels() {
        List<RatingEngineSummary> summaries = ratingEngineProxy.getRatingEngineSummaries(customerSpace);
        if (CollectionUtils.isNotEmpty(summaries)) {
            List<RatingModelContainer> activeModels = Flux.fromIterable(summaries) //
                    .parallel().runOn(Schedulers.parallel()) //
                    .map(this::getValidRatingModel) //
                    .filter(Objects::nonNull) //
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
        String engineId = summary.getId();
        RatingEngine engine = ratingEngineProxy.getRatingEngine(customerSpace, engineId);
        RatingModel ratingModel = engine.getActiveModel();
        if (ratingModel != null) {
            if (RatingEngineType.AI_BASED.equals(summary.getType())) {
                boolean isValid = isValidAIModel((AIModel) ratingModel);
                if (!isValid) {
                    log.warn("AI rating model " + ratingModel.getId() + " is not ready for scoring.");
                    return null;
                }
            } else if (RatingEngineType.RULE_BASED.equals(summary.getType())) {
                boolean isValid = isValidRuleBasedModel((RuleBasedModel) ratingModel);
                if (!isValid) {
                    log.warn("Rule based rating model " + ratingModel.getId() + " is not ready for scoring.");
                    return null;
                }
            }
            return new RatingModelContainer(ratingModel, summary);
        } else {
            return null;
        }
    }

    private boolean isValidAIModel(AIModel model) {
        if (model.getModelSummary() == null) {
            log.warn("AI model " + model.getId() + " has null model summary.");
            return false;
        }
        if (StringUtils.isBlank(model.getModelSummary().getId())) {
            log.warn("AI model " + model.getId() + " has black model guid.");
            return false;
        }
        return true;
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

}
