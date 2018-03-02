package com.latticeengines.cdl.workflow.steps.rating;

import java.util.List;

import javax.inject.Inject;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.collections4.MapUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import com.latticeengines.common.exposed.util.NamingUtils;
import com.latticeengines.domain.exposed.pls.AIModel;
import com.latticeengines.domain.exposed.pls.ModelSummary;
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
                } else if (StringUtils.isBlank(aiModel.getModelGuid())) {
                    ModelSummary modelSummary = aiModel.getModelSummary();
                    aiModel.setModelGuid(modelSummary.getId());
                    aiModel.setModelSummary(null);
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
        } else if (StringUtils.isNotBlank(model.getModelGuid())) {
            valid = true;
        } else if (model.getModelSummary() == null  || StringUtils.isBlank(model.getModelSummary().getId())) {
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

}
