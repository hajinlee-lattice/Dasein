package com.latticeengines.cdl.workflow.steps.rating;

import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

import javax.inject.Inject;

import org.apache.commons.collections4.CollectionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import com.latticeengines.common.exposed.util.NamingUtils;
import com.latticeengines.domain.exposed.pls.RatingEngine;
import com.latticeengines.domain.exposed.pls.RatingEngineSummary;
import com.latticeengines.domain.exposed.pls.RatingEngineType;
import com.latticeengines.domain.exposed.pls.RatingModel;
import com.latticeengines.domain.exposed.pls.RatingModelContainer;
import com.latticeengines.domain.exposed.serviceflows.cdl.steps.process.ProcessRatingStepConfiguration;
import com.latticeengines.proxy.exposed.cdl.RatingEngineProxy;
import com.latticeengines.serviceflows.workflow.core.BaseWorkflowStep;

@Component("prepareForRating")
public class PrepareForRating extends BaseWorkflowStep<ProcessRatingStepConfiguration> {

    private static final Logger log = LoggerFactory.getLogger(PrepareForRating.class);

    @Inject
    private RatingEngineProxy ratingEngineProxy;

    private String customerSpace;
    private String rawRatingTableName;

    @Override
    public void execute() {
        customerSpace = configuration.getCustomerSpace().toString();
        rawRatingTableName = NamingUtils.timestamp("RawRating");
        putObjectInContext(RAW_RATING_TABLE_NAME, rawRatingTableName);
        readActiveRatingModels();
        putObjectInContext(TABLE_GOING_TO_REDSHIFT, null);
        putObjectInContext(APPEND_TO_REDSHIFT_TABLE, null);
    }

    private void readActiveRatingModels() {
        // TODO: need to filter based on engine status ?
        List<RatingEngineSummary> summaries = ratingEngineProxy.getRatingEngineSummaries(customerSpace);
        if (CollectionUtils.isNotEmpty(summaries)) {
            List<RatingModelContainer> activeModels = summaries.stream() //
                    .map(summary -> {
                        String engineId = summary.getId();
                        RatingEngine engine = ratingEngineProxy.getRatingEngine(customerSpace, engineId);
                        RatingModel ratingModel = engine.getActiveModel();
                        if (ratingModel != null) {
                            return new RatingModelContainer(ratingModel, summary);
                        } else {
                            return null;
                        }
                    }) //
                    .filter(Objects::nonNull) //
                    .filter(model -> RatingEngineType.RULE_BASED.equals(model.getEngineSummary().getType())) //
                    .collect(Collectors.toList());
            if (CollectionUtils.isNotEmpty(activeModels)) {
                log.info("Found " + activeModels.size() + " active rating models.");
                putObjectInContext(RATING_MODELS, activeModels);
            }
        } else {
            log.info("There is no rating engine summaries");
        }
    }

}
