package com.latticeengines.cdl.workflow.listeners;

import java.util.List;
import java.util.Map;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.collections4.MapUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.pls.RatingEngine;
import com.latticeengines.domain.exposed.pls.RatingEngineSummary;
import com.latticeengines.domain.exposed.pls.RatingModel;
import com.latticeengines.proxy.exposed.cdl.RatingEngineProxy;

final class RatingEngineCountUtils {

    private static final Logger log = LoggerFactory.getLogger(RatingEngineCountUtils.class);

    static void updateRatingEngineCounts(final RatingEngineProxy ratingEngineProxy, final String customerSpace) {
        List<RatingEngineSummary> ratingEngineSummaries = ratingEngineProxy.getRatingEngineSummaries(customerSpace);
        if (CollectionUtils.isNotEmpty(ratingEngineSummaries)) {
            ratingEngineSummaries.forEach(summary -> {
                String engineId = summary.getId();
                RatingEngine ratingEngine = ratingEngineProxy.getRatingEngine(customerSpace, engineId);
                RatingModel ratingModel = ratingEngine.getActiveModel();
                if (ratingModel != null) {
                    ratingEngineProxy.updateRatingModel(customerSpace, engineId, ratingModel.getId(), ratingModel);
                    RatingEngine updatedEngine = ratingEngineProxy.getRatingEngine(customerSpace, engineId);
                    Map<String, Long> counts = updatedEngine.getCountsAsMap();
                    log.info("Updated the counts of rating engine " + engineId + " to "
                            + (MapUtils.isNotEmpty(counts) ? JsonUtils.pprint(counts) : null));
                }
            });
        }
    }

}
