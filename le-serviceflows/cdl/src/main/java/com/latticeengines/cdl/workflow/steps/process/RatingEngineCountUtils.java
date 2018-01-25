package com.latticeengines.cdl.workflow.steps.process;

import java.util.List;
import java.util.Map;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.collections4.MapUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.proxy.exposed.cdl.RatingEngineProxy;

final class RatingEngineCountUtils {

    private static final Logger log = LoggerFactory.getLogger(RatingEngineCountUtils.class);

    static void updateRatingEngineCounts(final RatingEngineProxy ratingEngineProxy, final String customerSpace) {
        List<String> ratingEngineIds = ratingEngineProxy.getRatingEngineIds(customerSpace);
        if (CollectionUtils.isNotEmpty(ratingEngineIds)) {
            log.info("Going to update " + ratingEngineIds.size() + " rating engines.");
            ratingEngineIds.forEach(engineId -> {
                try {
                    Map<String, Long> counts = ratingEngineProxy.updateRatingEngineCounts(customerSpace, engineId);
                    log.info("Updated the counts of rating engine " + engineId + " to "
                            + (MapUtils.isNotEmpty(counts) ? JsonUtils.pprint(counts) : null));
                } catch (Exception e) {
                    log.error("Failed to update the counts of rating engine " + engineId, e);
                }
            });
        } else {
            log.info("There is no rating engines to update.");
        }
    }

}
