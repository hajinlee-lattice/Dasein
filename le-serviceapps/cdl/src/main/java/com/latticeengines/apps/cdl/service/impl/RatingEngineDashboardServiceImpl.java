package com.latticeengines.apps.cdl.service.impl;

import java.util.List;
import java.util.Map;

import javax.inject.Inject;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import com.latticeengines.apps.cdl.service.RatingEngineDashboardService;
import com.latticeengines.apps.cdl.service.RatingEngineService;
import com.latticeengines.domain.exposed.cdl.RatingEngineDependencyType;
import com.latticeengines.domain.exposed.metadata.MetadataSegment;
import com.latticeengines.domain.exposed.pls.Play;
import com.latticeengines.domain.exposed.pls.RatingEngine;
import com.latticeengines.domain.exposed.pls.RatingEngineDashboard;
import com.latticeengines.domain.exposed.pls.RatingEngineSummary;
import com.latticeengines.domain.exposed.ratings.coverage.CoverageInfo;
import com.latticeengines.domain.exposed.util.RatingEngineUtils;
import com.latticeengines.proxy.exposed.cdl.PlayProxy;

@Component("ratingEngineDashboardService")
public class RatingEngineDashboardServiceImpl extends RatingEngineTemplate implements RatingEngineDashboardService {

    private static Logger log = LoggerFactory.getLogger(RatingEngineDashboardServiceImpl.class);

    @Inject
    private RatingEngineService ratingEngineService;

    @Inject
    private PlayProxy playProxy;

    @Override
    public RatingEngineDashboard getRatingsDashboard(String customerSpace, String ratingEngineId) {

        log.info(String.format("Loading rating dashboard for : %s", ratingEngineId));

        RatingEngineDashboard dashboard = new RatingEngineDashboard();

        // get rating engine summary
        RatingEngine ratingEngine = ratingEngineService.getRatingEngineById(ratingEngineId, true, true);
        RatingEngineSummary ratingEngineSummary = constructRatingEngineSummary(ratingEngine, customerSpace);
        log.info(String.format("Step 1 - Loading rating engine summary completed for : %s", ratingEngineId));

        // get coverage info
        CoverageInfo coverageInfo = RatingEngineUtils.getCoverageInfo(ratingEngine);
        log.info(String.format("Step 2 - Loading ratings coverage completed for : %s", ratingEngineId));

        // get segment info
        MetadataSegment segment = ratingEngine.getSegment();

        // get related plays
        List<Play> plays = playProxy.getPlays(customerSpace, false, ratingEngineId);
        log.info(String.format("Step 3 - Loading related plays completed for : %s", ratingEngineId));

        // get dependencies
        Map<RatingEngineDependencyType, List<String>> dependencies = ratingEngineService
                .getRatingEngineDependencies(customerSpace, ratingEngineId);
        log.info(String.format("Step 3.1 - Loading related dependencies completed for : %s", ratingEngineId));

        dashboard.setSummary(ratingEngineSummary);
        dashboard.setCoverageInfo(coverageInfo);
        dashboard.setSegment(segment);
        dashboard.setPlays(plays);
        dashboard.setDependencies(dependencies);

        return dashboard;
    }
}
