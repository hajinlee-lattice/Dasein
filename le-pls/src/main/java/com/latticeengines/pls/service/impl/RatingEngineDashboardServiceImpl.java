package com.latticeengines.pls.service.impl;

import java.util.Arrays;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.latticeengines.domain.exposed.metadata.MetadataSegment;
import com.latticeengines.domain.exposed.pls.CoverageInfo;
import com.latticeengines.domain.exposed.pls.Play;
import com.latticeengines.domain.exposed.pls.RatingEngine;
import com.latticeengines.domain.exposed.pls.RatingEngineDashboard;
import com.latticeengines.domain.exposed.pls.RatingEngineSummary;
import com.latticeengines.domain.exposed.pls.RatingsCountRequest;
import com.latticeengines.domain.exposed.pls.RatingsCountResponse;
import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.pls.service.PlayService;
import com.latticeengines.pls.service.RatingCoverageService;
import com.latticeengines.pls.service.RatingEngineDashboardService;
import com.latticeengines.pls.service.RatingEngineService;
import com.latticeengines.security.exposed.util.MultiTenantContext;

@Component("ratingEngineDashboardService")
public class RatingEngineDashboardServiceImpl extends RatingEngineTemplate implements RatingEngineDashboardService {

    private static Logger log = LoggerFactory.getLogger(RatingEngineDashboardServiceImpl.class);

    @Autowired
    private RatingEngineService ratingEngineService;

    @Autowired
    private RatingCoverageService ratingCoverageService;

    @Autowired
    private PlayService playService;

    @Override
    public RatingEngineDashboard getRatingsDashboard(String ratingEngineId) {
        Tenant tenant = MultiTenantContext.getTenant();

        log.info(String.format("Loading rating dashboard for : %s", ratingEngineId));

        RatingEngineDashboard dashboard = new RatingEngineDashboard();

        // get rating engine summary
        RatingEngine ratingEngine = ratingEngineService.getRatingEngineById(ratingEngineId, true);
        RatingEngineSummary ratingEngineSummary = constructRatingEngineSummary(ratingEngine, tenant.getId());
        log.info(String.format("Step 1 - Loading rating engine summary completed for : %s", ratingEngineId));

        // get coverage info
        RatingsCountRequest coverageRequest = new RatingsCountRequest();
        coverageRequest.setRatingEngineIds(Arrays.asList(ratingEngine.getId()));
        RatingsCountResponse ratingsCountResponse = ratingCoverageService.getCoverageInfo(coverageRequest);
        CoverageInfo coverageInfo = ratingsCountResponse.getRatingEngineIdCoverageMap().get(ratingEngine.getId());
        log.info(String.format("Step 2 - Loading ratings coverage completed for : %s", ratingEngineId));

        // get segment info
        MetadataSegment segment = ratingEngine.getSegment();

        // get related plays
        List<Play> plays = playService.getAllFullPlays(false, ratingEngineId);
        log.info(String.format("Step 3 - Loading related plays completed for : %s", ratingEngineId));

        dashboard.setSummary(ratingEngineSummary);
        dashboard.setCoverageInfo(coverageInfo);
        dashboard.setSegment(segment);
        dashboard.setPlays(plays);

        return dashboard;
    }
}
