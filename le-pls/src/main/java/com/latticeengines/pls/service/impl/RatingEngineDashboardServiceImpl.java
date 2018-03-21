package com.latticeengines.pls.service.impl;

import java.util.List;
import java.util.Map;

import javax.inject.Inject;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import com.latticeengines.db.exposed.util.MultiTenantContext;
import com.latticeengines.domain.exposed.cdl.RatingEngineDependencyType;
import com.latticeengines.domain.exposed.metadata.MetadataSegment;
import com.latticeengines.domain.exposed.pls.CoverageInfo;
import com.latticeengines.domain.exposed.pls.Play;
import com.latticeengines.domain.exposed.pls.RatingEngine;
import com.latticeengines.domain.exposed.pls.RatingEngineDashboard;
import com.latticeengines.domain.exposed.pls.RatingEngineSummary;
import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.domain.exposed.util.RatingEngineUtils;
import com.latticeengines.pls.service.RatingCoverageService;
import com.latticeengines.pls.service.RatingEngineDashboardService;
import com.latticeengines.proxy.exposed.cdl.PlayProxy;
import com.latticeengines.proxy.exposed.cdl.RatingEngineProxy;

@Component("ratingEngineDashboardService")
public class RatingEngineDashboardServiceImpl extends RatingEngineTemplate implements RatingEngineDashboardService {

    private static Logger log = LoggerFactory.getLogger(RatingEngineDashboardServiceImpl.class);

    @Inject
    private RatingEngineProxy ratingEngineProxy;

    @Inject
    private RatingCoverageService ratingCoverageService;

    @Inject
    private PlayProxy playProxy;

    @Override
    public RatingEngineDashboard getRatingsDashboard(String ratingEngineId) {
        Tenant tenant = MultiTenantContext.getTenant();

        log.info(String.format("Loading rating dashboard for : %s", ratingEngineId));

        RatingEngineDashboard dashboard = new RatingEngineDashboard();

        // get rating engine summary
        RatingEngine ratingEngine = ratingEngineProxy.getRatingEngine(tenant.getId(), ratingEngineId);
        RatingEngineSummary ratingEngineSummary = constructRatingEngineSummary(ratingEngine, tenant.getId());
        log.info(String.format("Step 1 - Loading rating engine summary completed for : %s", ratingEngineId));

        // get coverage info
        CoverageInfo coverageInfo = RatingEngineUtils.getCoverageInfo(ratingEngine);
        log.info(String.format("Step 2 - Loading ratings coverage completed for : %s", ratingEngineId));

        // get segment info
        MetadataSegment segment = ratingEngine.getSegment();

        // get related plays
        List<Play> plays = playProxy.getPlays(tenant.getId(), false, ratingEngineId);
        log.info(String.format("Step 3 - Loading related plays completed for : %s", ratingEngineId));

        // get dependencies
        Map<RatingEngineDependencyType, List<String>> dependencies = ratingEngineProxy
                .getRatingEngineDependencies(tenant.getId(), ratingEngineId);
        log.info(String.format("Step 3.1 - Loading related dependencies completed for : %s", ratingEngineId));

        dashboard.setSummary(ratingEngineSummary);
        dashboard.setCoverageInfo(coverageInfo);
        dashboard.setSegment(segment);
        dashboard.setPlays(plays);
        dashboard.setDependencies(dependencies);

        return dashboard;
    }
}
