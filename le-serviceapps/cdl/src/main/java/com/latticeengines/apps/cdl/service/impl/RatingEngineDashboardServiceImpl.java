package com.latticeengines.apps.cdl.service.impl;

import static com.latticeengines.apps.cdl.service.impl.RatingModelServiceBase.getRatingModelService;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import javax.inject.Inject;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import com.latticeengines.apps.cdl.service.PlayService;
import com.latticeengines.apps.cdl.service.RatingEngineDashboardService;
import com.latticeengines.apps.cdl.service.RatingEngineService;
import com.latticeengines.apps.cdl.service.RatingModelService;
import com.latticeengines.domain.exposed.metadata.MetadataSegment;
import com.latticeengines.domain.exposed.pls.Play;
import com.latticeengines.domain.exposed.pls.RatingEngine;
import com.latticeengines.domain.exposed.pls.RatingEngineDashboard;
import com.latticeengines.domain.exposed.pls.RatingEngineSummary;
import com.latticeengines.domain.exposed.pls.RatingModel;
import com.latticeengines.domain.exposed.pls.RatingModelDTO;
import com.latticeengines.domain.exposed.ratings.coverage.CoverageInfo;

@Component("ratingEngineDashboardService")
public class RatingEngineDashboardServiceImpl extends RatingEngineTemplate implements RatingEngineDashboardService {

    private static Logger log = LoggerFactory.getLogger(RatingEngineDashboardServiceImpl.class);

    @Inject
    private RatingEngineService ratingEngineService;

    @Inject
    private PlayService playService;

    @SuppressWarnings("unchecked")
    @Override
    public RatingEngineDashboard getRatingsDashboard(String customerSpace, String ratingEngineId) {
        if (StringUtils.isBlank(ratingEngineId)) {
            log.warn("Null or empty rating engine ID, cannot build dashboard");
            return null;
        }

        log.info(String.format("Loading rating dashboard for : %s", ratingEngineId));

        RatingEngineDashboard dashboard = new RatingEngineDashboard();

        // get rating engine summary
        RatingEngine ratingEngine = ratingEngineService.getRatingEngineById(ratingEngineId, true, true);
        if (ratingEngine == null) {
            log.warn(String.format("Unable to find a rating engine by ID: %s for tenant: %s", ratingEngineId,
                    customerSpace));
            return null;
        }

        RatingEngineSummary ratingEngineSummary = constructRatingEngineSummary(ratingEngine, customerSpace);
        log.info(String.format("Step 1 - Loading rating engine summary completed for : %s", ratingEngineId));

        // get coverage info
        CoverageInfo coverageInfo = new CoverageInfo(ratingEngine.getSegment().getAccounts(),
                ratingEngine.getSegment().getContacts(), ratingEngineSummary.getBucketMetadata());
        log.info(String.format("Step 2 - Loading ratings coverage completed for : %s", ratingEngineId));

        // get segment info
        MetadataSegment segment = ratingEngine.getSegment();

        // get related plays
        List<Play> plays = playService.getAllFullPlays(false, ratingEngineId);
        log.info(String.format("Step 3 - Loading related plays completed for : %s", ratingEngineId));

        // get dependencies
        Map<String, List<String>> dependencies = ratingEngineService.getRatingEngineDependencies(customerSpace,
                ratingEngineId);
        log.info(String.format("Step 3.1 - Loading related dependencies completed for : %s", ratingEngineId));

        // get iterations
        RatingModelService<RatingModel> ratingModelService = getRatingModelService(ratingEngine.getType());
        List<RatingModelDTO> iterations = ratingModelService.getAllRatingModelsByRatingEngineId(ratingEngineId).stream()
                .map(RatingModelDTO::new).collect(Collectors.toList());

        log.info(String.format("Step 4 - Loading related model iterations for : %s", ratingEngineId));

        dashboard.setSummary(ratingEngineSummary);
        dashboard.setCoverageInfo(coverageInfo);
        dashboard.setSegment(segment);
        dashboard.setPlays(plays);
        dashboard.setDependencies(dependencies);
        dashboard.setIterations(iterations);

        return dashboard;
    }
}
