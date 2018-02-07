package com.latticeengines.pls.service.impl;

import javax.inject.Inject;

import org.springframework.stereotype.Component;

import com.latticeengines.db.exposed.util.MultiTenantContext;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.pls.RatingsCountRequest;
import com.latticeengines.domain.exposed.pls.RatingsCountResponse;
import com.latticeengines.pls.service.RatingCoverageService;
import com.latticeengines.proxy.exposed.cdl.RatingEngineProxy;

@Component("ratingCoverageService")
public class RatingCoverageServiceImpl implements RatingCoverageService {

    @Inject
    private RatingEngineProxy ratingEngineProxy;

    @Override
    public RatingsCountResponse getCoverageInfo(RatingsCountRequest request) {
        CustomerSpace customerSpace = MultiTenantContext.getCustomerSpace();
        return ratingEngineProxy.getRatingEngineCoverageInfo(customerSpace.toString(), request);
    }
}
