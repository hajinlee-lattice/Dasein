package com.latticeengines.proxy.exposed.cdl;

import static com.latticeengines.proxy.exposed.ProxyUtils.shortenCustomerSpace;

import org.springframework.stereotype.Component;

import com.latticeengines.domain.exposed.ratings.coverage.RatingModelsCoverageRequest;
import com.latticeengines.domain.exposed.ratings.coverage.RatingModelsCoverageResponse;
import com.latticeengines.domain.exposed.ratings.coverage.RatingsCountRequest;
import com.latticeengines.domain.exposed.ratings.coverage.RatingsCountResponse;
import com.latticeengines.proxy.exposed.MicroserviceRestApiProxy;

@Component("ratingCoverageProxy")
public class RatingCoverageProxy extends MicroserviceRestApiProxy {

    private static final String URL_PREFIX = "/customerspaces/{customerSpace}/coverage";

    protected RatingCoverageProxy() {
        super("cdl");
    }

    public RatingsCountResponse getCoverageInfo(String customerSpace, RatingsCountRequest request) {
        String url = constructUrl(URL_PREFIX + "/facade", shortenCustomerSpace(customerSpace));
        return post("getRatingsCoverage", url, request, RatingsCountResponse.class);
    }

    public RatingModelsCoverageResponse getCoverageInfoForSegment(String customerSpace, String segmentName, RatingModelsCoverageRequest request) {
        String url = constructUrl(URL_PREFIX + "/segment/{segmentName}", shortenCustomerSpace(customerSpace), segmentName);
        return post("getRatingsCoverageForSegment", url, request, RatingModelsCoverageResponse.class);
    }
}
