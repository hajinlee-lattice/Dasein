package com.latticeengines.apps.cdl.controller;

import javax.inject.Inject;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

import com.latticeengines.apps.cdl.service.RatingCoverageService;
import com.latticeengines.domain.exposed.ratings.coverage.ProductsCoverageRequest;
import com.latticeengines.domain.exposed.ratings.coverage.RatingEnginesCoverageRequest;
import com.latticeengines.domain.exposed.ratings.coverage.RatingEnginesCoverageResponse;
import com.latticeengines.domain.exposed.ratings.coverage.RatingsCountRequest;
import com.latticeengines.domain.exposed.ratings.coverage.RatingsCountResponse;

import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;

@Api(value = "Coverage API", description = "REST resource for Coverage API")
@RestController
@RequestMapping("/customerspaces/{customerSpace}/coverage")
public class CoverageResource {

    private static final Logger log = LoggerFactory.getLogger(CoverageResource.class);

    @Inject
    private RatingCoverageService ratingCoverageService;

    /**
     * @param customerSpace
     * @param ratingModelSegmentIds
     *
     *            This method is deprecated as it is causing confusion while
     *            doing multiple things. Instead we are creating new api's for
     *            each of the usecase. Which will allow us to track api usages
     *            as well.
     *
     * @return
     */
    @PostMapping("/facade")
    @ResponseBody
    @ApiOperation(value = "Get CoverageInfo for ids in Rating count request")
    @Deprecated
    public RatingsCountResponse getRatingEngineCoverageInfo(@PathVariable String customerSpace,
            @RequestBody RatingsCountRequest ratingModelSegmentIds) {
        return ratingCoverageService.getCoverageInfo(customerSpace, ratingModelSegmentIds);
    }

    @PostMapping("/segment/{segmentName}")
    @ResponseBody
    @ApiOperation(value = "Get Segments CoverageInfo for List of Rating Model Ids")
    public RatingEnginesCoverageResponse getRatingEngineCoverageCountForSegment(@PathVariable String customerSpace,
            @PathVariable String segmentName, @RequestBody RatingEnginesCoverageRequest ratingCoverageRequest) {
        return ratingCoverageService.getRatingCoveragesForSegment(customerSpace, segmentName, ratingCoverageRequest);
    }

    @PostMapping("/segment/products")
    @ResponseBody
    @ApiOperation(value = "Get Segments CoverageInfo for List of Rating Model Ids")
    public RatingEnginesCoverageResponse getProductCoverageCountForSegments(
            @PathVariable String customerSpace,
            @RequestParam(value = "purchasedbeforeperiod", required = false) Integer purchasedBeforePeriod,
            @RequestBody ProductsCoverageRequest productsCoverageRequest) {
        return ratingCoverageService.getProductCoveragesForSegment(customerSpace,
                productsCoverageRequest, purchasedBeforePeriod);
    }
}
