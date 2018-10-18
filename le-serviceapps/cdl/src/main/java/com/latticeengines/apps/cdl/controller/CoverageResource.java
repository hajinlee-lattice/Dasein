package com.latticeengines.apps.cdl.controller;

import javax.inject.Inject;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

import com.latticeengines.apps.cdl.service.RatingCoverageService;
import com.latticeengines.domain.exposed.ratings.coverage.RatingsCountRequest;
import com.latticeengines.domain.exposed.ratings.coverage.RatingsCountResponse;
import com.latticeengines.domain.exposed.ratings.coverage.RatingModelsCoverageRequest;
import com.latticeengines.domain.exposed.ratings.coverage.RatingModelsCoverageResponse;

import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;

@Api(value = "Coverage API", description = "REST resource for Coverage API")
@RestController
@RequestMapping("/customerspaces/{customerSpace}/coverage")
public class CoverageResource {

    private static final Logger log = LoggerFactory.getLogger(CoverageResource.class);

    @Inject
    private RatingCoverageService ratingCoverageService;

    @PostMapping("/facade")
    @ResponseBody
    @ApiOperation(value = "Get CoverageInfo for ids in Rating count request")
    public RatingsCountResponse getRatingEngineCoverageInfo(@PathVariable String customerSpace,
            @RequestBody RatingsCountRequest ratingModelSegmentIds) {
        return ratingCoverageService.getCoverageInfo(customerSpace, ratingModelSegmentIds);
    }

    @PostMapping("/segment/{segmentName}")
    @ResponseBody
    @ApiOperation(value = "Get Segments CoverageInfo for List of Rating Model Ids")
    public RatingModelsCoverageResponse getRatingEngineCoverageCountForSegment(@PathVariable String customerSpace,
            @PathVariable String segmentName, @RequestBody RatingModelsCoverageRequest ratingCoverageRequest) {
        return ratingCoverageService.getRatingsCoverageForSegment(customerSpace, segmentName, ratingCoverageRequest);
    }
}
