package com.latticeengines.apps.lp.controller;

import java.util.List;
import java.util.Map;

import javax.inject.Inject;

import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

import com.latticeengines.apps.core.annotation.NoCustomerSpace;
import com.latticeengines.apps.lp.service.BucketedScoreService;
import com.latticeengines.domain.exposed.SimpleBooleanResponse;
import com.latticeengines.domain.exposed.pls.BucketMetadata;
import com.latticeengines.domain.exposed.pls.BucketedScoreSummary;
import com.latticeengines.domain.exposed.serviceapps.lp.CreateBucketMetadataRequest;

import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;

@Api(value = "bucketedscore", description = "REST resource for bucketed scores")
@RestController
@RequestMapping("/bucketedscore")
public class BucketedScoreResource {

    @Inject
    private BucketedScoreService bucketedScoreService;

    @PostMapping(value = "/abcdbuckets")
    @ResponseBody
    @ApiOperation(value = "Create ABCD Buckets")
    @NoCustomerSpace
    public SimpleBooleanResponse createABCDBuckets(@RequestBody CreateBucketMetadataRequest request) {
        bucketedScoreService.createABCDBuckets(request);
        return SimpleBooleanResponse.successResponse();
    }

    @GetMapping(value = "/abcdbuckets/model/{modelGuid}")
    @ResponseBody
    @ApiOperation(value = "Get ABCD Buckets history info by model GUID")
    @NoCustomerSpace
    public Map<Long, List<BucketMetadata>> getABCDBucketsByModelGuid(@PathVariable String modelGuid) {
        return bucketedScoreService.getModelBucketMetadataGroupedByCreationTimes(modelGuid);
    }

    @GetMapping(value = "/abcdbuckets/engine/{engineId}")
    @ResponseBody
    @ApiOperation(value = "Get ABCD Buckets info by rating engine Id")
    @NoCustomerSpace
    public Map<Long, List<BucketMetadata>> getABCDBucketsByEngineId(@PathVariable String engineId) {
        return bucketedScoreService.getRatingEngineBucketMetadataGroupedByCreationTimes(engineId);
    }

    @GetMapping(value = "/uptodateabcdbuckets/engine/{engineId}")
    @ResponseBody
    @ApiOperation(value = "Get ABCD Buckets info by rating engine Id")
    @NoCustomerSpace
    public List<BucketMetadata> getUpToDateABCDBucketsByEngineId(@PathVariable String engineId) {
        return bucketedScoreService.getABCDBucketsByRatingEngineId(engineId);
    }

    @GetMapping(value = "/uptodateabcdbuckets/model/{modelGuid}")
    @ResponseBody
    @ApiOperation(value = "Get up-to-date ABCD Buckets info by model GUID")
    @NoCustomerSpace
    public List<BucketMetadata> getUpToDateABCDBucketsByModelGuid(@PathVariable String modelGuid) {
        return bucketedScoreService.getABCDBucketsByModelGuid(modelGuid);
    }

    @GetMapping(value = "/summary/model/{modelGuid}")
    @ResponseBody
    @ApiOperation(value = "Get bucketed score summary for model GUID")
    @NoCustomerSpace
    public BucketedScoreSummary getBucketedScoreSummary(@PathVariable String modelGuid) {
        return bucketedScoreService.getBucketedScoreSummaryByModelGuid(modelGuid);
    }

    @PostMapping(value = "/summary/model/{modelGuid}")
    @ResponseBody
    @ApiOperation(value = "Create or update bucketed score summary for model GUID")
    @NoCustomerSpace
    public BucketedScoreSummary createBucketedScoreSummary(@PathVariable String modelGuid, @RequestBody BucketedScoreSummary summary) {
        return bucketedScoreService.createOrUpdateBucketedScoreSummary(modelGuid, summary);
    }

}
