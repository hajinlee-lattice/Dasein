package com.latticeengines.apps.lp.controller;

import java.util.List;
import java.util.Map;

import javax.inject.Inject;

import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

import com.latticeengines.apps.lp.service.BucketedScoreService;
import com.latticeengines.domain.exposed.SimpleBooleanResponse;
import com.latticeengines.domain.exposed.pls.BucketMetadata;
import com.latticeengines.domain.exposed.serviceapps.lp.CreateBucketMetadataRequest;

import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;

@Api(value = "bucketedscore", description = "REST resource for bucketed scores")
@RestController
@RequestMapping("/customerspaces/{customerSpace}/bucketedscore")
public class BucketedScoreResource {

    @Inject
    private BucketedScoreService bucketedScoreService;

    @GetMapping(value = "/abcdbuckets")
    @ResponseBody
    @ApiOperation(value = "Create ABCD Buckets")
    public SimpleBooleanResponse createABCDBuckets(@PathVariable String customerSpace,
                                                   @RequestBody CreateBucketMetadataRequest request) {
        bucketedScoreService.createABCDBucketsForModel(request);
        return SimpleBooleanResponse.successResponse();
    }

    @GetMapping(value = "/abcdbuckets/model/{modelGuid}")
    @ResponseBody
    @ApiOperation(value = "Get ABCD Buckets history info by model GUID")
    public Map<Long, List<BucketMetadata>> getABCDBucketsByModelGuid(@PathVariable String customerSpace,
            @PathVariable String modelGuid) {
        return bucketedScoreService.getModelBucketMetadataGroupedByCreationTimes(modelGuid);
    }

    @GetMapping(value = "/uptodateabcdbuckets/model/{modelGuid}")
    @ResponseBody
    @ApiOperation(value = "Get up-to-date ABCD Buckets info by model GUID")
    public List<BucketMetadata> getUpToDateABCDBucketsByModelGuid(@PathVariable String customerSpace,
            @PathVariable String modelGuid) {
        return bucketedScoreService.getUpToDateModelBucketMetadata(modelGuid);
    }

    @GetMapping(value = "/abcdbuckets/engine/{engineId}")
    @ResponseBody
    @ApiOperation(value = "Get ABCD Buckets info by rating engine Id")
    public List<BucketMetadata> getUpToDateABCDBucketsByEngineId(@PathVariable String customerSpace,
                                                       @PathVariable String engineId) {
        return bucketedScoreService.getABCDBucketsByRatingEngine(engineId);
    }

}
