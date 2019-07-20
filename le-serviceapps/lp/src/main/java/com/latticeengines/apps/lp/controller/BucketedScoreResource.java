package com.latticeengines.apps.lp.controller;

import java.util.List;
import java.util.Map;

import javax.inject.Inject;

import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

import com.latticeengines.apps.lp.service.BucketedScoreService;
import com.latticeengines.domain.exposed.SimpleBooleanResponse;
import com.latticeengines.domain.exposed.pls.BucketMetadata;
import com.latticeengines.domain.exposed.pls.BucketedScoreSummary;
import com.latticeengines.domain.exposed.serviceapps.lp.CreateBucketMetadataRequest;
import com.latticeengines.domain.exposed.serviceapps.lp.UpdateBucketMetadataRequest;

import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;
import io.swagger.annotations.ApiParam;

@Api(value = "bucketedscore", description = "REST resource for bucketed scores")
@RestController
@RequestMapping("/customerspaces/{customerSpace}/bucketedscore")
public class BucketedScoreResource {

    @Inject
    private BucketedScoreService bucketedScoreService;

    @PostMapping(value = "/abcdbuckets")
    @ResponseBody
    @ApiOperation(value = "Create ABCD Buckets")
    public SimpleBooleanResponse createABCDBuckets(@PathVariable String customerSpace,
            @RequestBody CreateBucketMetadataRequest request) {
        bucketedScoreService.createABCDBuckets(request);
        return SimpleBooleanResponse.successResponse();
    }

    @PutMapping(value = "/abcdbuckets")
    @ResponseBody
    @ApiOperation(value = "Create ABCD Buckets")
    public List<BucketMetadata> updateABCDBuckets(@PathVariable String customerSpace,
            @RequestBody UpdateBucketMetadataRequest request) {
        return bucketedScoreService.updateABCDBuckets(request);
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
        return bucketedScoreService.getABCDBucketsByModelGuid(modelGuid);
    }

    @GetMapping(value = "/modelabcdbuckets/model/{modelGuid}")
    @ResponseBody
    @ApiOperation(value = "Get Model ABCD Buckets info by model GUID")
    public List<BucketMetadata> getModelABCDBucketsByModelGuid(@PathVariable String customerSpace,
            @PathVariable String modelGuid) {
        return bucketedScoreService.getModelABCDBucketsByModelGuid(modelGuid);
    }
    
    @GetMapping(value = "/publishedbuckets/model")
    @ResponseBody
    @ApiOperation(value = "Get all up-to-date ABCD Buckets info from list of by model GUID")
    public Map<String, List<BucketMetadata>> getAllPublishedBucketMetadataByModelSummaryIdList(
            @PathVariable String customerSpace, @ApiParam(value = "List of model summary ids", required = false) //
            @RequestParam(value = "model-summary-id", required = false) List<String> modelSummaryIdList) {
        return bucketedScoreService.getAllPublishedBucketMetadataByModelSummaryIdList(modelSummaryIdList);
    }

    @GetMapping(value = "/publishedbuckets/model/{modelSummaryId}")
    @ResponseBody
    @ApiOperation(value = "Get up-to-date ABCD Buckets info by model GUID")
    public List<BucketMetadata> getPublishedBucketMetadataByModelGuid(@PathVariable String customerSpace,
            @PathVariable String modelSummaryId) {
        return bucketedScoreService.getPublishedBucketMetadataByModelGuid(modelSummaryId);
    }

    @GetMapping(value = "/summary/model/{modelGuid}")
    @ResponseBody
    @ApiOperation(value = "Get bucketed score summary for model GUID")
    public BucketedScoreSummary getBucketedScoreSummary(@PathVariable String customerSpace,
            @PathVariable String modelGuid) {
        return bucketedScoreService.getBucketedScoreSummaryByModelGuid(modelGuid);
    }

    @PostMapping(value = "/summary/model/{modelGuid}")
    @ResponseBody
    @ApiOperation(value = "Create or update bucketed score summary for model GUID")
    public BucketedScoreSummary createBucketedScoreSummary(@PathVariable String customerSpace,
            @PathVariable String modelGuid, @RequestBody BucketedScoreSummary summary) {
        return bucketedScoreService.createOrUpdateBucketedScoreSummary(modelGuid, summary);
    }

    @GetMapping(value = "/abcdbuckets/engine/{engineId}")
    @ResponseBody
    @ApiOperation(value = "Get ABCD Buckets for given rating engine Id grouped by creation times")
    public Map<Long, List<BucketMetadata>> getABCDBucketsByEngineId(@PathVariable String customerSpace,
            @PathVariable String engineId) {
        return bucketedScoreService.getRatingEngineBucketMetadataGroupedByCreationTimes(engineId);
    }

    @GetMapping(value = "/abcdbuckets/ratingengines/{ratingEngineId}")
    @ResponseBody
    @ApiOperation(value = "Get all ABCD Buckets created for a given rating engine Id")
    public List<BucketMetadata> getAllBucketsByRatingEngineId(@PathVariable String customerSpace,
            @PathVariable String ratingEngineId) {
        return bucketedScoreService.getAllBucketsByRatingEngineId(ratingEngineId);
    }

    @GetMapping(value = "/publishedbuckets/ratingengines/{ratingEngineId}")
    @ResponseBody
    @ApiOperation(value = "Get all published ABCD Buckets created for a given rating engine Id")
    public List<BucketMetadata> getAllPublishedBucketsByRatingEngineId(@PathVariable String customerSpace,
            @PathVariable String ratingEngineId) {
        return bucketedScoreService.getAllPublishedBucketsByRatingEngineId(ratingEngineId);
    }

    @GetMapping(value = "/uptodateabcdbuckets/engine/{engineId}")
    @ResponseBody
    @ApiOperation(value = "Get ABCD Buckets info by rating engine Id")
    public List<BucketMetadata> getUpToDateABCDBucketsByEngineId(@PathVariable String customerSpace,
            @PathVariable String engineId) {
        return bucketedScoreService.getABCDBucketsByRatingEngineId(engineId);
    }
}
