package com.latticeengines.pls.controller;

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

import com.latticeengines.domain.exposed.pls.BucketMetadata;
import com.latticeengines.domain.exposed.pls.BucketedScoreSummary;
import com.latticeengines.pls.service.BucketedScoreService;

import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;

@Api(value = "bucketedscore", description = "REST resource for bucketed scores")
@RestController
@RequestMapping("/bucketedscore")
public class BucketedScoreResource {

    @Inject
    private BucketedScoreService bucketedScoreService;

    @GetMapping("/summary/{modelId}")
    @ResponseBody
    @ApiOperation(value = "Get BucketedScoresSummary for specific model")
    public BucketedScoreSummary getBuckedScoresSummary(@PathVariable String modelId) throws Exception {
        return bucketedScoreService.getBucketedScoreSummaryForModelId(modelId);
    }

    @GetMapping("/summary/ratingengine/{ratingId}/model/{modelId}")
    @ResponseBody
    @ApiOperation(value = "Get BucketedScoresSummary for given Rating Engine Id and Model Id.")
    public BucketedScoreSummary getBuckedScoresSummaryBasedOnRatingEngineAndRatingModel(@PathVariable String ratingId,
            @PathVariable String modelId) throws Exception {
        return bucketedScoreService.getBuckedScoresSummaryBasedOnRatingEngineAndRatingModel(ratingId, modelId);
    }

    @GetMapping("/abcdbuckets/{modelId}")
    @ResponseBody
    @ApiOperation(value = "Get ABCD Buckets history info for the model")
    public Map<Long, List<BucketMetadata>> getABCDBuckets(@PathVariable String modelId) {
        return bucketedScoreService.getModelBucketMetadataGroupedByCreationTimes(modelId);
    }

    @GetMapping("/abcdbuckets/ratingengine/{ratingId}")
    @ResponseBody
    @ApiOperation(value = "Get ABCD Buckets history info given Rating Engine Id")
    public Map<Long, List<BucketMetadata>> getABCDBucketsBasedOnRatingEngineId(@PathVariable String ratingId) {
        return bucketedScoreService.getModelBucketMetadataGroupedByCreationTimesBasedOnRatingEngineId(ratingId);
    }

    @GetMapping("/abcdbuckets/uptodate/{modelId}")
    @ResponseBody
    @ApiOperation(value = "Get up-to-date ABCD Buckets info for the model")
    public List<BucketMetadata> getUpToDateABCDBuckets(@PathVariable String modelId) {
        return bucketedScoreService.getUpToDateModelBucketMetadata(modelId);
    }

    @PostMapping("/abcdbuckets/{modelGuid}")
    @ApiOperation(value = "Create a group of ABCD buckets")
    public void createABCDBuckets(@PathVariable String modelGuid, @RequestBody List<BucketMetadata> bucketMetadatas) {
        bucketedScoreService.createBucketMetadatas(modelGuid, bucketMetadatas);
    }

    @PostMapping("/abcdbuckets/ratingengine/{ratingId}/model/{modelGuid}")
    @ApiOperation(value = "Create a group of ABCD buckets given Rating Engine Id and Rating Model Id")
    public void createABCDBuckets(@PathVariable String ratingId, @PathVariable String modelGuid,
            @RequestBody List<BucketMetadata> bucketMetadatas) {
        bucketedScoreService.createBucketMetadatas(ratingId, modelGuid, bucketMetadatas);
    }

}
