package com.latticeengines.pls.controller;

import java.util.List;
import java.util.Map;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

import com.latticeengines.db.exposed.util.MultiTenantContext;
import com.latticeengines.domain.exposed.pls.BucketMetadata;
import com.latticeengines.domain.exposed.pls.BucketedScoreSummary;
import com.latticeengines.pls.service.BucketedScoreService;

import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;

@Api(value = "bucketedscore", description = "REST resource for bucketed scores")
@RestController
@RequestMapping("/bucketedscore")
public class BucketedScoreResource {

    @Autowired
    private BucketedScoreService bucketedScoreService;

    @RequestMapping(value = "/summary/{modelId}", method = RequestMethod.GET, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Get BucketedScoresSummary for specific model")
    public BucketedScoreSummary getBuckedScoresSummary(@PathVariable String modelId) throws Exception {
        return bucketedScoreService.getBucketedScoreSummaryForModelId(modelId);
    }

    @RequestMapping(value = "/summary/ratingengine/{ratingId}/model/{modelId}", method = RequestMethod.GET, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Get BucketedScoresSummary for given Rating Engine Id and Model Id.")
    public BucketedScoreSummary getBuckedScoresSummaryBasedOnRatingEngineAndRatingModel(@PathVariable String ratingId,
            @PathVariable String modelId) throws Exception {
        return bucketedScoreService.getBuckedScoresSummaryBasedOnRatingEngineAndRatingModel(ratingId, modelId);
    }

    @RequestMapping(value = "/abcdbuckets/{modelId}", method = RequestMethod.GET, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Get ABCD Buckets history info for the model")
    public Map<Long, List<BucketMetadata>> getABCDBuckets(@PathVariable String modelId) {
        return bucketedScoreService.getModelBucketMetadataGroupedByCreationTimes(modelId);
    }

    @RequestMapping(value = "/abcdbuckets/ratingengine/{ratingId}", method = RequestMethod.GET, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Get ABCD Buckets history info given Rating Engine Id")
    public Map<Long, List<BucketMetadata>> getABCDBucketsBasedOnRatingEngineId(@PathVariable String ratingId) {
        return bucketedScoreService.getModelBucketMetadataGroupedByCreationTimesBasedOnRatingEngineId(ratingId);
    }

    @RequestMapping(value = "/abcdbuckets/uptodate/{modelId}", method = RequestMethod.GET, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Get up-to-date ABCD Buckets info for the model")
    public List<BucketMetadata> getUpToDateABCDBuckets(@PathVariable String modelId) {
        return bucketedScoreService.getUpToDateModelBucketMetadata(modelId);
    }

    @RequestMapping(value = "/abcdbuckets/uptodate/ratingengine/{ratingId}", method = RequestMethod.GET, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Get up-to-date ABCD Buckets info given Rating Engine Id")
    public List<BucketMetadata> getUpToDateABCDBucketsBasedOnRatingEngineId(@PathVariable String ratingId) {
        return bucketedScoreService.getUpToDateABCDBucketsBasedOnRatingEngineId(ratingId);
    }

    @RequestMapping(value = "/abcdbuckets/{modelId}", method = RequestMethod.POST, headers = "Accept=application/json")
    @ApiOperation(value = "Create a group of ABCD buckets")
    public void createABCDBuckets(@PathVariable String modelId, @RequestBody List<BucketMetadata> bucketMetadatas) {
        for (BucketMetadata bucketMetadata : bucketMetadatas) {
            bucketMetadata.setLastModifiedByUser(MultiTenantContext.getEmailAddress());
        }
        bucketedScoreService.createBucketMetadatas(modelId, bucketMetadatas);
    }

    @RequestMapping(value = "/abcdbuckets/ratingengine/{ratingId}/model/{modelId}", method = RequestMethod.POST, headers = "Accept=application/json")
    @ApiOperation(value = "Create a group of ABCD buckets given Rating Engine Id and Rating Model Id")
    public void createABCDBuckets(@PathVariable String ratingId, @PathVariable String modelId,
            @RequestBody List<BucketMetadata> bucketMetadatas) {
        bucketedScoreService.createBucketMetadatas(ratingId, modelId, bucketMetadatas,
                MultiTenantContext.getEmailAddress());
    }

}
