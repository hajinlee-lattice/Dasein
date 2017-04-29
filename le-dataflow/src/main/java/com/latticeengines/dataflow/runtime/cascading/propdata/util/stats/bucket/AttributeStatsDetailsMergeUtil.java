package com.latticeengines.dataflow.runtime.cascading.propdata.util.stats.bucket;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.collections.CollectionUtils;

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.latticeengines.domain.exposed.datacloud.statistics.AttributeStatsDetails;
import com.latticeengines.domain.exposed.datacloud.statistics.Bucket;
import com.latticeengines.domain.exposed.datacloud.statistics.Buckets;

import edu.emory.mathcs.backport.java.util.Collections;

public class AttributeStatsDetailsMergeUtil {

    public static AttributeStatsDetails addStatsDetails(AttributeStatsDetails firstStatsDetails,
            AttributeStatsDetails secondStatsDetails, boolean printTop) {
        if (firstStatsDetails == null || secondStatsDetails == null) {
            if (firstStatsDetails != null) {
                return firstStatsDetails;
            } else if (secondStatsDetails != null) {
                return secondStatsDetails;
            } else {
                return null;
            }
        }

        AttributeStatsDetails resultAttributeStatsDetails = new AttributeStatsDetails();
        if (printTop) {
            System.out.println("First count " + firstStatsDetails.getNonNullCount() + "Second count "
                    + secondStatsDetails.getNonNullCount());
        }
        resultAttributeStatsDetails.setNonNullCount(firstStatsDetails.getNonNullCount() //
                + secondStatsDetails.getNonNullCount());

        Buckets combinedBuckets = addBuckets(firstStatsDetails.getBuckets(), //
                secondStatsDetails.getBuckets(), printTop);

        resultAttributeStatsDetails.setBuckets(combinedBuckets);

        return resultAttributeStatsDetails;
    }

    public static Buckets addBuckets(Buckets firstBucketObj, //
            Buckets secondBucketsObj, boolean printTop) {
        if (firstBucketObj == null && secondBucketsObj == null) {
            return null;
        } else if (firstBucketObj == null) {
            return secondBucketsObj;
        } else if (secondBucketsObj == null) {
            return firstBucketObj;
        } else if (firstBucketObj.getType() != secondBucketsObj.getType()) {
            throw new RuntimeException("Both arguments should have same bucket type");
        }

        try {
            Buckets resultBucketsObj = null;
            resultBucketsObj = firstBucketObj;

            if (CollectionUtils.isNotEmpty(secondBucketsObj.getBucketList())) {
                for (Bucket secondBucket : secondBucketsObj.getBucketList()) {
                    combineIndividualBucket(resultBucketsObj, secondBucket);
                }

                if (resultBucketsObj.getBucketList() != null && resultBucketsObj.getBucketList().size() > 1) {
                    List<Bucket> sortedBucketList = new ArrayList<>();
                    List<Long> bucketIds = new ArrayList<>();
                    Map<Long, Bucket> tempBucketMap = new HashMap<>();

                    for (Bucket bucket : resultBucketsObj.getBucketList()) {
                        bucketIds.add(bucket.getId());
                        tempBucketMap.put(bucket.getId(), bucket);
                    }

                    Collections.sort(bucketIds);

                    for (Long id : bucketIds) {
                        sortedBucketList.add(tempBucketMap.get(id));
                    }

                    resultBucketsObj.setBucketList(sortedBucketList);
                }
            }

            return resultBucketsObj;
        } catch (IOException ex) {
            throw new RuntimeException(ex);
        }
    }

    private static void combineIndividualBucket(Buckets resultBucketsObj, Bucket secondBucket)
            throws IOException, JsonParseException, JsonMappingException, JsonProcessingException {
        Bucket matchingResultBucket = findMatchingResultBucket(resultBucketsObj, secondBucket);

        if (matchingResultBucket == null) {
            matchingResultBucket = secondBucket;
            resultBucketsObj.getBucketList().add(matchingResultBucket);
        } else {
            if (matchingResultBucket.getEncodedCountList() == null) {
                matchingResultBucket.setCount(//
                        matchingResultBucket.getCount() + secondBucket.getCount());
            } else {
                combineEncodedCountLists(secondBucket, matchingResultBucket);
            }
        }
    }

    private static void combineEncodedCountLists(Bucket secondBucket, Bucket matchingResultBucket) {
        Long[] resEncodedCountList = matchingResultBucket.getEncodedCountList();
        Long[] secEncodedCountList = secondBucket.getEncodedCountList();

        if (resEncodedCountList.length >= secEncodedCountList.length) {
            for (int i = 0; i < secEncodedCountList.length; i++) {
                resEncodedCountList[i] = resEncodedCountList[i] + secEncodedCountList[i];
            }
        } else {
            Long[] newResEncodedCountList = new Long[secEncodedCountList.length];
            for (int i = 0; i < secEncodedCountList.length; i++) {
                newResEncodedCountList[i] = secEncodedCountList[i];
                if (i < resEncodedCountList.length) {
                    newResEncodedCountList[i] = newResEncodedCountList[i] + resEncodedCountList[i];
                }
            }
            matchingResultBucket.setEncodedCountList(newResEncodedCountList);
        }

        if (matchingResultBucket.getEncodedCountList().length > 0) {
            matchingResultBucket.setCount(matchingResultBucket.getEncodedCountList()[0]);
        }
    }

    private static Bucket findMatchingResultBucket(Buckets resultBucketsObj, Bucket secondBucket) {
        Bucket matchingResultBucket = null;
        if (CollectionUtils.isNotEmpty(resultBucketsObj.getBucketList())) {
            for (Bucket resultBucket : resultBucketsObj.getBucketList()) {
                if (resultBucket.getBucketLabel().equals(secondBucket.getBucketLabel())) {
                    matchingResultBucket = resultBucket;
                }
            }
        }
        return matchingResultBucket;
    }
}
