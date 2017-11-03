package com.latticeengines.dataflow.runtime.cascading.propdata.util.stats.bucket.impl;

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.collections4.CollectionUtils;

import com.latticeengines.dataflow.runtime.cascading.propdata.util.stats.bucket.AttrStatsDetailsMergeTool;
import com.latticeengines.domain.exposed.datacloud.statistics.AttributeStats;
import com.latticeengines.domain.exposed.datacloud.statistics.Bucket;
import com.latticeengines.domain.exposed.datacloud.statistics.Buckets;

import edu.emory.mathcs.backport.java.util.Collections;

public abstract class AttrStatsDetailsMergeTemplate implements AttrStatsDetailsMergeTool, Serializable {

    private static final long serialVersionUID = -8516799137018006941L;

    protected abstract void combineIndividualBuckets(Buckets resultBucketsObj, Buckets secondBucketsObj)
            throws IOException;

    public AttributeStats merge(AttributeStats firstStatsDetails,
                                AttributeStats secondStatsDetails, boolean printTop) {
        if (firstStatsDetails == null || secondStatsDetails == null) {
            if (firstStatsDetails != null) {
                return firstStatsDetails;
            } else if (secondStatsDetails != null) {
                return secondStatsDetails;
            } else {
                return null;
            }
        }

        AttributeStats resultAttributeStatsDetails = new AttributeStats();
        if (printTop) {
            System.out.println("First count " //
                    + firstStatsDetails.getNonNullCount() + "Second count "//
                    + secondStatsDetails.getNonNullCount());
        }
        setNonNullCount(firstStatsDetails, secondStatsDetails, resultAttributeStatsDetails);

        Buckets combinedBuckets = mergeBuckets(firstStatsDetails.getBuckets(), //
                secondStatsDetails.getBuckets(), printTop);

        resultAttributeStatsDetails.setBuckets(combinedBuckets);

        return resultAttributeStatsDetails;
    }

    protected void setNonNullCount(AttributeStats firstStatsDetails, //
                                   AttributeStats secondStatsDetails, //
                                   AttributeStats resultAttributeStatsDetails) {
        resultAttributeStatsDetails.setNonNullCount(firstStatsDetails.getNonNullCount() //
                + secondStatsDetails.getNonNullCount());
    }

    private Buckets mergeBuckets(Buckets firstBucketObj, //
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

                combineIndividualBuckets(resultBucketsObj, secondBucketsObj);

                if (resultBucketsObj.getBucketList() != null //
                        && resultBucketsObj.getBucketList().size() > 1) {
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
}
