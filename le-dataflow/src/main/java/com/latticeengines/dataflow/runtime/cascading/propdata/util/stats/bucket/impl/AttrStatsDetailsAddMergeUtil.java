package com.latticeengines.dataflow.runtime.cascading.propdata.util.stats.bucket.impl;

import java.io.IOException;

import org.apache.commons.collections4.CollectionUtils;

import com.latticeengines.domain.exposed.datacloud.statistics.Bucket;
import com.latticeengines.domain.exposed.datacloud.statistics.Buckets;

public class AttrStatsDetailsAddMergeUtil extends AttrStatsDetailsMergeTemplate {

    private static final long serialVersionUID = -1579787764379239L;

    @Override
    protected void combineIndividualBuckets(Buckets resultBucketsObj, Buckets secondBucketsObj)
            throws IOException {
        for (Bucket secondBucket : secondBucketsObj.getBucketList()) {

            Bucket matchingResultBucket = //
                    findMatchingResultBucket(resultBucketsObj, secondBucket);

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
    }

    private void combineEncodedCountLists(Bucket secondBucket, Bucket matchingResultBucket) {
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
                    newResEncodedCountList[i] = //
                            newResEncodedCountList[i] + resEncodedCountList[i];
                }
            }
            matchingResultBucket.setEncodedCountList(newResEncodedCountList);
        }
    }

    private Bucket findMatchingResultBucket(Buckets resultBucketsObj, Bucket secondBucket) {
        Bucket matchingResultBucket = null;
        if (CollectionUtils.isNotEmpty(resultBucketsObj.getBucketList())) {
            for (Bucket resultBucket : resultBucketsObj.getBucketList()) {
                if (resultBucket.getLabel().equals(secondBucket.getLabel())) {
                    matchingResultBucket = resultBucket;
                }
            }
        }
        return matchingResultBucket;
    }
}
