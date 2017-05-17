package com.latticeengines.dataflow.runtime.cascading.propdata.util.stats.bucket.impl;

import java.io.IOException;

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.latticeengines.domain.exposed.datacloud.statistics.AttributeStatsDetails;
import com.latticeengines.domain.exposed.datacloud.statistics.Bucket;
import com.latticeengines.domain.exposed.datacloud.statistics.BucketType;
import com.latticeengines.domain.exposed.datacloud.statistics.Buckets;

public class AttrStatsDetailsDedupMergeUtil extends AttrStatsDetailsMergeTemplate {

    private static final long serialVersionUID = -2863931475162886958L;

    @Override
    protected void combineIndividualBuckets(Buckets resultBucketsObj, Buckets secondBucketsObj)
            throws IOException, JsonParseException, //
            JsonMappingException, JsonProcessingException {
        if (resultBucketsObj.getBucketList().size() > 1 //
                || secondBucketsObj.getBucketList().size() > 1) {
            if (resultBucketsObj.getType() == BucketType.Boolean //
                    && (resultBucketsObj.getBucketList().size() == 2//
                            && resultBucketsObj.getBucketList().get(0).getEncodedCountList() != null)) {
                // this is encoded bucket case which can have two buckets
            } else {
                throw new RuntimeException("Buckets dedup works only when both list have at max 1 items");
            }
        }

        Bucket rBucket = resultBucketsObj.getBucketList().get(0);
        Bucket sBucket = secondBucketsObj.getBucketList().get(0);

        if (secondBucketsObj.getType() == BucketType.Boolean) {
            if (secondBucketsObj.getBucketList().get(0).getEncodedCountList() != null) {
                Bucket rBucketTrue = resultBucketsObj.getBucketList().get(0);
                Bucket rBucketFalse = resultBucketsObj.getBucketList().get(1);

                // swap if needed
                if (rBucketTrue.getId() > rBucketFalse.getId()) {
                    Bucket temp = rBucketTrue;
                    rBucketTrue = rBucketFalse;
                    rBucketFalse = temp;
                }

                Bucket sBucketTrue = secondBucketsObj.getBucketList().get(0);
                Bucket sBucketFalse = secondBucketsObj.getBucketList().get(1);

                // swap if needed
                if (sBucketTrue.getId() > sBucketFalse.getId()) {
                    Bucket temp = sBucketTrue;
                    sBucketTrue = sBucketFalse;
                    sBucketFalse = temp;
                }

                combineEncodedCountLists(sBucketTrue, rBucketTrue, true);
                combineEncodedCountLists(sBucketFalse, rBucketFalse, false);

                resultBucketsObj.getBucketList().clear();
                resultBucketsObj.getBucketList().add(rBucketTrue);
                resultBucketsObj.getBucketList().add(rBucketFalse);
            } else {
                replaceBucketIfNeeded(resultBucketsObj, sBucket, //
                        rBucket.getId() > sBucket.getId());
            }
        } else {
            replaceBucketIfNeeded(resultBucketsObj, sBucket, //
                    rBucket.getId() < sBucket.getId());
        }
    }

    private void replaceBucketIfNeeded(Buckets resultBucketsObj, //
            Bucket sBucket, boolean condition) {
        if (condition) {
            resultBucketsObj.getBucketList().remove(0);
            resultBucketsObj.getBucketList().add(sBucket);
        }
    }

    @Override
    protected void setNonNullCount(AttributeStatsDetails firstStatsDetails, //
            AttributeStatsDetails secondStatsDetails, //
            AttributeStatsDetails resultAttributeStatsDetails) {
        long maxCount = Math.max(firstStatsDetails.getNonNullCount(), //
                secondStatsDetails.getNonNullCount());
        resultAttributeStatsDetails.setNonNullCount(maxCount);
    }

    private void combineEncodedCountLists(Bucket secondBucket, //
            Bucket matchingResultBucket, boolean forTrue) {
        Long[] resEncodedCountList = matchingResultBucket.getEncodedCountList();
        Long[] secEncodedCountList = secondBucket.getEncodedCountList();

        if (resEncodedCountList.length >= secEncodedCountList.length) {
            for (int i = 0; i < secEncodedCountList.length; i++) {
                if (forTrue) {
                    resEncodedCountList[i] = Math.max(resEncodedCountList[i], //
                            secEncodedCountList[i]);
                } else {
                    resEncodedCountList[i] = Math.min(resEncodedCountList[i], //
                            secEncodedCountList[i]);
                }
            }

            if (!forTrue) {
                for (int i = secEncodedCountList.length; i < resEncodedCountList.length; i++) {
                    resEncodedCountList[i] = 0L;
                }
            }
        } else {
            Long[] newResEncodedCountList = new Long[secEncodedCountList.length];
            for (int i = 0; i < secEncodedCountList.length; i++) {
                newResEncodedCountList[i] = secEncodedCountList[i];
                if (i < resEncodedCountList.length) {
                    if (forTrue) {
                        newResEncodedCountList[i] = Math.max(newResEncodedCountList[i], //
                                resEncodedCountList[i]);
                    } else {
                        newResEncodedCountList[i] = Math.min(newResEncodedCountList[i], //
                                resEncodedCountList[i]);
                    }
                }
            }
            matchingResultBucket.setEncodedCountList(newResEncodedCountList);
        }
    }

}
