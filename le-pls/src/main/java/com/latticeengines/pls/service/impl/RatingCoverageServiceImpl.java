package com.latticeengines.pls.service.impl;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Random;

import org.springframework.stereotype.Component;

import com.latticeengines.domain.exposed.pls.CoverageInfo;
import com.latticeengines.domain.exposed.pls.RatingBucketCoverage;
import com.latticeengines.domain.exposed.pls.RatingModelIdPair;
import com.latticeengines.domain.exposed.pls.RatingsCountRequest;
import com.latticeengines.domain.exposed.pls.RatingsCountResponse;
import com.latticeengines.domain.exposed.pls.RuleBucketName;
import com.latticeengines.pls.service.RatingCoverageService;

@Component
public class RatingCoverageServiceImpl implements RatingCoverageService {

    @Override
    public RatingsCountResponse getCoverageInfo(RatingsCountRequest request) {
        if (request.getRatingEngineIds() != null) {
            return processRatingIds(request);
        } else if (request.getSegmentIds() != null) {
            return processSegmentIds(request);
        } else if (request.getRatingEngineModelIds() != null) {
            return processRatingEngineModelIds(request);
        }

        return null;
    }

    private RatingsCountResponse processRatingIds(RatingsCountRequest request) {
        RatingsCountResponse result = new RatingsCountResponse();
        HashMap<String, CoverageInfo> ratingEngineIdCoverageMap = new HashMap<>();
        result.setRatingEngineIdCoverageMap(ratingEngineIdCoverageMap);

        Random rand = new Random(System.currentTimeMillis());

        for (String ratingModelSegmentId : request.getRatingEngineIds()) {
            CoverageInfo coverageInfo = new CoverageInfo();
            Long accountCount = 5000L;
            accountCount += rand.nextInt(1000);
            Long contactCount = 7000L;
            contactCount += rand.nextInt(500);

            coverageInfo.setAccountCount(accountCount);
            coverageInfo.setContactCount(contactCount);

            List<RatingBucketCoverage> bucketCoverageCounts = new ArrayList<>();
            long totalSum = 0;
            int totalParts = 21;
            for (RuleBucketName bucket : RuleBucketName.values()) {
                int partsInBucket = bucket.ordinal() + 1;
                long countInBucket = (accountCount * partsInBucket) / totalParts;
                if (bucket == RuleBucketName.F) {
                    countInBucket = accountCount - totalSum;
                } else {
                    totalSum += countInBucket;
                }

                RatingBucketCoverage coveragePair = new RatingBucketCoverage();
                coveragePair.setBucket(bucket.getName());
                coveragePair.setCount(countInBucket);
                bucketCoverageCounts.add(coveragePair);
            }
            coverageInfo.setBucketCoverageCounts(bucketCoverageCounts);
            ratingEngineIdCoverageMap.put(ratingModelSegmentId, coverageInfo);
        }
        return result;
    }

    private RatingsCountResponse processSegmentIds(RatingsCountRequest request) {
        RatingsCountResponse result = new RatingsCountResponse();
        HashMap<String, CoverageInfo> segmentIdCoverageMap = new HashMap<>();
        result.setSegmentIdCoverageMap(segmentIdCoverageMap);

        Random rand = new Random(System.currentTimeMillis());

        for (String ratingModelSegmentId : request.getSegmentIds()) {
            CoverageInfo coverageInfo = new CoverageInfo();
            Long accountCount = 5000L;
            accountCount += rand.nextInt(1000);
            Long contactCount = 7000L;
            contactCount += rand.nextInt(500);

            coverageInfo.setAccountCount(accountCount);
            coverageInfo.setContactCount(contactCount);

            segmentIdCoverageMap.put(ratingModelSegmentId, coverageInfo);
        }
        return result;
    }

    private RatingsCountResponse processRatingEngineModelIds(RatingsCountRequest request) {
        RatingsCountResponse result = new RatingsCountResponse();
        HashMap<RatingModelIdPair, CoverageInfo> ratingEngineModelIdCoverageMap = new HashMap<>();
        result.setRatingEngineModelIdCoverageMap(ratingEngineModelIdCoverageMap);

        Random rand = new Random(System.currentTimeMillis());

        for (RatingModelIdPair ratingModelSegmentId : request.getRatingEngineModelIds()) {
            CoverageInfo coverageInfo = new CoverageInfo();
            Long accountCount = 5000L;
            accountCount += rand.nextInt(1000);
            Long contactCount = 7000L;
            contactCount += rand.nextInt(500);

            coverageInfo.setAccountCount(accountCount);
            coverageInfo.setContactCount(contactCount);

            List<RatingBucketCoverage> bucketCoverageCounts = new ArrayList<>();
            long totalSum = 0;
            int totalParts = 21;
            for (RuleBucketName bucket : RuleBucketName.values()) {
                int partsInBucket = bucket.ordinal() + 1;
                long countInBucket = (accountCount * partsInBucket) / totalParts;
                if (bucket == RuleBucketName.F) {
                    countInBucket = accountCount - totalSum;
                } else {
                    totalSum += countInBucket;
                }

                RatingBucketCoverage coveragePair = new RatingBucketCoverage();
                coveragePair.setBucket(bucket.getName());
                coveragePair.setCount(countInBucket);
                bucketCoverageCounts.add(coveragePair);
            }
            coverageInfo.setBucketCoverageCounts(bucketCoverageCounts);
            ratingEngineModelIdCoverageMap.put(ratingModelSegmentId, coverageInfo);
        }
        return result;
    }

}
