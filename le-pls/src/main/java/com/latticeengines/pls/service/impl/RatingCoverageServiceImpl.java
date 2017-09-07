package com.latticeengines.pls.service.impl;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.latticeengines.domain.exposed.metadata.MetadataSegment;
import com.latticeengines.domain.exposed.pls.CoverageInfo;
import com.latticeengines.domain.exposed.pls.RatingBucketCoverage;
import com.latticeengines.domain.exposed.pls.RatingEngine;
import com.latticeengines.domain.exposed.pls.RatingModel;
import com.latticeengines.domain.exposed.pls.RatingModelIdPair;
import com.latticeengines.domain.exposed.pls.RatingsCountRequest;
import com.latticeengines.domain.exposed.pls.RatingsCountResponse;
import com.latticeengines.domain.exposed.pls.RuleBucketName;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.domain.exposed.query.frontend.FrontEndQuery;
import com.latticeengines.domain.exposed.query.frontend.FrontEndRestriction;
import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.pls.service.MetadataSegmentService;
import com.latticeengines.pls.service.RatingCoverageService;
import com.latticeengines.pls.service.RatingEngineService;
import com.latticeengines.proxy.exposed.objectapi.EntityProxy;
import com.latticeengines.security.exposed.util.MultiTenantContext;

@Component("ratingCoverageService")
public class RatingCoverageServiceImpl implements RatingCoverageService {

    @Autowired
    private RatingEngineService ratingEngineService;

    @Autowired
    private MetadataSegmentService metadataSegmentService;

    @Autowired
    private EntityProxy entityProxy;

    Random rand = new Random(System.currentTimeMillis());

    @Override
    public RatingsCountResponse getCoverageInfo(RatingsCountRequest request) {
        return getCoverageInfo(request, true);
    }

    @Override
    public RatingsCountResponse getCoverageInfo(RatingsCountRequest request, boolean getDummyCoverage) {
        if (request.getRatingEngineIds() != null) {
            return getDummyCoverage ? processRatingIdsDummy(request) : processRatingIds(request);
        } else if (request.getSegmentIds() != null) {
            return getDummyCoverage ? processSegmentIdsDummy(request) : processSegmentIds(request);
        } else if (request.getRatingEngineModelIds() != null) {
            return processRatingEngineModelIds(request);
        }

        return null;
    }

    private RatingsCountResponse processRatingIds(RatingsCountRequest request) {
        Tenant tenent = MultiTenantContext.getTenant();

        RatingsCountResponse result = new RatingsCountResponse();
        Map<String, CoverageInfo> ratingEngineIdCoverageMap = new ConcurrentHashMap<>();
        result.setRatingEngineIdCoverageMap(ratingEngineIdCoverageMap);

        request.getRatingEngineIds() //
                .stream().parallel() //
                .forEach( //
                        ratingEngineId -> //
                        processSingleRatingId(tenent, ratingEngineIdCoverageMap, ratingEngineId));

        return result;
    }

    private RatingsCountResponse processSegmentIds(RatingsCountRequest request) {
        Tenant tenent = MultiTenantContext.getTenant();

        RatingsCountResponse result = new RatingsCountResponse();
        Map<String, CoverageInfo> segmentIdCoverageMap = new ConcurrentHashMap<>();
        result.setRatingEngineIdCoverageMap(segmentIdCoverageMap);

        request.getSegmentIds() //
                .stream().parallel() //
                .forEach( //
                        ratingEngineId -> //
                        processSingleSegmentId(tenent, segmentIdCoverageMap, ratingEngineId));

        result.setSegmentIdCoverageMap(segmentIdCoverageMap);

        return result;
    }

    private void processSingleSegmentId(Tenant tenent, Map<String, CoverageInfo> segmentIdCoverageMap,
            String segmentId) {
        MetadataSegment segment = metadataSegmentService.getSegmentByName(segmentId);
        FrontEndQuery accountFrontEndQuery = new FrontEndQuery();
        accountFrontEndQuery.setMainEntity(BusinessEntity.Account);

        FrontEndRestriction accountRestriction = new FrontEndRestriction(segment.getAccountRestriction());
        FrontEndRestriction contactRestriction = new FrontEndRestriction(segment.getContactRestriction());

        accountFrontEndQuery.setAccountRestriction(accountRestriction);
        accountFrontEndQuery.setContactRestriction(contactRestriction);

        Long accountCount = entityProxy.getCount( //
                tenent.getId(), //
                accountFrontEndQuery);

        CoverageInfo coverageInfo = new CoverageInfo();
        // TODO - fix it to read contact count from redshift
        Long contactCount = 7000L;
        contactCount += rand.nextInt(500);

        coverageInfo.setAccountCount(accountCount);
        coverageInfo.setContactCount(contactCount);

        segmentIdCoverageMap.put(segmentId, coverageInfo);
    }

    private void processSingleRatingId(Tenant tenent, Map<String, CoverageInfo> ratingEngineIdCoverageMap,
            String ratingEngineId) {
        RatingEngine ratingEngine = ratingEngineService.getRatingEngineById(ratingEngineId);
        FrontEndQuery accountFrontEndQuery = new FrontEndQuery();
        accountFrontEndQuery.setMainEntity(BusinessEntity.Account);

        FrontEndRestriction accountRestriction = new FrontEndRestriction(
                ratingEngine.getSegment().getAccountRestriction());
        FrontEndRestriction contactRestriction = new FrontEndRestriction(
                ratingEngine.getSegment().getContactRestriction());

        accountFrontEndQuery.setAccountRestriction(accountRestriction);
        accountFrontEndQuery.setContactRestriction(contactRestriction);

        List<RatingModel> ratingModels = new ArrayList<>();
        for (RatingModel model : ratingEngine.getRatingModels()) {
            ratingModels.add(model);
            break;
        }
        accountFrontEndQuery.setRatingModels(ratingModels);

        Map<String, Long> countInfo = entityProxy.getRatingCount( //
                tenent.getId(), //
                accountFrontEndQuery);
        Optional<Long> accountCountOption = countInfo.entrySet().stream().map(e -> e.getValue())
                .reduce((x, y) -> x + y);
        Long accountCount = accountCountOption.get();

        CoverageInfo coverageInfo = new CoverageInfo();
        // TODO - fix it to read contact count from redshift
        Long contactCount = 7000L;
        contactCount += rand.nextInt(500);

        coverageInfo.setAccountCount(accountCount);
        coverageInfo.setContactCount(contactCount);

        List<RatingBucketCoverage> bucketCoverageCounts = new ArrayList<>();
        for (RuleBucketName bucket : RuleBucketName.values()) {
            Long countInBucket = 0L;

            if (countInfo.containsKey(bucket.getName())) {
                countInBucket = countInfo.get(bucket.getName());
            } else if (countInfo.containsKey(bucket.name())) {
                countInBucket = countInfo.get(bucket.name());
            }

            RatingBucketCoverage coveragePair = new RatingBucketCoverage();
            coveragePair.setBucket(bucket.getName());
            coveragePair.setCount(countInBucket);
            bucketCoverageCounts.add(coveragePair);
        }
        coverageInfo.setBucketCoverageCounts(bucketCoverageCounts);
        ratingEngineIdCoverageMap.put(ratingEngineId, coverageInfo);
    }

    private RatingsCountResponse processRatingIdsDummy(RatingsCountRequest request) {
        RatingsCountResponse result = new RatingsCountResponse();
        HashMap<String, CoverageInfo> ratingEngineIdCoverageMap = new HashMap<>();
        result.setRatingEngineIdCoverageMap(ratingEngineIdCoverageMap);

        Random rand = new Random(System.currentTimeMillis());

        for (String ratingEngineId : request.getRatingEngineIds()) {
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
            ratingEngineIdCoverageMap.put(ratingEngineId, coverageInfo);
        }
        return result;
    }

    private RatingsCountResponse processSegmentIdsDummy(RatingsCountRequest request) {
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
