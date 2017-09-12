package com.latticeengines.pls.service.impl;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.google.common.annotations.VisibleForTesting;
import com.latticeengines.domain.exposed.metadata.MetadataSegment;
import com.latticeengines.domain.exposed.pls.CoverageInfo;
import com.latticeengines.domain.exposed.pls.RatingBucketCoverage;
import com.latticeengines.domain.exposed.pls.RatingEngine;
import com.latticeengines.domain.exposed.pls.RatingModel;
import com.latticeengines.domain.exposed.pls.RatingModelIdPair;
import com.latticeengines.domain.exposed.pls.RatingsCountRequest;
import com.latticeengines.domain.exposed.pls.RatingsCountResponse;
import com.latticeengines.domain.exposed.pls.RuleBasedModel;
import com.latticeengines.domain.exposed.pls.RuleBucketName;
import com.latticeengines.domain.exposed.pls.SegmentIdAndModelRulesPair;
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

    private static Logger log = LoggerFactory.getLogger(RatingCoverageServiceImpl.class);

    @Autowired
    private RatingEngineService ratingEngineService;

    @Autowired
    private MetadataSegmentService metadataSegmentService;

    @Autowired
    private EntityProxy entityProxy;

    Random rand = new Random(System.currentTimeMillis());

    @Override
    public RatingsCountResponse getCoverageInfo(RatingsCountRequest request) {
        return getCoverageInfo(request, false);
    }

    @Override
    public RatingsCountResponse getCoverageInfo(RatingsCountRequest request, boolean getDummyCoverage) {
        if (request.getRatingEngineIds() != null) {
            return getDummyCoverage ? processRatingIdsDummy(request) : processRatingIds(request);
        } else if (request.getSegmentIds() != null) {
            return getDummyCoverage ? processSegmentIdsDummy(request) : processSegmentIds(request);
        } else if (request.getRatingEngineModelIds() != null) {
            return processRatingEngineModelIds(request);
        } else if (request.getSegmentIdModelRules() != null) {
            return getDummyCoverage ? processSegmentIdModelRulesDummy(request) : processSegmentIdModelRules(request);
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
                        processSingleRatingId(tenent, ratingEngineIdCoverageMap, //
                                ratingEngineId, request.isRestrictNotNullSalesforceId()));

        return result;
    }

    private RatingsCountResponse processSegmentIds(RatingsCountRequest request) {
        Tenant tenent = MultiTenantContext.getTenant();

        RatingsCountResponse result = new RatingsCountResponse();
        Map<String, CoverageInfo> segmentIdCoverageMap = new ConcurrentHashMap<>();
        result.setSegmentIdCoverageMap(segmentIdCoverageMap);

        request.getSegmentIds() //
                .stream().parallel() //
                .forEach( //
                        segmentId -> //
                        processSingleSegmentId(tenent, segmentIdCoverageMap, //
                                segmentId, request.isRestrictNotNullSalesforceId()));

        return result;
    }

    private RatingsCountResponse processSegmentIdModelRules(RatingsCountRequest request) {
        Tenant tenent = MultiTenantContext.getTenant();

        RatingsCountResponse result = new RatingsCountResponse();
        Map<String, CoverageInfo> segmentIdModelRulesCoverageMap = new ConcurrentHashMap<>();
        result.setSegmentIdModelRulesCoverageMap(segmentIdModelRulesCoverageMap);

        request.getSegmentIdModelRules() //
                .stream().parallel() //
                .forEach( //
                        segmentIdModelRulesPair -> //
                        processSingleSegmentIdModelRulesPair(tenent, segmentIdModelRulesCoverageMap, //
                                segmentIdModelRulesPair, request.isRestrictNotNullSalesforceId()));

        return result;
    }

    private void processSingleSegmentId(Tenant tenent, Map<String, CoverageInfo> segmentIdCoverageMap, String segmentId,
            boolean isRestrictNotNullSalesforceId) {
        try {
            MetadataSegment segment = metadataSegmentService.getSegmentByName(segmentId);

            FrontEndQuery accountFrontEndQuery = createEntityFronEndQuery(BusinessEntity.Account,
                    isRestrictNotNullSalesforceId, segment);
            FrontEndQuery contactFrontEndQuery = createEntityFronEndQuery(BusinessEntity.Contact,
                    isRestrictNotNullSalesforceId, segment);

            Long accountCount = entityProxy.getCount( //
                    tenent.getId(), //
                    accountFrontEndQuery);
            Long contactCount = getContactCount(tenent, contactFrontEndQuery);

            CoverageInfo coverageInfo = new CoverageInfo();

            coverageInfo.setAccountCount(accountCount);
            coverageInfo.setContactCount(contactCount);

            segmentIdCoverageMap.put(segmentId, coverageInfo);
        } catch (Exception ex) {
            log.info("Ignoring exception in getting coverage info for segment id: " + segmentId, ex);
        }
    }

    Long getContactCount(Tenant tenent, FrontEndQuery contactFrontEndQuery) {
        Long contactCount = 0L;
        try {
            contactCount = entityProxy.getCount( //
                    tenent.getId(), //
                    contactFrontEndQuery);
        } catch (Exception ex) {
            log.info("Ignoring exception in getting contact count" + ex);
        }
        return contactCount;
    }

    FrontEndQuery createEntityFronEndQuery(BusinessEntity entityType, boolean isRestrictNotNullSalesforceId,
            MetadataSegment segment) {
        FrontEndQuery entityFrontEndQuery = new FrontEndQuery();

        entityFrontEndQuery.setMainEntity(entityType);

        FrontEndRestriction accountRestriction = new FrontEndRestriction(segment.getAccountRestriction());
        FrontEndRestriction contactRestriction = new FrontEndRestriction(segment.getContactRestriction());

        entityFrontEndQuery.setAccountRestriction(accountRestriction);
        entityFrontEndQuery.setContactRestriction(contactRestriction);
        entityFrontEndQuery.setRestrictNotNullSalesforceId(isRestrictNotNullSalesforceId);
        return entityFrontEndQuery;
    }

    private void processSingleRatingId(Tenant tenent, Map<String, CoverageInfo> ratingEngineIdCoverageMap,
            String ratingEngineId, boolean isRestrictNotNullSalesforceId) {
        try {
            RatingEngine ratingEngine = ratingEngineService.getRatingEngineById(ratingEngineId);
            MetadataSegment segment = ratingEngine.getSegment();

            FrontEndQuery accountFrontEndQuery = createEntityFronEndQuery(BusinessEntity.Account,
                    isRestrictNotNullSalesforceId, segment);
            FrontEndQuery contactFrontEndQuery = createEntityFronEndQuery(BusinessEntity.Contact,
                    isRestrictNotNullSalesforceId, segment);

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
            Long accountCount = accountCountOption.orElse(0L);
            Long contactCount = getContactCount(tenent, contactFrontEndQuery);

            CoverageInfo coverageInfo = new CoverageInfo();

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
        } catch (Exception ex) {
            log.info("Ignoring exception in getting coverage info for rating id: " + ratingEngineId, ex);
        }

    }

    private void processSingleSegmentIdModelRulesPair(Tenant tenent,
            Map<String, CoverageInfo> segmentIdModelRulesCoverageMap,
            SegmentIdAndModelRulesPair segmentIdModelRulesPair, boolean isRestrictNotNullSalesforceId) {
        try {
            MetadataSegment segment = metadataSegmentService.getSegmentByName(segmentIdModelRulesPair.getSegmentId());
            FrontEndQuery accountFrontEndQuery = createEntityFronEndQuery(BusinessEntity.Account,
                    isRestrictNotNullSalesforceId, segment);
            FrontEndQuery contactFrontEndQuery = createEntityFronEndQuery(BusinessEntity.Contact,
                    isRestrictNotNullSalesforceId, segment);

            List<RatingModel> ratingModels = new ArrayList<>();
            RuleBasedModel ratingModelWrapper = new RuleBasedModel();
            ratingModelWrapper.setRatingRule(segmentIdModelRulesPair.getRatingRule());
            ratingModels.add(ratingModelWrapper);
            accountFrontEndQuery.setRatingModels(ratingModels);

            Map<String, Long> countInfo = entityProxy.getRatingCount( //
                    tenent.getId(), //
                    accountFrontEndQuery);
            Optional<Long> accountCountOption = countInfo.entrySet().stream().map(e -> e.getValue())
                    .reduce((x, y) -> x + y);
            Long accountCount = accountCountOption.orElse(0L);
            Long contactCount = getContactCount(tenent, contactFrontEndQuery);

            CoverageInfo coverageInfo = new CoverageInfo();

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
            segmentIdModelRulesCoverageMap.put(segmentIdModelRulesPair.getSegmentId(), coverageInfo);
        } catch (Exception ex) {
            log.info("Ignoring exception in getting coverage info for segmentIdModelRulesPair: "
                    + segmentIdModelRulesPair, ex);
        }
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
        HashMap<String, CoverageInfo> ratingEngineModelIdCoverageMap = new HashMap<>();
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
            ratingEngineModelIdCoverageMap.put(ratingModelSegmentId.getRatingModelId(), coverageInfo);
        }
        return result;
    }

    private RatingsCountResponse processSegmentIdModelRulesDummy(RatingsCountRequest request) {
        RatingsCountResponse result = new RatingsCountResponse();
        HashMap<String, CoverageInfo> segmentIdAndModelRulesPairCoverageMap = new HashMap<>();
        result.setSegmentIdModelRulesCoverageMap(segmentIdAndModelRulesPairCoverageMap);

        Random rand = new Random(System.currentTimeMillis());

        for (SegmentIdAndModelRulesPair segmentIdAndModelRulesPair : request.getSegmentIdModelRules()) {
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
            segmentIdAndModelRulesPairCoverageMap.put(segmentIdAndModelRulesPair.getSegmentId(), coverageInfo);
        }
        return result;
    }

    @VisibleForTesting
    void setRatingEngineService(RatingEngineService ratingEngineService) {
        this.ratingEngineService = ratingEngineService;
    }

    @VisibleForTesting
    void setMetadataSegmentService(MetadataSegmentService metadataSegmentService) {
        this.metadataSegmentService = metadataSegmentService;
    }

    @VisibleForTesting
    void setEntityProxy(EntityProxy entityProxy) {
        this.entityProxy = entityProxy;
    }
}
