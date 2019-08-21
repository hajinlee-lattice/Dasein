package com.latticeengines.apps.cdl.service.impl;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ForkJoinPool;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import javax.annotation.PostConstruct;
import javax.inject.Inject;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.collections4.MapUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import com.google.common.annotations.VisibleForTesting;
import com.latticeengines.apps.cdl.service.RatingCoverageService;
import com.latticeengines.apps.cdl.service.RatingEngineService;
import com.latticeengines.apps.cdl.service.SegmentService;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.common.exposed.util.ThreadPoolUtils;
import com.latticeengines.db.exposed.util.MultiTenantContext;
import com.latticeengines.domain.exposed.cdl.ModelingQueryType;
import com.latticeengines.domain.exposed.cdl.ModelingStrategy;
import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.domain.exposed.metadata.InterfaceName;
import com.latticeengines.domain.exposed.metadata.MetadataSegment;
import com.latticeengines.domain.exposed.pls.AIModel;
import com.latticeengines.domain.exposed.pls.CrossSellModelingConfigKeys;
import com.latticeengines.domain.exposed.pls.ModelingConfigFilter;
import com.latticeengines.domain.exposed.pls.RatingBucketName;
import com.latticeengines.domain.exposed.pls.RatingEngine;
import com.latticeengines.domain.exposed.pls.RatingModel;
import com.latticeengines.domain.exposed.pls.RuleBasedModel;
import com.latticeengines.domain.exposed.pls.cdl.rating.CrossSellRatingConfig;
import com.latticeengines.domain.exposed.pls.cdl.rating.model.CrossSellModelingConfig;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.domain.exposed.query.ComparisonType;
import com.latticeengines.domain.exposed.query.Restriction;
import com.latticeengines.domain.exposed.query.RestrictionBuilder;
import com.latticeengines.domain.exposed.query.frontend.EventFrontEndQuery;
import com.latticeengines.domain.exposed.query.frontend.FrontEndQuery;
import com.latticeengines.domain.exposed.query.frontend.FrontEndRestriction;
import com.latticeengines.domain.exposed.query.frontend.RatingEngineFrontEndQuery;
import com.latticeengines.domain.exposed.ratings.coverage.CoverageInfo;
import com.latticeengines.domain.exposed.ratings.coverage.ProductAndEventQueryPair;
import com.latticeengines.domain.exposed.ratings.coverage.ProductsCoverageRequest;
import com.latticeengines.domain.exposed.ratings.coverage.RatingBucketCoverage;
import com.latticeengines.domain.exposed.ratings.coverage.RatingEnginesCoverageRequest;
import com.latticeengines.domain.exposed.ratings.coverage.RatingEnginesCoverageResponse;
import com.latticeengines.domain.exposed.ratings.coverage.RatingIdLookupColumnPair;
import com.latticeengines.domain.exposed.ratings.coverage.RatingModelIdPair;
import com.latticeengines.domain.exposed.ratings.coverage.RatingsCountRequest;
import com.latticeengines.domain.exposed.ratings.coverage.RatingsCountResponse;
import com.latticeengines.domain.exposed.ratings.coverage.SegmentIdAndModelRulesPair;
import com.latticeengines.domain.exposed.ratings.coverage.SegmentIdAndSingleRulePair;
import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.proxy.exposed.cdl.DataCollectionProxy;
import com.latticeengines.proxy.exposed.objectapi.EntityProxy;
import com.latticeengines.proxy.exposed.objectapi.EventProxy;
import com.latticeengines.proxy.exposed.objectapi.RatingProxy;

@Component("ratingCoverageService")
public class RatingCoverageServiceImpl implements RatingCoverageService {

    private static Logger log = LoggerFactory.getLogger(RatingCoverageServiceImpl.class);

    private static final String DEFAULT_ID_FOR_MODEL_RULE = "DEFAULT_ID_FOR_MODEL_RULE";

    @Value("${pls.rating.coverageservice.threadpool.size:10}")
    private Integer fetcherNum;

    @Value("${pls.rating.coverageservice.threshold.parallel:3}")
    private Integer thresholdForParallelProcessing;

    @Inject
    private RatingEngineService ratingEngineService;

    @Inject
    private SegmentService segmentService;

    @Inject
    private EntityProxy entityProxy;

    @Inject
    private EventProxy eventProxy;

    @Inject
    private DataCollectionProxy dataCollectionProxy;

    @Inject
    private RatingProxy ratingProxy;

    private ForkJoinPool tpForParallelStream;

    @PostConstruct
    public void init() {
        tpForParallelStream = ThreadPoolUtils.getForkJoinThreadPool("rating-coverage", fetcherNum);
    }

    @Override
    public RatingsCountResponse getCoverageInfo(String customerSpace, RatingsCountRequest request) {
        RatingsCountResponse result = new RatingsCountResponse();
        Map<String, Map<String, String>> uberErrorMap = new ConcurrentHashMap<>();
        result.setErrorMap(uberErrorMap);

        if (CollectionUtils.isNotEmpty(request.getRatingEngineIds())) {
            processRatingIds(request, result);
        }

        if (CollectionUtils.isNotEmpty(request.getSegmentIds())) {
            processSegmentIds(request, result);
        }

        if (CollectionUtils.isNotEmpty(request.getRatingEngineModelIds())) {
            processRatingEngineModelIds(request, result);
        }

        if (CollectionUtils.isNotEmpty(request.getSegmentIdModelRules())) {
            processSegmentIdModelRules(request, result);
        }

        if (CollectionUtils.isNotEmpty(request.getSegmentIdAndSingleRules())) {
            processSegmentIdSingleRules(request, result);
        }

        if (CollectionUtils.isNotEmpty(request.getRatingIdLookupColumnPairs())) {
            processRatingIdLookupColumnPairs(request, result);
        }

        errorMapCleanup(result, uberErrorMap);

        return result;
    }

    private void processRatingIdLookupColumnPairs(RatingsCountRequest request, RatingsCountResponse result) {
        Tenant tenant = MultiTenantContext.getTenant();

        Map<String, CoverageInfo> ratingIdLookupColumnPairsCoverageMap = new ConcurrentHashMap<>();
        result.setRatingIdLookupColumnPairsCoverageMap(ratingIdLookupColumnPairsCoverageMap);
        Map<String, String> errorMap = new ConcurrentHashMap<>();
        result.getErrorMap().put(RATING_ID_LOOKUP_COL_PAIR_ERROR_MAP_KEY, errorMap);

        if (request.getRatingIdLookupColumnPairs().size() < thresholdForParallelProcessing) {
            // it is more efficient to use sequential stream (will use current
            // thread) if collection size is small. It also ensures that small
            // requests are not blocked if threadpool is used by bigger requests
            Stream<RatingIdLookupColumnPair> stream = //
                    request.getRatingIdLookupColumnPairs().stream();
            ratingIdLookupColumnStreamProcessing(request, tenant, ratingIdLookupColumnPairsCoverageMap, errorMap,
                    stream);
        } else {
            tpForParallelStream.submit(//
                    () -> //
                    {
                        Stream<RatingIdLookupColumnPair> parallelStream = //
                                request.getRatingIdLookupColumnPairs().stream() //
                                        .parallel();

                        ratingIdLookupColumnStreamProcessing(request, tenant, ratingIdLookupColumnPairsCoverageMap,
                                errorMap, parallelStream);
                    }) //
                    .join();
        }
    }

    private void processRatingIds(RatingsCountRequest request, RatingsCountResponse result) {
        Tenant tenant = MultiTenantContext.getTenant();
        result.setRatingEngineIdCoverageMap(new ConcurrentHashMap<>());
        Map<String, String> errorMap = new ConcurrentHashMap<>();
        result.getErrorMap().put(RATING_IDS_ERROR_MAP_KEY, errorMap);

        if (request.getRatingEngineIds().size() < thresholdForParallelProcessing) {
            // it is more efficient to use sequential stream (will use current
            // thread) if collection size is small. It also ensures that small
            // requests are not blocked if threadpool is used by bigger requests
            Stream<String> stream = //
                    request.getRatingEngineIds().stream();
            ratingEngineStreamProcessing(request, tenant, result, stream);
        } else {
            tpForParallelStream.submit(//
                    () -> //
                    {
                        Stream<String> parallelStream = //
                                request.getRatingEngineIds().stream() //
                                        .parallel();

                        ratingEngineStreamProcessing(request, tenant, result, parallelStream);
                    }) //
                    .join();
        }
    }

    private void processSegmentIds(RatingsCountRequest request, RatingsCountResponse result) {
        Tenant tenant = MultiTenantContext.getTenant();

        Map<String, CoverageInfo> segmentIdCoverageMap = new ConcurrentHashMap<>();
        result.setSegmentIdCoverageMap(segmentIdCoverageMap);
        Map<String, String> errorMap = new ConcurrentHashMap<>();
        result.getErrorMap().put(SEGMENT_IDS_ERROR_MAP_KEY, errorMap);

        if (request.getSegmentIds().size() < thresholdForParallelProcessing) {
            // it is more efficient to use sequential stream (will use current
            // thread) if collection size is small. It also ensures that small
            // requests are not blocked if threadpool is used by bigger requests
            Stream<String> stream = //
                    request.getSegmentIds().stream();
            segmentStreamProcessing(request, tenant, segmentIdCoverageMap, errorMap, stream);
        } else {
            tpForParallelStream.submit(//
                    () -> //
                    {
                        Stream<String> parallelStream = //
                                request.getSegmentIds().stream() //
                                        .parallel();

                        segmentStreamProcessing(request, tenant, segmentIdCoverageMap, errorMap, parallelStream);
                    }) //
                    .join();
        }
    }

    private void processSegmentIdModelRules(RatingsCountRequest request, RatingsCountResponse result) {
        Tenant tenant = MultiTenantContext.getTenant();

        Map<String, CoverageInfo> segmentIdModelRulesCoverageMap = new ConcurrentHashMap<>();
        result.setSegmentIdModelRulesCoverageMap(segmentIdModelRulesCoverageMap);
        Map<String, String> errorMap = new ConcurrentHashMap<>();
        result.getErrorMap().put(SEGMENT_ID_MODEL_RULES_ERROR_MAP_KEY, errorMap);

        if (request.getSegmentIdModelRules().size() < thresholdForParallelProcessing) {
            // it is more efficient to use sequential stream (will use current
            // thread) if collection size is small. It also ensures that small
            // requests are not blocked if threadpool is used by bigger requests
            Stream<SegmentIdAndModelRulesPair> stream = //
                    request.getSegmentIdModelRules().stream();
            segmentIdModelRulesStreamProcessing(request, tenant, segmentIdModelRulesCoverageMap, errorMap, stream);
        } else {
            tpForParallelStream.submit(//
                    () -> //
                    {
                        Stream<SegmentIdAndModelRulesPair> parallelStream = //
                                request.getSegmentIdModelRules().stream() //
                                        .parallel();

                        segmentIdModelRulesStreamProcessing(request, tenant, segmentIdModelRulesCoverageMap, errorMap,
                                parallelStream);
                    }) //
                    .join();
        }
    }

    private void processSegmentIdSingleRules(RatingsCountRequest request, RatingsCountResponse result) {
        Tenant tenant = MultiTenantContext.getTenant();

        Map<String, CoverageInfo> segmentIdAndSingleRulesCoverageMap = new ConcurrentHashMap<>();
        result.setSegmentIdAndSingleRulesCoverageMap(segmentIdAndSingleRulesCoverageMap);
        Map<String, String> errorMap = new ConcurrentHashMap<>();
        result.getErrorMap().put(SEGMENT_ID_SINGLE_RULES_ERROR_MAP_KEY, errorMap);

        Map<String, MetadataSegment> segmentMap = loadSegments(request.getSegmentIdAndSingleRules(), errorMap);

        if (request.getSegmentIdAndSingleRules().size() < thresholdForParallelProcessing) {
            // it is more efficient to use sequential stream (will use current
            // thread) if collection size is small. It also ensures that small
            // requests are not blocked if threadpool is used by bigger requests
            Stream<SegmentIdAndSingleRulePair> stream = //
                    request.getSegmentIdAndSingleRules().stream();

            segmentIdAndSingleRulesStreamProcessing( //
                    request, tenant, segmentMap, //
                    segmentIdAndSingleRulesCoverageMap, //
                    errorMap, stream);
        } else {
            tpForParallelStream.submit(//
                    () -> //
                    {
                        Stream<SegmentIdAndSingleRulePair> parallelStream = //
                                request.getSegmentIdAndSingleRules().stream() //
                                        .parallel();

                        segmentIdAndSingleRulesStreamProcessing( //
                                request, tenant, segmentMap, //
                                segmentIdAndSingleRulesCoverageMap, //
                                errorMap, parallelStream);
                    }) //
                    .join();
        }
    }

    private Map<String, MetadataSegment> loadSegments(List<SegmentIdAndSingleRulePair> segmentIdAndSingleRules,
            Map<String, String> errorMap) {

        Map<String, MetadataSegment> segmentMap = new ConcurrentHashMap<>();

        if (CollectionUtils.isNotEmpty(segmentIdAndSingleRules)) {
            Set<String> uniqueSegmentIds = //
                    segmentIdAndSingleRules.stream() //
                            .map(SegmentIdAndSingleRulePair::getSegmentId) //
                            .collect(Collectors.toSet());

            uniqueSegmentIds.stream() //
                    .forEach(segmentId -> {
                        try {
                            MetadataSegment segment = segmentService.findByName(segmentId);
                            segmentMap.put(segmentId, segment);
                        } catch (Exception ex) {
                            logInErrorMap(errorMap, segmentId, "Could not load segment");
                        }
                    });

        }
        return segmentMap;
    }

    // this method can accept parallel or sequential stream
    private void ratingIdLookupColumnStreamProcessing(RatingsCountRequest request, Tenant tenant,
            Map<String, CoverageInfo> ratingEngineIdCoverageMap, Map<String, String> errorMap,
            Stream<RatingIdLookupColumnPair> stream) {
        stream //
                .forEach( //
                        ratingIdLookupColumnPair -> //
                        processSingleRatingIdLookupColumnPair(tenant, ratingEngineIdCoverageMap, errorMap, //
                                ratingIdLookupColumnPair, request.isRestrictNotNullSalesforceId()));
    }

    // this method can accept parallel or sequential stream
    private void ratingEngineStreamProcessing(RatingsCountRequest request, Tenant tenant, RatingsCountResponse result,
            Stream<String> stream) {
        stream.forEach(ratingEngineId -> {
            try {
                CoverageInfo coverageInfo = processSingleRatingId(tenant, null, ratingEngineId,
                        request.isRestrictNotNullSalesforceId(), null, true, false, false);
                result.getRatingEngineIdCoverageMap().put(ratingEngineId, coverageInfo);
            } catch (Exception ex) {
                log.info("Ignoring exception in getting coverage info for rating id: " + ratingEngineId, ex);
                result.getErrorMap().get(RATING_IDS_ERROR_MAP_KEY).put(ratingEngineId, ex.getMessage());
            }
        });
    }

    // this method can accept parallel or sequential stream
    private void segmentStreamProcessing(RatingsCountRequest request, Tenant tenant,
            Map<String, CoverageInfo> segmentIdCoverageMap, Map<String, String> errorMap, Stream<String> stream) {
        stream //
                .forEach( //
                        segmentId -> //
                        processSingleSegmentId(tenant, segmentIdCoverageMap, errorMap, //
                                segmentId, request.isRestrictNotNullSalesforceId()));
    }

    // this method can accept parallel or sequential stream
    private void segmentIdModelRulesStreamProcessing(RatingsCountRequest request, Tenant tenant,
            Map<String, CoverageInfo> segmentIdModelRulesCoverageMap, Map<String, String> errorMap,
            Stream<SegmentIdAndModelRulesPair> stream) {
        stream //
                .forEach( //
                        segmentIdModelRulesPair -> //
                        processSingleSegmentIdModelRulesPair(tenant, segmentIdModelRulesCoverageMap, errorMap, //
                                segmentIdModelRulesPair, request.isRestrictNotNullSalesforceId()));
    }

    // this method can accept parallel or sequential stream
    private void segmentIdAndSingleRulesStreamProcessing(RatingsCountRequest request, Tenant tenant,
            Map<String, MetadataSegment> segmentMap, Map<String, CoverageInfo> segmentIdAndSingleRulesCoverageMap,
            Map<String, String> errorMap, Stream<SegmentIdAndSingleRulePair> stream) {

        stream //
                .forEach( //
                        segmentIdSingleRulesPair -> //
                        processSingleSegmentIdSingleRulesPair(tenant, segmentMap, segmentIdAndSingleRulesCoverageMap, errorMap, //
                                segmentIdSingleRulesPair, request.isRestrictNotNullSalesforceId()));
    }

    private void processSingleSegmentId(Tenant tenant, Map<String, CoverageInfo> segmentIdCoverageMap,
            Map<String, String> errorMap, String segmentId, boolean isRestrictNotNullSalesforceId) {
        try {
            MultiTenantContext.setTenant(tenant);

            MetadataSegment segment = segmentService.findByName(segmentId);

            if (segment == null) {
                logInErrorMap(errorMap, segmentId, "Invalid segment");
                return;
            }

            FrontEndQuery accountFrontEndQuery = //
                    createEntityFrontEndQuery(BusinessEntity.Account, //
                            isRestrictNotNullSalesforceId, segment);
            FrontEndQuery contactFrontEndQuery = //
                    createEntityFrontEndQuery(BusinessEntity.Contact, //
                            isRestrictNotNullSalesforceId, segment);

            loadBasicCoverage(tenant, segmentIdCoverageMap, segmentId, accountFrontEndQuery, contactFrontEndQuery);
        } catch (Exception ex) {
            log.info("Ignoring exception in getting coverage info for segment id: " + segmentId, ex);
            logInErrorMap(errorMap, segmentId, ex.getMessage());
        }
    }

    private void processSingleSegmentIdSingleRulesPair(Tenant tenant, Map<String, MetadataSegment> segmentMap,
            Map<String, CoverageInfo> segmentIdAndSingleRulesCoverageMap, Map<String, String> errorMap,
            SegmentIdAndSingleRulePair segmentIdSingleRulePair, boolean isRestrictNotNullSalesforceId) {
        try {
            MultiTenantContext.setTenant(tenant);

            MetadataSegment segment = //
                    segmentMap.get(segmentIdSingleRulePair.getSegmentId());

            if (segment == null) {
                // we have already logged in message for segment being null
                return;
            }

            FrontEndQuery accountFrontEndQuery = //
                    createEntityFronEndQueryForSegmentAndRule(BusinessEntity.Account, //
                            isRestrictNotNullSalesforceId, segment, segmentIdSingleRulePair);
            FrontEndQuery contactFrontEndQuery = //
                    createEntityFronEndQueryForSegmentAndRule(BusinessEntity.Contact, //
                            isRestrictNotNullSalesforceId, segment, segmentIdSingleRulePair);

            loadBasicCoverage(tenant, segmentIdAndSingleRulesCoverageMap, segmentIdSingleRulePair.getResponseKeyId(),
                    accountFrontEndQuery, contactFrontEndQuery);
        } catch (Exception ex) {
            log.info("Ignoring exception in getting coverage info for segmentIdSingleRulePair: "
                    + segmentIdSingleRulePair, ex);
            logInErrorMap(errorMap, segmentIdSingleRulePair.getResponseKeyId(), ex.getMessage());
        }
    }

    private void processSingleRatingIdLookupColumnPair(Tenant tenant,
            Map<String, CoverageInfo> ratingEngineIdCoverageMap, Map<String, String> errorMap,
            RatingIdLookupColumnPair ratingIdLookupColumnPair, boolean isRestrictNotNullSalesforceId) {
        try {
            MultiTenantContext.setTenant(tenant);

            if (StringUtils.isBlank(ratingIdLookupColumnPair.getResponseKeyId())
                    || StringUtils.isBlank(ratingIdLookupColumnPair.getRatingEngineId())
                    || StringUtils.isBlank(ratingIdLookupColumnPair.getLookupColumn())) {
                String key = ratingIdLookupColumnPair.getResponseKeyId();
                if (StringUtils.isBlank(key)) {
                    key = "NULL_KEY";
                }
                logInErrorMap(errorMap, key,
                        String.format(
                                "Make sure to pass non-empty values for "
                                        + "responseKeyId = %s, ratingEngineId = %s, lookupColumn = %s",
                                ratingIdLookupColumnPair.getResponseKeyId(), //
                                ratingIdLookupColumnPair.getRatingEngineId(), //
                                ratingIdLookupColumnPair.getLookupColumn()));
                return;
            }

            RatingEngine ratingEngine = ratingEngineService
                    .getRatingEngineById(ratingIdLookupColumnPair.getRatingEngineId(), true, true);

            if (ratingEngine == null || ratingEngine.getSegment() == null) {
                logInErrorMap(errorMap, ratingIdLookupColumnPair.getResponseKeyId(), "Invalid rating engine");
                return;
            }

            MetadataSegment segment = ratingEngine.getSegment();

            FrontEndQuery accountFrontEndQuery0 = //
                    createEntityFrontEndQuery(BusinessEntity.Account, //
                            isRestrictNotNullSalesforceId, segment, ratingIdLookupColumnPair.getLookupColumn());
            FrontEndQuery contactFrontEndQuery = //
                    createEntityFrontEndQuery(BusinessEntity.Contact, //
                            isRestrictNotNullSalesforceId, segment, ratingIdLookupColumnPair.getLookupColumn());

            RatingEngineFrontEndQuery accountFrontEndQuery = RatingEngineFrontEndQuery
                    .fromFrontEndQuery(accountFrontEndQuery0);
            accountFrontEndQuery.setRatingEngineId(ratingIdLookupColumnPair.getRatingEngineId());

            log.info("Front end query for Account: " + JsonUtils.serialize(accountFrontEndQuery));
            Map<String, Long> countInfo = entityProxy.getRatingCount( //
                    tenant.getId(), //
                    accountFrontEndQuery);

            Optional<Long> accountCountOption = countInfo.entrySet().stream().map(Map.Entry::getValue)
                    .reduce((x, y) -> x + y);

            CoverageInfo coverageInfo = new CoverageInfo();

            Long accountCount = accountCountOption.orElse(0L);
            coverageInfo.setAccountCount(accountCount);

            try {
                Long contactCount = getContactCount(tenant, contactFrontEndQuery);
                coverageInfo.setContactCount(contactCount);
            } catch (Exception ex) {
                log.info("Got error in fetching contact count", ex);
            }

            List<RatingBucketCoverage> bucketCoverageCounts = new ArrayList<>();
            for (RatingBucketName bucket : RatingBucketName.values()) {
                Long countInBucket = 0L;

                if (countInfo.containsKey(bucket.getName())) {
                    countInBucket = countInfo.get(bucket.getName());
                } else if (countInfo.containsKey(bucket.name())) {
                    countInBucket = countInfo.get(bucket.name());
                } else {
                    // do not put bucket info for bucket which is not defined
                    continue;
                }

                RatingBucketCoverage coveragePair = new RatingBucketCoverage();
                coveragePair.setBucket(bucket.getName());
                coveragePair.setCount(countInBucket);
                bucketCoverageCounts.add(coveragePair);
            }
            coverageInfo.setBucketCoverageCounts(bucketCoverageCounts);
            ratingEngineIdCoverageMap.put(ratingIdLookupColumnPair.getResponseKeyId(), coverageInfo);
        } catch (Exception ex) {
            log.info("Ignoring exception in getting coverage info for rating id: " + ratingIdLookupColumnPair, ex);
            logInErrorMap(errorMap, ratingIdLookupColumnPair.getResponseKeyId(), ex.getMessage());
        }

    }

    private CoverageInfo processSingleRatingId(Tenant tenant, MetadataSegment targetSegment, String ratingEngineId,
            boolean isRestrictNullLookupId, String lookupId, boolean loadContactCount,
            boolean loadContactsCountByBucket, boolean applyEmailFilter) {
        try {
            MultiTenantContext.setTenant(tenant);

            RatingEngine ratingEngine = ratingEngineService.getRatingEngineById(ratingEngineId, true, true);

            if (ratingEngine == null || ratingEngine.getSegment() == null) {
                throw new RuntimeException("Invalid rating engine. Doesn't have segment association.");
            }

            MetadataSegment querySegment = targetSegment != null ? targetSegment : ratingEngine.getSegment();

            FrontEndQuery accountFrontEndQuery = //
                    createEntityFrontEndQuery(BusinessEntity.Account, //
                            isRestrictNullLookupId, querySegment, lookupId);

            if (applyEmailFilter) {
                Restriction newContactRestriction = applyEmailFilterToContactRestriction(
                        accountFrontEndQuery.getContactRestriction().getRestriction());
                accountFrontEndQuery.setContactRestriction(new FrontEndRestriction(newContactRestriction));
            }

            RatingEngineFrontEndQuery ratingEngineAccountFrontEndQuery = RatingEngineFrontEndQuery
                    .fromFrontEndQuery(accountFrontEndQuery);
            ratingEngineAccountFrontEndQuery.setRatingEngineId(ratingEngineId);

            log.info("Front end query for Account: " + JsonUtils.serialize(ratingEngineAccountFrontEndQuery));
            Map<String, Long> countInfo = entityProxy.getRatingCount( //
                    tenant.getId(), //
                    ratingEngineAccountFrontEndQuery);

            Optional<Long> accountCountOption = countInfo.entrySet().stream().map(Map.Entry::getValue)
                    .reduce((x, y) -> x + y);

            CoverageInfo coverageInfo = new CoverageInfo();

            Long accountCount = accountCountOption.orElse(0L);
            coverageInfo.setAccountCount(accountCount);

            List<RatingBucketCoverage> bucketCoverageCounts = new ArrayList<>();
            for (RatingBucketName bucket : RatingBucketName.values()) {
                Long countInBucket = 0L;

                if (countInfo.containsKey(bucket.getName())) {
                    countInBucket = countInfo.get(bucket.name());
                } else {
                    // do not put bucket info for bucket which is not defined
                    continue;
                }

                RatingBucketCoverage coveragePair = new RatingBucketCoverage();
                coveragePair.setBucket(bucket.getName());
                coveragePair.setCount(countInBucket);
                bucketCoverageCounts.add(coveragePair);
            }
            coverageInfo.setBucketCoverageCounts(bucketCoverageCounts);

            // Populate Unscored counts
            // unscored accounts
            String ratingField = RatingEngine.toRatingAttrName(ratingEngineId, RatingEngine.ScoreType.Rating);
            Restriction unscoredRestriction = Restriction.builder().let(BusinessEntity.Rating, ratingField).isNull()
                    .build();
            Restriction unscoredAccountsInSegmentRest = Restriction.builder()
                    .and(accountFrontEndQuery.getAccountRestriction().getRestriction(), unscoredRestriction).build();
            FrontEndQuery unscoredFrontEndQuery = new FrontEndQuery();
            unscoredFrontEndQuery.setMainEntity(BusinessEntity.Account);
            unscoredFrontEndQuery.setAccountRestriction(new FrontEndRestriction(unscoredAccountsInSegmentRest));
            unscoredFrontEndQuery.setContactRestriction(accountFrontEndQuery.getContactRestriction());
            coverageInfo.setUnscoredAccountCount(entityProxy.getCount(tenant.getId(), unscoredFrontEndQuery));

            boolean hasContactTable = dataCollectionProxy.hasContact(tenant.getId(), null);
            // TODO: this is not working as expected. Though contact table
            // exists, this api still returns false.
            if (!hasContactTable) {
                log.info("Contact Table is not available for Tenant: {}", tenant.getId());
            }
            if (hasContactTable && (loadContactCount || loadContactsCountByBucket)) {
                try {
                    // If user requests for ContactCount by bucket, we can
                    // compute the total count.
                    // Otherwise, total count should be fetched using single
                    // query instead of looping every bucket.
                    if (loadContactsCountByBucket) {
                        coverageInfo.setContactCount(0L);
                        coverageInfo.getBucketCoverageCounts().stream().forEach(bucketCoverage -> {
                            try {
                                FrontEndQuery contactFrontEndQuery = createEntityFrontEndQuery(BusinessEntity.Contact,
                                        isRestrictNullLookupId, querySegment, lookupId, ratingEngineId,
                                        bucketCoverage.getBucket());

                                if (applyEmailFilter) {
                                    Restriction newContactRestriction = applyEmailFilterToContactRestriction(
                                            contactFrontEndQuery.getContactRestriction().getRestriction());
                                    contactFrontEndQuery
                                            .setContactRestriction(new FrontEndRestriction(newContactRestriction));
                                }

                                Long bucketCount = getContactCount(tenant, contactFrontEndQuery);
                                bucketCoverage.setContactCount(bucketCount);
                                coverageInfo.setContactCount(bucketCount + coverageInfo.getContactCount());
                            } catch (Exception ex) {
                                // Ignore the exception
                                log.info("Error while fetching contact count for Rating Bucket {}",
                                        bucketCoverage.getBucket(), ex);
                            }
                        });

                    } else {
                        FrontEndQuery contactFrontEndQuery = createEntityFrontEndQuery(BusinessEntity.Contact,
                                isRestrictNullLookupId, querySegment, lookupId, ratingEngineId, null);

                        if (applyEmailFilter) {
                            Restriction newContactRestriction = applyEmailFilterToContactRestriction(
                                    contactFrontEndQuery.getContactRestriction().getRestriction());
                            contactFrontEndQuery.setContactRestriction(new FrontEndRestriction(newContactRestriction));
                        }

                        Long contactCount = getContactCount(tenant, contactFrontEndQuery);
                        coverageInfo.setContactCount(contactCount);
                    }

                    // unscored contacts
                    unscoredFrontEndQuery.setMainEntity(BusinessEntity.Contact);
                    Long unscoredContactCount = getContactCount(tenant, unscoredFrontEndQuery);
                    coverageInfo.setUnscoredContactCount(unscoredContactCount);

                } catch (Exception ex) {
                    log.info("Got error in fetching contact count", ex);
                }
            }
            return coverageInfo;
        } catch (Exception ex) {
            throw ex;
        }

    }

    private Restriction applyEmailFilterToContactRestriction(Restriction contactRestriction) {
        Restriction emailFilter = Restriction.builder().let(BusinessEntity.Contact, InterfaceName.Email.name())
                .isNotNull().build();
        Restriction newContactRestriction = Restriction.builder().and(contactRestriction, emailFilter).build();
        return newContactRestriction;
    }

    private void processSingleSegmentIdModelRulesPair(Tenant tenant,
            Map<String, CoverageInfo> segmentIdModelRulesCoverageMap, Map<String, String> errorMap,
            SegmentIdAndModelRulesPair segmentIdModelRulesPair, boolean isRestrictNotNullSalesforceId) {
        try {
            MultiTenantContext.setTenant(tenant);

            MetadataSegment segment = segmentService.findByName(segmentIdModelRulesPair.getSegmentId());
            FrontEndQuery accountFrontEndQuery = //
                    createEntityFrontEndQuery(BusinessEntity.Account, //
                            isRestrictNotNullSalesforceId, segment);
            FrontEndQuery contactFrontEndQuery = //
                    createEntityFrontEndQuery(BusinessEntity.Contact, //
                            isRestrictNotNullSalesforceId, segment);

            List<RatingModel> ratingModels = new ArrayList<>();
            RuleBasedModel ratingModelWrapper = new RuleBasedModel();
            ratingModelWrapper.setRatingRule(segmentIdModelRulesPair.getRatingRule());
            ratingModels.add(ratingModelWrapper);
            if (StringUtils.isEmpty(ratingModelWrapper.getId())) {
                ratingModelWrapper.setId(DEFAULT_ID_FOR_MODEL_RULE);
            }
            accountFrontEndQuery.setRatingModels(ratingModels);

            log.info("Front end query for Account: " + JsonUtils.serialize(accountFrontEndQuery));
            Map<String, Long> countInfo = ratingProxy.getCoverage( //
                    tenant.getId(), //
                    accountFrontEndQuery);

            Optional<Long> accountCountOption = countInfo.entrySet().stream().map(Map.Entry::getValue)
                    .reduce((x, y) -> x + y);

            CoverageInfo coverageInfo = new CoverageInfo();

            Long accountCount = accountCountOption.orElse(0L);
            coverageInfo.setAccountCount(accountCount);

            try {
                Long contactCount = getContactCount(tenant, contactFrontEndQuery);
                coverageInfo.setContactCount(contactCount);
            } catch (Exception ex) {
                log.info("Got error in fetching contact count", ex);
            }

            List<RatingBucketCoverage> bucketCoverageCounts = new ArrayList<>();
            for (RatingBucketName bucket : RatingBucketName.values()) {
                Long countInBucket = 0L;

                if (countInfo.containsKey(bucket.getName())) {
                    countInBucket = countInfo.get(bucket.getName());
                } else if (countInfo.containsKey(bucket.name())) {
                    countInBucket = countInfo.get(bucket.name());
                } else {
                    // do not put bucket info for bucket which is not defined
                    continue;
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
            logInErrorMap(errorMap, segmentIdModelRulesPair.getSegmentId(), ex.getMessage());
        }
    }

    void loadBasicCoverage(Tenant tenant, Map<String, CoverageInfo> segmentIdAndSingleRulesCoverageMap, String key,
            FrontEndQuery accountFrontEndQuery, FrontEndQuery contactFrontEndQuery) throws Exception {
        log.info("Front end query for Account: " + JsonUtils.serialize(accountFrontEndQuery));

        CoverageInfo coverageInfo = new CoverageInfo();
        Exception accountCountException = null;
        Exception contactCountException = null;

        try {
            Long accountCount = entityProxy.getCount( //
                    tenant.getId(), //
                    accountFrontEndQuery);
            coverageInfo.setAccountCount(accountCount);
        } catch (Exception ex) {
            accountCountException = ex;
            log.info("Got error in fetching account count", ex);
        }

        try {
            Long contactCount = getContactCount(tenant, contactFrontEndQuery);
            coverageInfo.setContactCount(contactCount);
        } catch (Exception ex) {
            contactCountException = ex;
            log.info("Got error in fetching contact count", ex);
        }

        // PLS-5940 - we want to do more fine grained exception handling so that
        // if contact count exception occurs it doesn't affect
        // successful account count
        // if account count is successful, put the coverage info in final
        // result
        if (accountCountException == null) {
            segmentIdAndSingleRulesCoverageMap.put(key, coverageInfo);
        }

        // after putting coverage info (if any count is successful) then throw
        // account exception exception for regular exception handling in caller
        // api
        if (accountCountException != null) {
            throw accountCountException;
        } else if (contactCountException != null) {
            throw contactCountException;
        }
    }

    private Long getContactCount(Tenant tenant, FrontEndQuery contactFrontEndQuery) {
        // TODO: uncomment it after api is fixed.
        // if (dataCollectionProxy.hasContact(tenant.getId(), null)) {
        // return 0L;
        // }
        if (log.isDebugEnabled()) {
            log.debug("Front end query for Contact: " + JsonUtils.serialize(contactFrontEndQuery));
        }
        return entityProxy.getCount(tenant.getId(), contactFrontEndQuery);
    }

    private FrontEndQuery createEntityFrontEndQuery(BusinessEntity entityType, boolean isRestrictNotNullLookupId,
            MetadataSegment segment) {
        return createEntityFrontEndQuery(entityType, isRestrictNotNullLookupId, segment, null);
    }

    private FrontEndQuery createEntityFrontEndQuery(BusinessEntity entityType, boolean isRestrictNotNullLookupId,
            MetadataSegment segment, String lookupColumn) {
        return createEntityFrontEndQuery(entityType, isRestrictNotNullLookupId, segment, lookupColumn, null, null);
    }

    private FrontEndQuery createEntityFrontEndQuery(BusinessEntity entityType, boolean isRestrictNullLookupId,
            MetadataSegment segment, String lookupColumn, String ratingEngineId, String ratingBucket) {
        FrontEndQuery entityFrontEndQuery = new FrontEndQuery();

        entityFrontEndQuery.setMainEntity(entityType);

        List<Restriction> frontEndRestrictions = new ArrayList<>();
        frontEndRestrictions.add(segment.getAccountRestriction());

        if (isRestrictNullLookupId && StringUtils.isNotBlank(lookupColumn)) {
            frontEndRestrictions.add(lookupColumnRestriction(lookupColumn));
        }

        if (StringUtils.isNotBlank(ratingEngineId)) {
            frontEndRestrictions.add(createRatingEngineRestriction(ratingEngineId, ratingBucket));
        }

        FrontEndRestriction accountRestriction = prepareFrontEndRestriction(frontEndRestrictions);
        FrontEndRestriction contactRestriction = new FrontEndRestriction(segment.getContactRestriction());

        entityFrontEndQuery.setAccountRestriction(accountRestriction);
        entityFrontEndQuery.setContactRestriction(contactRestriction);

        return entityFrontEndQuery;
    }

    private Restriction lookupColumnRestriction(String lookupColumn) {
        return Restriction.builder()//
                .let(BusinessEntity.Account, lookupColumn).isNotNull()//
                .build();
    }

    private Restriction createRatingEngineRestriction(String ratingEngineId, String ratingBucket) {
        String ratingField = RatingEngine.toRatingAttrName(ratingEngineId, RatingEngine.ScoreType.Rating);
        RestrictionBuilder ratingRestrictionbuilder = Restriction.builder().let(BusinessEntity.Rating, ratingField);

        return (StringUtils.isNotBlank(ratingBucket)) ? ratingRestrictionbuilder.eq(ratingBucket).build()
                : ratingRestrictionbuilder.isNotNull().build();
    }

    private FrontEndQuery createEntityFronEndQueryForSegmentAndRule(BusinessEntity entityType,
            boolean isRestrictNotNullSalesforceId, MetadataSegment segment,
            SegmentIdAndSingleRulePair segmentIdSingleRulePair) {
        FrontEndQuery entityFrontEndQuery = new FrontEndQuery();

        entityFrontEndQuery.setMainEntity(entityType);

        FrontEndRestriction accountRestriction = prepareFrontEndRestriction(prepareRestrictionList(
                segment.getAccountRestriction(), segmentIdSingleRulePair.getAccountRestriction()));
        FrontEndRestriction contactRestriction = prepareFrontEndRestriction(prepareRestrictionList(
                segment.getContactRestriction(), segmentIdSingleRulePair.getContacttRestriction()));

        entityFrontEndQuery.setAccountRestriction(accountRestriction);
        entityFrontEndQuery.setContactRestriction(contactRestriction);
        entityFrontEndQuery.setRestrictNotNullSalesforceId(isRestrictNotNullSalesforceId);

        return entityFrontEndQuery;
    }

    private List<Restriction> prepareRestrictionList(Restriction segmenEntitytRestriction,
            Restriction ruleEntityRestriction) {
        List<Restriction> restrictionList = new ArrayList<>();

        if (segmenEntitytRestriction != null) {
            restrictionList.add(segmenEntitytRestriction);
        }

        if (ruleEntityRestriction != null) {
            restrictionList.add(ruleEntityRestriction);
        }

        return restrictionList;
    }

    private FrontEndRestriction prepareFrontEndRestriction(List<Restriction> restrictions) {
        Restriction finalRestriction;

        if (restrictions.size() == 0) {
            finalRestriction = null;
        } else if (restrictions.size() == 1) {
            finalRestriction = restrictions.get(0);
        } else {
            finalRestriction = Restriction.builder().and(restrictions).build();
        }

        return new FrontEndRestriction(finalRestriction);
    }

    // this feature is not yet needed so impl is dummy
    private void processRatingEngineModelIds(RatingsCountRequest request, RatingsCountResponse result) {
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
            for (RatingBucketName bucket : RatingBucketName.values()) {
                int partsInBucket = bucket.ordinal() + 1;
                long countInBucket = (accountCount * partsInBucket) / totalParts;
                if (bucket == RatingBucketName.F) {
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
    }

    private void logInErrorMap(final Map<String, String> errorMap, final String key, final String msg) {
        try {
            errorMap.put(key, msg);
        } catch (Exception ex) {
            log.info("Ignoring unexpected error while putting msg in error map for key: " + key, ex);
        }
    }

    private void errorMapCleanup(RatingsCountResponse result, Map<String, Map<String, String>> uberErrorMap) {
        if (MapUtils.isNotEmpty(uberErrorMap)) {
            Set<String> keySet = new HashSet<>();
            keySet.addAll(uberErrorMap.keySet());

            keySet.stream() //
                    .filter(k -> MapUtils.isEmpty(uberErrorMap.get(k))) //
                    .forEach(uberErrorMap::remove);
        }
        if (MapUtils.isEmpty(uberErrorMap)) {
            result.setErrorMap(null);
        }
    }

    @VisibleForTesting
    void setRatingEngineService(RatingEngineService ratingEngineService) {
        this.ratingEngineService = ratingEngineService;
    }

    @VisibleForTesting
    void setEntityProxy(EntityProxy entityProxy) {
        this.entityProxy = entityProxy;
    }

    @VisibleForTesting
    void setRatingProxy(RatingProxy ratingProxy) {
        this.ratingProxy = ratingProxy;
    }

    @Override
    public RatingEnginesCoverageResponse getRatingCoveragesForSegment(String customerSpace, String segmentName,
            RatingEnginesCoverageRequest request) {
        RatingEnginesCoverageResponse response;
        Tenant tenant = MultiTenantContext.getTenant();

        MetadataSegment targetSegment = segmentService.findByName(segmentName);
        if (targetSegment == null) {
            throw new LedpException(LedpCode.LEDP_40045, new String[] { segmentName });
        }

        if (StringUtils.isBlank(request.getLookupId())) {
            throw new LedpException(LedpCode.LEDP_40070);
        }

        if (request.getRatingEngineIds().size() < thresholdForParallelProcessing) {
            // it is more efficient to use sequential stream (will use current
            // thread) if
            // collection size is small. It also ensures that small requests are
            // not blocked
            // if threadpool is used by bigger requests
            Stream<String> stream = request.getRatingEngineIds().stream();
            response = processRatingModelStream(tenant, stream, targetSegment, request);
        } else {
            response = tpForParallelStream.submit(() -> {
                Stream<String> parallelStream = request.getRatingEngineIds().stream().parallel();
                return processRatingModelStream(tenant, parallelStream, targetSegment, request);
            }).join();
        }

        return response;
    }

    // This method can accept parallel or sequential stream
    private RatingEnginesCoverageResponse processRatingModelStream(Tenant tenant, Stream<String> stream,
            MetadataSegment targetSegment, RatingEnginesCoverageRequest request) {
        RatingEnginesCoverageResponse response = new RatingEnginesCoverageResponse();
        stream.forEach(ratingModelId -> {
            try {
                CoverageInfo coverageInfo = processSingleRatingId(tenant, targetSegment, ratingModelId,
                        request.isRestrictNullLookupId(), request.getLookupId(), request.isLoadContactsCount(),
                        request.isLoadContactsCountByBucket(), request.isApplyEmailFilter());
                response.getRatingModelsCoverageMap().put(ratingModelId, coverageInfo);
            } catch (Exception ex) {
                log.info("Ignoring exception in getting coverage info for rating id: " + ratingModelId, ex);
                response.getErrorMap().put(ratingModelId,
                        StringUtils.isNotBlank(ex.getMessage()) ? ex.getMessage() : "null");
            }
        });
        return response;
    }

    @Override
    public RatingEnginesCoverageResponse getProductCoveragesForSegment(String customerSpace,
            ProductsCoverageRequest productsCoverageRequest, Integer purchasedBeforePeriod) {
        RatingEnginesCoverageResponse response = null;

        Tenant tenant = MultiTenantContext.getTenant();

        RatingEngine ratingEngine = productsCoverageRequest.getRatingEngine();
        ModelingStrategy strategy = ((CrossSellRatingConfig) ratingEngine.getAdvancedRatingConfig())
                .getModelingStrategy();

        RatingModel ratingModel = ratingEngine.getLatestIteration();
        List<String> productIds = productsCoverageRequest.getProductIds();

        MetadataSegment targetSegment = ratingEngine.getSegment();
        AIModel aiModel = (AIModel) ratingModel;

        if (targetSegment == null) {
            throw new LedpException(LedpCode.LEDP_40045, new String[] { "" });
        }

        if (strategy == ModelingStrategy.CROSS_SELL_REPEAT_PURCHASE) {
            CrossSellModelingConfig advancedConfig = (CrossSellModelingConfig) aiModel.getAdvancedModelingConfig();
            advancedConfig.setModelingStrategy(strategy);
            Map<CrossSellModelingConfigKeys, ModelingConfigFilter> filters = advancedConfig.getFilters();

            if (filters == null) {
                filters = new HashMap<CrossSellModelingConfigKeys, ModelingConfigFilter>();
            }

            filters.put(CrossSellModelingConfigKeys.PURCHASED_BEFORE_PERIOD,
                    new ModelingConfigFilter(CrossSellModelingConfigKeys.PURCHASED_BEFORE_PERIOD,
                            ComparisonType.PRIOR_ONLY, purchasedBeforePeriod));

            advancedConfig.setFilters(filters);
        }

        List<ProductAndEventQueryPair> eventQueries = new ArrayList<ProductAndEventQueryPair>();
        productIds.forEach(productId -> {
            CrossSellModelingConfig advancedConfig = (CrossSellModelingConfig) aiModel.getAdvancedModelingConfig();
            advancedConfig.setTargetProducts(Collections.singletonList(productId));
            aiModel.setAdvancedModelingConfig(advancedConfig);
            EventFrontEndQuery efeq = ratingEngineService.getModelingQuery(tenant.getId(), ratingEngine, aiModel,
                    ModelingQueryType.TARGET, null);
            eventQueries.add(new ProductAndEventQueryPair(productId, efeq));
        });

        if (productIds.size() < thresholdForParallelProcessing) {
            Stream<ProductAndEventQueryPair> stream = eventQueries.stream();
            response = processEventFrontEndQueryStream(tenant, stream, targetSegment);
        } else {
            response = tpForParallelStream.submit(() -> {
                Stream<ProductAndEventQueryPair> parallelStream = eventQueries.stream().parallel();
                return processEventFrontEndQueryStream(tenant, parallelStream, targetSegment);
            }).join();
        }

        return response;
    }

    private RatingEnginesCoverageResponse processEventFrontEndQueryStream(Tenant tenant,
            Stream<ProductAndEventQueryPair> stream, MetadataSegment targetSegment) {
        RatingEnginesCoverageResponse response = new RatingEnginesCoverageResponse();
        stream.forEach(query -> {
            try {
                log.info("Event front end query for products: " + JsonUtils.serialize(query));
                CoverageInfo coverageInfo = processSingleQuery(tenant, query.getEventFrontEndQuery(), targetSegment);
                response.getRatingModelsCoverageMap().put(query.getProductId(), coverageInfo);
            } catch (Exception ex) {
                log.info("Ignoring exception in getting coverage info for product id: " + query.getProductId(), ex);
                response.getErrorMap().put(query.getProductId(), ex != null ? ex.getMessage() : "null");
                response.getErrorMap().put("debug", "true");
            }
        });
        return response;
    }

    private CoverageInfo processSingleQuery(Tenant tenant, EventFrontEndQuery query, MetadataSegment targetSegment) {
        try {
            MultiTenantContext.setTenant(tenant);
            Long count = eventProxy.getScoringCount(tenant.getId(), query);

            CoverageInfo coverageInfo = new CoverageInfo();
            coverageInfo.setUnscoredAccountCount(count);

            if (targetSegment.getAccounts() != null && targetSegment.getAccounts() > 0) {
                coverageInfo.setAccountCount(targetSegment.getAccounts() - coverageInfo.getUnscoredAccountCount());
            }

            return coverageInfo;
        } catch (Exception ex) {
            throw ex;
        }
    }
}
