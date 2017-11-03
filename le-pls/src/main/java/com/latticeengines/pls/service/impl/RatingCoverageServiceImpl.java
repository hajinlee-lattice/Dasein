package com.latticeengines.pls.service.impl;

import java.util.ArrayList;
import java.util.HashMap;
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

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import com.google.common.annotations.VisibleForTesting;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.common.exposed.util.ThreadPoolUtils;
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
import com.latticeengines.domain.exposed.pls.SegmentIdAndSingleRulePair;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.domain.exposed.query.Restriction;
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

    private static final String DEFAULT_ID_FOR_MODEL_RULE = "DEFAULT_ID_FOR_MODEL_RULE";

    @Value("${pls.rating.coverageservice.threadpool.size:10}")
    private Integer fetcherNum;

    @Value("${pls.rating.coverageservice.threshold.parallel:3}")
    private Integer thresholdForParallelProcessing;

    @Autowired
    private RatingEngineService ratingEngineService;

    @Autowired
    private MetadataSegmentService metadataSegmentService;

    @Autowired
    private EntityProxy entityProxy;

    private ForkJoinPool tpForParallelStream;

    Random rand = new Random(System.currentTimeMillis());

    @PostConstruct
    public void init() {
        tpForParallelStream = ThreadPoolUtils.getForkJoinThreadPool("rating-coverage", fetcherNum);
    }

    @Override
    public RatingsCountResponse getCoverageInfo(RatingsCountRequest request) {
        RatingsCountResponse result = new RatingsCountResponse();
        Map<String, Map<String, String>> uberErrorMap = new ConcurrentHashMap<>();
        result.setErrorMap(uberErrorMap);

        if (request.getRatingEngineIds() != null) {
            processRatingIds(request, result);
        }

        if (request.getSegmentIds() != null) {
            processSegmentIds(request, result);
        }

        if (request.getRatingEngineModelIds() != null) {
            processRatingEngineModelIds(request, result);
        }

        if (request.getSegmentIdModelRules() != null) {
            processSegmentIdModelRules(request, result);
        }

        if (request.getSegmentIdAndSingleRules() != null) {
            processSegmentIdSingleRules(request, result);
        }

        return result;
    }

    private void processRatingIds(RatingsCountRequest request, RatingsCountResponse result) {
        Tenant tenent = MultiTenantContext.getTenant();

        Map<String, CoverageInfo> ratingEngineIdCoverageMap = new ConcurrentHashMap<>();
        result.setRatingEngineIdCoverageMap(ratingEngineIdCoverageMap);
        Map<String, String> errorMap = new ConcurrentHashMap<>();
        result.getErrorMap().put(RATING_IDS_ERROR_MAP_KEY, errorMap);

        if (request.getRatingEngineIds().size() < thresholdForParallelProcessing) {
            // it is more efficient to use sequential stream (will use current
            // thread) if collection size is small. It also ensures that small
            // requests are not blocked if threadpool is used by bigger requests
            Stream<String> stream = //
                    request.getRatingEngineIds().stream();
            ratingEngineStreamProcessing(request, tenent, ratingEngineIdCoverageMap, errorMap, stream);
        } else {
            tpForParallelStream.submit(//
                    () -> //
                    {
                        Stream<String> parallelStream = //
                                request.getRatingEngineIds().stream() //
                                        .parallel();

                        ratingEngineStreamProcessing(request, tenent, ratingEngineIdCoverageMap, errorMap,
                                parallelStream);
                    }) //
                    .join();
        }
    }

    private void processSegmentIds(RatingsCountRequest request, RatingsCountResponse result) {
        Tenant tenent = MultiTenantContext.getTenant();

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
            segmentStreamProcessing(request, tenent, segmentIdCoverageMap, errorMap, stream);
        } else {
            tpForParallelStream.submit(//
                    () -> //
                    {
                        Stream<String> parallelStream = //
                                request.getSegmentIds().stream() //
                                        .parallel();

                        segmentStreamProcessing(request, tenent, segmentIdCoverageMap, errorMap, parallelStream);
                    }) //
                    .join();
        }
    }

    private void processSegmentIdModelRules(RatingsCountRequest request, RatingsCountResponse result) {
        Tenant tenent = MultiTenantContext.getTenant();

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
            segmentIdModelRulesStreamProcessing(request, tenent, segmentIdModelRulesCoverageMap, errorMap, stream);
        } else {
            tpForParallelStream.submit(//
                    () -> //
                    {
                        Stream<SegmentIdAndModelRulesPair> parallelStream = //
                                request.getSegmentIdModelRules().stream() //
                                        .parallel();

                        segmentIdModelRulesStreamProcessing(request, tenent, segmentIdModelRulesCoverageMap, errorMap,
                                parallelStream);
                    }) //
                    .join();
        }
    }

    private void processSegmentIdSingleRules(RatingsCountRequest request, RatingsCountResponse result) {
        Tenant tenent = MultiTenantContext.getTenant();

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
                    request, tenent, segmentMap, //
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
                                request, tenent, segmentMap, //
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
                            MetadataSegment segment = //
                                    metadataSegmentService.getSegmentByName( //
                                            segmentId, false);
                            segmentMap.put(segmentId, segment);
                        } catch (Exception ex) {
                            logInErrorMap(errorMap, segmentId, "Could not load segment");
                        }
                    });

        }
        return segmentMap;
    }

    // this method can accept parallel or sequential stream
    private void ratingEngineStreamProcessing(RatingsCountRequest request, Tenant tenent,
            Map<String, CoverageInfo> ratingEngineIdCoverageMap, Map<String, String> errorMap, Stream<String> stream) {
        stream //
                .forEach( //
                        ratingEngineId -> //
                        processSingleRatingId(tenent, ratingEngineIdCoverageMap, errorMap, //
                                ratingEngineId, request.isRestrictNotNullSalesforceId()));
    }

    // this method can accept parallel or sequential stream
    private void segmentStreamProcessing(RatingsCountRequest request, Tenant tenent,
            Map<String, CoverageInfo> segmentIdCoverageMap, Map<String, String> errorMap, Stream<String> stream) {
        stream //
                .forEach( //
                        segmentId -> //
                        processSingleSegmentId(tenent, segmentIdCoverageMap, errorMap, //
                                segmentId, request.isRestrictNotNullSalesforceId()));
    }

    // this method can accept parallel or sequential stream
    private void segmentIdModelRulesStreamProcessing(RatingsCountRequest request, Tenant tenent,
            Map<String, CoverageInfo> segmentIdModelRulesCoverageMap, Map<String, String> errorMap,
            Stream<SegmentIdAndModelRulesPair> stream) {
        stream //
                .forEach( //
                        segmentIdModelRulesPair -> //
                        processSingleSegmentIdModelRulesPair(tenent, segmentIdModelRulesCoverageMap, errorMap, //
                                segmentIdModelRulesPair, request.isRestrictNotNullSalesforceId()));
    }

    // this method can accept parallel or sequential stream
    private void segmentIdAndSingleRulesStreamProcessing(RatingsCountRequest request, Tenant tenent,
            Map<String, MetadataSegment> segmentMap, Map<String, CoverageInfo> segmentIdAndSingleRulesCoverageMap,
            Map<String, String> errorMap, Stream<SegmentIdAndSingleRulePair> stream) {

        stream //
                .forEach( //
                        segmentIdSingleRulesPair -> //
                        processSingleSegmentIdSingleRulesPair(tenent, segmentMap, segmentIdAndSingleRulesCoverageMap, errorMap, //
                                segmentIdSingleRulesPair, request.isRestrictNotNullSalesforceId()));
    }

    private void processSingleSegmentId(Tenant tenent, Map<String, CoverageInfo> segmentIdCoverageMap,
            Map<String, String> errorMap, String segmentId, boolean isRestrictNotNullSalesforceId) {
        try {
            MultiTenantContext.setTenant(tenent);

            MetadataSegment segment = //
                    metadataSegmentService.getSegmentByName(segmentId, false);

            if (segment == null) {
                logInErrorMap(errorMap, segmentId, "Invalid segment");
                return;
            }

            FrontEndQuery accountFrontEndQuery = //
                    createEntityFronEndQuery(BusinessEntity.Account, //
                            isRestrictNotNullSalesforceId, segment);
            FrontEndQuery contactFrontEndQuery = //
                    createEntityFronEndQuery(BusinessEntity.Contact, //
                            isRestrictNotNullSalesforceId, segment);

            loadBasicCoverage(tenent, segmentIdCoverageMap, segmentId, accountFrontEndQuery, contactFrontEndQuery);
        } catch (Exception ex) {
            log.info("Ignoring exception in getting coverage info for segment id: " + segmentId, ex);
            logInErrorMap(errorMap, segmentId, ex.getMessage());
        }
    }

    private void processSingleSegmentIdSingleRulesPair(Tenant tenent, Map<String, MetadataSegment> segmentMap,
            Map<String, CoverageInfo> segmentIdAndSingleRulesCoverageMap, Map<String, String> errorMap,
            SegmentIdAndSingleRulePair segmentIdSingleRulePair, boolean isRestrictNotNullSalesforceId) {
        try {
            MultiTenantContext.setTenant(tenent);

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

            loadBasicCoverage(tenent, segmentIdAndSingleRulesCoverageMap, segmentIdSingleRulePair.getResponseKeyId(),
                    accountFrontEndQuery, contactFrontEndQuery);
        } catch (Exception ex) {
            log.info("Ignoring exception in getting coverage info for segmentIdSingleRulePair: "
                    + segmentIdSingleRulePair, ex);
            logInErrorMap(errorMap, segmentIdSingleRulePair.getResponseKeyId(), ex.getMessage());
        }
    }

    private void processSingleRatingId(Tenant tenent, Map<String, CoverageInfo> ratingEngineIdCoverageMap,
            Map<String, String> errorMap, String ratingEngineId, boolean isRestrictNotNullSalesforceId) {
        try {
            MultiTenantContext.setTenant(tenent);

            RatingEngine ratingEngine = ratingEngineService.getRatingEngineById(ratingEngineId, false);

            if (ratingEngine == null || ratingEngine.getSegment() == null) {
                logInErrorMap(errorMap, ratingEngineId, "Invalid rating engine");
                return;
            }

            MetadataSegment segment = ratingEngine.getSegment();

            FrontEndQuery accountFrontEndQuery = //
                    createEntityFronEndQuery(BusinessEntity.Account, //
                            isRestrictNotNullSalesforceId, segment);
            FrontEndQuery contactFrontEndQuery = //
                    createEntityFronEndQuery(BusinessEntity.Contact, //
                            isRestrictNotNullSalesforceId, segment);

            List<RatingModel> ratingModels = new ArrayList<>();
            for (RatingModel model : ratingEngine.getRatingModels()) {
                ratingModels.add(model);
                break;
            }
            accountFrontEndQuery.setRatingModels(ratingModels);

            log.info("Front end query for Account: " + JsonUtils.serialize(accountFrontEndQuery));
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
            ratingEngineIdCoverageMap.put(ratingEngineId, coverageInfo);
        } catch (Exception ex) {
            log.info("Ignoring exception in getting coverage info for rating id: " + ratingEngineId, ex);
            logInErrorMap(errorMap, ratingEngineId, ex.getMessage());
        }

    }

    private void processSingleSegmentIdModelRulesPair(Tenant tenent,
            Map<String, CoverageInfo> segmentIdModelRulesCoverageMap, Map<String, String> errorMap,
            SegmentIdAndModelRulesPair segmentIdModelRulesPair, boolean isRestrictNotNullSalesforceId) {
        try {
            MultiTenantContext.setTenant(tenent);

            MetadataSegment segment = //
                    metadataSegmentService.getSegmentByName( //
                            segmentIdModelRulesPair.getSegmentId(), false);
            FrontEndQuery accountFrontEndQuery = //
                    createEntityFronEndQuery(BusinessEntity.Account, //
                            isRestrictNotNullSalesforceId, segment);
            FrontEndQuery contactFrontEndQuery = //
                    createEntityFronEndQuery(BusinessEntity.Contact, //
                            isRestrictNotNullSalesforceId, segment);

            List<RatingModel> ratingModels = new ArrayList<>();
            RuleBasedModel ratingModelWrapper = new RuleBasedModel();
            ratingModelWrapper.setRatingRule(segmentIdModelRulesPair.getRatingRule());
            ratingModels.add(ratingModelWrapper);
            if (StringUtils.isEmpty(ratingModelWrapper.getId())) {
                ratingModelWrapper.setId(DEFAULT_ID_FOR_MODEL_RULE);
            }
            accountFrontEndQuery.setRatingModels(ratingModels);

            log.info("Front end queryfor Account: " + JsonUtils.serialize(accountFrontEndQuery));
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

    void loadBasicCoverage(Tenant tenent, Map<String, CoverageInfo> segmentIdAndSingleRulesCoverageMap, String key,
            FrontEndQuery accountFrontEndQuery, FrontEndQuery contactFrontEndQuery) {
        log.info("Front end query for Account: " + JsonUtils.serialize(accountFrontEndQuery));
        Long accountCount = entityProxy.getCount( //
                tenent.getId(), //
                accountFrontEndQuery);
        Long contactCount = getContactCount(tenent, contactFrontEndQuery);

        CoverageInfo coverageInfo = new CoverageInfo();

        coverageInfo.setAccountCount(accountCount);
        coverageInfo.setContactCount(contactCount);

        segmentIdAndSingleRulesCoverageMap.put(key, coverageInfo);
    }

    private Long getContactCount(Tenant tenent, FrontEndQuery contactFrontEndQuery) {

        log.info("Front end query for Contact: " + JsonUtils.serialize(contactFrontEndQuery));
        return entityProxy.getCount( //
                tenent.getId(), //
                contactFrontEndQuery);
    }

    private FrontEndQuery createEntityFronEndQuery(BusinessEntity entityType, boolean isRestrictNotNullSalesforceId,
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

    private FrontEndQuery createEntityFronEndQueryForSegmentAndRule(BusinessEntity entityType,
            boolean isRestrictNotNullSalesforceId, MetadataSegment segment,
            SegmentIdAndSingleRulePair segmentIdSingleRulePair) {
        FrontEndQuery entityFrontEndQuery = new FrontEndQuery();

        entityFrontEndQuery.setMainEntity(entityType);

        FrontEndRestriction accountRestriction = prepareFronEndRestriction(prepareRestrctionList(
                segment.getAccountRestriction(), segmentIdSingleRulePair.getAccountRestriction()));
        FrontEndRestriction contactRestriction = prepareFronEndRestriction(prepareRestrctionList(
                segment.getContactRestriction(), segmentIdSingleRulePair.getContacttRestriction()));

        entityFrontEndQuery.setAccountRestriction(accountRestriction);
        entityFrontEndQuery.setContactRestriction(contactRestriction);
        entityFrontEndQuery.setRestrictNotNullSalesforceId(isRestrictNotNullSalesforceId);

        return entityFrontEndQuery;
    }

    private List<Restriction> prepareRestrctionList(Restriction segmenEntitytRestriction,
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

    private FrontEndRestriction prepareFronEndRestriction(List<Restriction> restrictions) {
        Restriction finalRestriction = null;

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
    }

    private void logInErrorMap(final Map<String, String> errorMap, final String key, final String msg) {
        try {
            errorMap.put(key, msg);
        } catch (Exception ex) {
            log.info("Ignoring unexpected error while putting msg in error map for key: " + key, ex);
        }
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
