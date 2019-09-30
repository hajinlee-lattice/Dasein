package com.latticeengines.objectapi.service.impl;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

import javax.inject.Inject;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.db.exposed.util.MultiTenantContext;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.datacloud.statistics.Bucket;
import com.latticeengines.domain.exposed.metadata.ColumnMetadata;
import com.latticeengines.domain.exposed.metadata.DataCollection;
import com.latticeengines.domain.exposed.metadata.statistics.AttributeRepository;
import com.latticeengines.domain.exposed.pls.RatingEngine;
import com.latticeengines.domain.exposed.query.AggregateLookup;
import com.latticeengines.domain.exposed.query.AttributeLookup;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.domain.exposed.query.ComparisonType;
import com.latticeengines.domain.exposed.query.DataPage;
import com.latticeengines.domain.exposed.query.EntityLookup;
import com.latticeengines.domain.exposed.query.GroupBy;
import com.latticeengines.domain.exposed.query.Lookup;
import com.latticeengines.domain.exposed.query.Query;
import com.latticeengines.domain.exposed.query.Restriction;
import com.latticeengines.domain.exposed.query.frontend.FrontEndQuery;
import com.latticeengines.domain.exposed.query.frontend.FrontEndRestriction;
import com.latticeengines.domain.exposed.query.frontend.RatingEngineFrontEndQuery;
import com.latticeengines.domain.exposed.util.TimeFilterTranslator;
import com.latticeengines.objectapi.service.EntityQueryService;
import com.latticeengines.objectapi.service.TransactionService;
import com.latticeengines.objectapi.util.EntityQueryTranslator;
import com.latticeengines.objectapi.util.QueryServiceUtils;
import com.latticeengines.query.exposed.evaluator.QueryEvaluatorService;
import com.latticeengines.query.exposed.exception.QueryEvaluationException;

import reactor.core.publisher.Flux;

@Service("entityQueryService")
public class EntityQueryServiceImpl extends BaseQueryServiceImpl implements EntityQueryService {

    private static final Logger log = LoggerFactory.getLogger(EntityQueryServiceImpl.class);

    private final TransactionService transactionService;

    @Inject
    public EntityQueryServiceImpl(QueryEvaluatorService queryEvaluatorService, TransactionService transactionService) {
        super(queryEvaluatorService);
        this.transactionService = transactionService;
    }

    protected TransactionService getTransactionService() {
        return this.transactionService;
    }

    @Override
    public long getCount(FrontEndQuery frontEndQuery, DataCollection.Version version, String sqlUser) {
        CustomerSpace customerSpace = MultiTenantContext.getCustomerSpace();
        AttributeRepository attrRepo = QueryServiceUtils.checkAndGetAttrRepo(customerSpace, version,
                queryEvaluatorService);
        try {
            Query query = getQuery(attrRepo, frontEndQuery, sqlUser, true);
            if (QueryServiceUtils.getQueryLoggingConfig()){
                log.info("getData using query: {}",
                        getQueryStr(frontEndQuery, version, sqlUser, true)
                        .replaceAll("\\r\\n|\\r|\\n", " "));
            }
            return queryEvaluatorService.getCount(attrRepo, query, sqlUser);
        } catch (Exception e) {
            String msg = "Failed to execute query " + JsonUtils.serialize(frontEndQuery) //
                    + " for tenant " + MultiTenantContext.getShortTenantId();
            if (version != null) {
                msg += " in " + version;
            }
            throw new QueryEvaluationException(msg, e);
        }
    }

    @Override
    public DataPage getData(FrontEndQuery frontEndQuery, DataCollection.Version version, String sqlUser,
            boolean enforceTranslation) {
        Flux<Map<String, Object>> flux = getDataFlux(frontEndQuery, version, sqlUser, enforceTranslation);
        List<Map<String, Object>> data = flux.collectList().block();
        return new DataPage(data);
    }

    @Override
    public String getQueryStr(FrontEndQuery frontEndQuery, DataCollection.Version version, String sqlUser,
            boolean isCountQuery) {
        CustomerSpace customerSpace = MultiTenantContext.getCustomerSpace();
        AttributeRepository attrRepo = QueryServiceUtils.checkAndGetAttrRepo(customerSpace, version,
                queryEvaluatorService);
        Query query = getQuery(attrRepo, frontEndQuery, sqlUser, isCountQuery);
        try {
            return queryEvaluatorService.getQueryStr(attrRepo, query, sqlUser);
        } catch (Exception e) {
            String msg = "Failed to construct query string " + JsonUtils.serialize(frontEndQuery) //
                    + " for tenant " + MultiTenantContext.getShortTenantId();
            if (version != null) {
                msg += " in " + version;
            }
            throw new QueryEvaluationException(msg, e);
        }
    }

    @Override
    public Query getQuery(AttributeRepository attrRepo, FrontEndQuery frontEndQuery, String sqlUser,
            boolean isCountQuery) {
        EntityQueryTranslator queryTranslator = new EntityQueryTranslator(queryEvaluatorService.getQueryFactory(),
                attrRepo);
        TimeFilterTranslator timeTranslator = QueryServiceUtils.getTimeFilterTranslator(transactionService,
                frontEndQuery);
        if (timeTranslator == null) {
            log.warn("for tenant " + MultiTenantContext.getShortTenantId() + ", timeTranslator is null");
        }
        Map<ComparisonType, Set<AttributeLookup>> map = queryTranslator.needPreprocess(frontEndQuery, timeTranslator);
        preprocess(map, attrRepo, timeTranslator);
        Query query = queryTranslator.translateEntityQuery(frontEndQuery, isCountQuery, timeTranslator, sqlUser);
        if (isCountQuery && !Boolean.TRUE.equals(frontEndQuery.getDistinct())) {
            query.setLookups(Collections.singletonList(new EntityLookup(frontEndQuery.getMainEntity())));
        } else {
            if (CollectionUtils.isEmpty(query.getLookups())) {
                query.addLookup(new EntityLookup(frontEndQuery.getMainEntity()));
            }
        }
        return query;
    }

    private Flux<Map<String, Object>> getDataFlux(FrontEndQuery frontEndQuery, DataCollection.Version version,
            String sqlUser, boolean enforceTranslation) {
        CustomerSpace customerSpace = MultiTenantContext.getCustomerSpace();
        AttributeRepository attrRepo = QueryServiceUtils.checkAndGetAttrRepo(customerSpace, version,
                queryEvaluatorService);
        Query query = getQuery(attrRepo, frontEndQuery, sqlUser, false);
        if (QueryServiceUtils.getQueryLoggingConfig()){
            log.info("getData using query:" + System.lineSeparator()
            + getQueryStr(frontEndQuery, version, sqlUser, false));
        }
        try {
            if (enforceTranslation) {
                Map<String, Map<Long, String>> translationMapping = getDecodeMapping(attrRepo, query.getLookups());
                return queryEvaluatorService.getDataFlux(attrRepo, query, sqlUser, translationMapping);
            } else {
                return queryEvaluatorService.getDataFlux(attrRepo, query, sqlUser);
            }
        } catch (Exception e) {
            String msg = "Failed to execute query " + JsonUtils.serialize(frontEndQuery) //
                    + " for tenant " + MultiTenantContext.getShortTenantId();
            if (version != null) {
                msg += " in " + version;
            }
            throw new QueryEvaluationException(msg, e);
        }
    }

    @Override
    public Map<String, Long> getRatingCount(RatingEngineFrontEndQuery frontEndQuery, DataCollection.Version version,
            String sqlUser) {
        String ratingEngineId = frontEndQuery.getRatingEngineId();
        if (StringUtils.isNotBlank(ratingEngineId)) {
            try {
                String ratingField = RatingEngine.toRatingAttrName(ratingEngineId, RatingEngine.ScoreType.Rating);
                CustomerSpace customerSpace = MultiTenantContext.getCustomerSpace();
                Query query = ratingCountQuery(customerSpace, ratingField, frontEndQuery, version, sqlUser);
                List<Map<String, Object>> data = queryEvaluatorService
                        .getData(customerSpace.toString(), version, query, sqlUser).getData();
                TreeMap<String, Long> counts = new TreeMap<>();
                data.forEach(map -> {
                    String rating = (String) map.get(ratingField);
                    if (StringUtils.isNotBlank(rating)) {
                        if (!counts.containsKey(ratingField)) {
                            counts.put(rating, 0L);
                        }
                        System.out.println(String.format("DEBUG: SQL User: %s, Rating: %s, Value: %s", sqlUser, rating,
                                map.get("count")));
                        counts.put(rating, counts.get(rating) + (Long) map.get("count"));
                    }
                });
                return counts;
            } catch (Exception e) {
                String msg = "Failed to execute query " + JsonUtils.serialize(frontEndQuery) //
                        + " for tenant " + MultiTenantContext.getShortTenantId();
                if (version != null) {
                    msg += " in " + version;
                }
                throw new QueryEvaluationException(msg, e);
            }
        } else {
            throw new IllegalArgumentException("RatingEngineID cannot be empty for rating count query.");
        }
    }

    @Override
    public Map<String, Map<Long, String>> getDecodeMapping(AttributeRepository attrRepo, Collection<Lookup> lookups) {
        Map<String, Map<Long, String>> translationMapping = new HashMap<>();
        lookups.stream().filter(lookup -> lookup instanceof AttributeLookup) //
                .map(lookup -> {
                    AttributeLookup attributeLookup = (AttributeLookup) lookup;
                    ColumnMetadata cm = attrRepo.getColumnMetadata(attributeLookup);
                    if (cm != null && cm.getBitOffset() != null) {
                        // avoid in-place mutation of cached objects
                        ColumnMetadata cm2 = cm.clone();
                        cm2.setAttrName(attributeLookup.getAttribute());
                        return cm2;
                    } else {
                        return new ColumnMetadata();
                    }
                }) //
                .filter(cm -> StringUtils.isNotBlank(cm.getAttrName()) //
                        && cm.getStats() != null //
                        && cm.getStats().getBuckets() != null //
                        && CollectionUtils.isNotEmpty(cm.getStats().getBuckets().getBucketList()))
                .forEach(cm -> {
                    Map<Long, String> enumMap = new HashMap<>();
                    List<Bucket> bucketList = cm.getStats().getBuckets().getBucketList();
                    bucketList.forEach(bucket -> enumMap.put(bucket.getId(), bucket.getLabel()));
                    translationMapping.put(cm.getAttrName(), enumMap);
                });
        return translationMapping;
    }

    private Query ratingCountQuery(CustomerSpace customerSpace, String ratingField,
            RatingEngineFrontEndQuery frontEndQuery, DataCollection.Version version, String sqlUser) {
        Restriction accountRestriction = frontEndQuery.getAccountRestriction() == null ? null
                : frontEndQuery.getAccountRestriction().getRestriction();
        Restriction restriction = Restriction.builder().let(BusinessEntity.Rating, ratingField).isNotNull().build();
        if (accountRestriction != null) {
            restriction = Restriction.builder().and(accountRestriction, restriction).build();
        }
        frontEndQuery.setAccountRestriction(new FrontEndRestriction(restriction));

        AttributeRepository attrRepo = QueryServiceUtils.checkAndGetAttrRepo(customerSpace, version,
                queryEvaluatorService);
        EntityQueryTranslator queryTranslator = new EntityQueryTranslator(queryEvaluatorService.getQueryFactory(),
                attrRepo);
        TimeFilterTranslator timeTranslator = QueryServiceUtils.getTimeFilterTranslator(transactionService,
                frontEndQuery);
        Map<ComparisonType, Set<AttributeLookup>> map = queryTranslator.needPreprocess(frontEndQuery, timeTranslator);
        preprocess(map, attrRepo, timeTranslator);
        Query query = queryTranslator.translateEntityQuery(frontEndQuery, true, timeTranslator, sqlUser);
        query.setPageFilter(null);
        query.setSort(null);
        AttributeLookup ratingLookup = new AttributeLookup(BusinessEntity.Rating, ratingField);
        AggregateLookup countLookup = AggregateLookup.count().as("count");
        query.setLookups(Arrays.asList(ratingLookup, countLookup));
        GroupBy groupBy = new GroupBy();
        groupBy.setLookups(Collections.singletonList(ratingLookup));
        query.setGroupBy(groupBy);
        return query;
    }

}
