package com.latticeengines.objectapi.service.impl;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.stream.Collectors;

import javax.inject.Inject;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.collections4.MapUtils;
import org.apache.commons.lang3.StringUtils;
import org.springframework.stereotype.Service;

import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.db.exposed.util.MultiTenantContext;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.datacloud.statistics.Bucket;
import com.latticeengines.domain.exposed.metadata.ColumnMetadata;
import com.latticeengines.domain.exposed.metadata.DataCollection;
import com.latticeengines.domain.exposed.metadata.InterfaceName;
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
import com.latticeengines.objectapi.util.QueryDecorator;
import com.latticeengines.objectapi.util.QueryServiceUtils;
import com.latticeengines.query.exposed.evaluator.QueryEvaluatorService;
import com.latticeengines.query.exposed.exception.QueryEvaluationException;
import com.latticeengines.query.factory.RedshiftQueryProvider;

import reactor.core.publisher.Flux;

@Service("entityQueryService")
public class EntityQueryServiceImpl extends BaseQueryServiceImpl implements EntityQueryService {

    private final QueryEvaluatorService queryEvaluatorService;

    private final TransactionService transactionService;

    @Inject
    public EntityQueryServiceImpl(QueryEvaluatorService queryEvaluatorService, TransactionService transactionService) {
        this.queryEvaluatorService = queryEvaluatorService;
        this.transactionService = transactionService;
    }

    @Override
    public long getCount(FrontEndQuery frontEndQuery, DataCollection.Version version, String sqlUser) {
        CustomerSpace customerSpace = MultiTenantContext.getCustomerSpace();
        AttributeRepository attrRepo = QueryServiceUtils.checkAndGetAttrRepo(customerSpace, version,
                queryEvaluatorService);
        try {
            EntityQueryTranslator queryTranslator = new EntityQueryTranslator(queryEvaluatorService.getQueryFactory(),
                    attrRepo);
            QueryDecorator decorator = getDecorator(frontEndQuery.getMainEntity(), false);
            TimeFilterTranslator timeTranslator = QueryServiceUtils.getTimeFilterTranslator(transactionService,
                    frontEndQuery);
            // check whether need to preprocess FrontEndQuery?
            // 1. wether any restriction has the new operator?
            // return Map<attr: operator>
            // LASTED_DATE
            Map<ComparisonType, Set<AttributeLookup>> map = queryTranslator.needPreprocess(frontEndQuery,
                    timeTranslator);
            // Preprocess front end query by saving the max value in cache
            // map
            // 1) query max date
            // 2) round
            // augment timetranslator
            preprocess(map, frontEndQuery, timeTranslator);

            // replace frontend query in place
            Query query = queryTranslator.translateEntityQuery(frontEndQuery, decorator, timeTranslator, sqlUser);
            query.setLookups(Collections.singletonList(new EntityLookup(frontEndQuery.getMainEntity())));
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
    public String getQueryStr(FrontEndQuery frontEndQuery, DataCollection.Version version, String sqlUser) {
        Query query = getDataQuery(frontEndQuery, version, sqlUser);
        CustomerSpace customerSpace = MultiTenantContext.getCustomerSpace();
        AttributeRepository attrRepo = QueryServiceUtils.checkAndGetAttrRepo(customerSpace, version,
                queryEvaluatorService);
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

    private Query getDataQuery(FrontEndQuery frontEndQuery, DataCollection.Version version, String sqlUser) {
        CustomerSpace customerSpace = MultiTenantContext.getCustomerSpace();
        AttributeRepository attrRepo = QueryServiceUtils.checkAndGetAttrRepo(customerSpace, version,
                queryEvaluatorService);
        EntityQueryTranslator queryTranslator = new EntityQueryTranslator(queryEvaluatorService.getQueryFactory(),
                attrRepo);
        QueryDecorator decorator = getDecorator(frontEndQuery.getMainEntity(), true);
        TimeFilterTranslator timeTranslator = QueryServiceUtils.getTimeFilterTranslator(transactionService,
                frontEndQuery);
        Query query = queryTranslator.translateEntityQuery(frontEndQuery, decorator, timeTranslator, sqlUser);
        if (CollectionUtils.isEmpty(query.getLookups())) {
            query.addLookup(new EntityLookup(frontEndQuery.getMainEntity()));
        }
        updateLookups(frontEndQuery.getMainEntity(), query);
        return query;
    }

    private Flux<Map<String, Object>> getDataFlux(FrontEndQuery frontEndQuery, DataCollection.Version version,
            String sqlUser, boolean enforceTranslation) {
        Query query = getDataQuery(frontEndQuery, version, sqlUser);
        CustomerSpace customerSpace = MultiTenantContext.getCustomerSpace();
        AttributeRepository attrRepo = QueryServiceUtils.checkAndGetAttrRepo(customerSpace, version,
                queryEvaluatorService);
        try {
            Map<String, Map<Long, String>> translationMapping = new HashMap<>();
            if (enforceTranslation) {
                query.getLookups() //
                        .stream() //
                        .filter(lookup -> lookup instanceof AttributeLookup) //
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
            }

            return queryEvaluatorService.getDataFlux(attrRepo, query, sqlUser) //
                    .map(row -> postProcess(row, enforceTranslation, translationMapping));
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
        Query query = queryTranslator.translateEntityQuery(frontEndQuery, null, timeTranslator, sqlUser);
        query.setPageFilter(null);
        query.setSort(null);
        AttributeLookup ratingLookup = new AttributeLookup(BusinessEntity.Rating, ratingField);
        AggregateLookup countLookup = AggregateLookup.count().as("Count");
        query.setLookups(Arrays.asList(ratingLookup, countLookup));
        GroupBy groupBy = new GroupBy();
        groupBy.setLookups(Collections.singletonList(ratingLookup));
        query.setGroupBy(groupBy);
        return query;
    }

    private void updateLookups(BusinessEntity entity, Query query) {
        if (BusinessEntity.Contact == entity) {
            List<Lookup> lookups = query.getLookups();
            if (lookups != null && lookups.stream().anyMatch(this::isContactCompanyNameLookup)) {
                List<Lookup> filtered = lookups.stream().filter(this::notContactCompanyNameLookup)
                        .collect(Collectors.toList());
                filtered.add(new AttributeLookup(BusinessEntity.Account, InterfaceName.CompanyName.toString()));
                filtered.add(new AttributeLookup(BusinessEntity.Account, InterfaceName.LDC_Name.toString()));
                query.setLookups(filtered);
            }
        }
    }

    private boolean notContactCompanyNameLookup(Lookup lookup) {
        return !isContactCompanyNameLookup(lookup);
    }

    private boolean isContactCompanyNameLookup(Lookup lookup) {
        if (lookup instanceof AttributeLookup) {
            AttributeLookup attrLookup = (AttributeLookup) lookup;
            String attributeName = attrLookup.getAttribute();
            return attributeName.equals(InterfaceName.CompanyName.toString())
                    && BusinessEntity.Contact == attrLookup.getEntity();
        }
        return false;
    }

    void preprocess(Map<ComparisonType, Set<AttributeLookup>> map, FrontEndQuery frontEndQuery,
            TimeFilterTranslator timeTranslator) {
        if (MapUtils.isNotEmpty(map)) {
            for (ComparisonType type : map.keySet()) {
                switch (type) {
                case LASTEST_DAY:
                    break;
                default:
                    throw new UnsupportedOperationException(
                            String.format("ComparisonType %s is not supported for pre-processing.", type));
                }
            }
        }
    }

    private Map<String, Object> postProcess(Map<String, Object> result, boolean enforceTranslation,
            Map<String, Map<Long, String>> translationMapping) {
        if (enforceTranslation //
                && MapUtils.isNotEmpty(translationMapping) //
                && MapUtils.isNotEmpty(result)) {
            final Map<String, Object> tempProcessed = result;
            result.keySet() //
                    .stream() //
                    .filter(translationMapping::containsKey) //
                    .forEach(key -> { //
                        Object val = tempProcessed.get(key);
                        if (val instanceof Long) {
                            Long enumNumeric = (Long) val;
                            if (enumNumeric == 0L) { // 0 is null
                                tempProcessed.put(key, null);
                            } else if (translationMapping.get(key).containsKey(enumNumeric)) {
                                tempProcessed.put(key, translationMapping.get(key).get(enumNumeric));
                            } else {
                                tempProcessed.put(key, enumNumeric);
                            }
                        }
                    });
        }
        return result;
    }

    void getMaxDates(Set<AttributeLookup> lookups, DataCollection.Version version) {
        CustomerSpace customerSpace = MultiTenantContext.getCustomerSpace();
        AttributeRepository attrRepo = QueryServiceUtils.checkAndGetAttrRepo(customerSpace, version,
                queryEvaluatorService);
        // System.out.println(JsonUtils.serialize(attrRepo));
        // Currently, only account and contact entity can have date attributes
        List<AggregateLookup> accountMaxLookups = new ArrayList<>();
        List<AggregateLookup> contactMaxLookups = new ArrayList<>();
        for (AttributeLookup lookup : lookups) {
            if (BusinessEntity.Account.equals(lookup.getEntity())) {
                accountMaxLookups.add(AggregateLookup.max(lookup).as(lookup.getAttribute().toLowerCase()));
            } else if (BusinessEntity.Contact.equals(lookup.getEntity())) {
                contactMaxLookups.add(AggregateLookup.max(lookup).as(lookup.getAttribute().toLowerCase()));
            } else {
                throw new UnsupportedOperationException(
                        String.format("Entity %s should not have Date Attribute.", lookup.getEntity().name()));
            }
        }

        if (CollectionUtils.isNotEmpty(accountMaxLookups)) {
            Query accountQuery = Query.builder() //
                    .select(accountMaxLookups.toArray(new Lookup[accountMaxLookups.size()])) //
                    .from(BusinessEntity.Account) //
                    .build();
            DataPage dataPage = queryEvaluatorService.getData(attrRepo, accountQuery,
                    RedshiftQueryProvider.USER_SEGMENT);
            Map<String, Object> map = dataPage.getData().get(0);
            System.out.println("Account");
            for (String key : map.keySet()) {
                System.out.println(key + ": " + map.get(key).toString());
            }
        }
        if (CollectionUtils.isNotEmpty(contactMaxLookups)) {
            Query contactQuery = Query.builder() //
                    .select(contactMaxLookups.toArray(new Lookup[contactMaxLookups.size()])) //
                    .from(BusinessEntity.Contact) //
                    .build();
            DataPage dataPage = queryEvaluatorService.getData(attrRepo, contactQuery,
                    RedshiftQueryProvider.USER_SEGMENT);
            Map<String, Object> map = dataPage.getData().get(0);
            System.out.println("Contact");
            for (String key : map.keySet()) {
                System.out.println(key + ": " + map.get(key).toString());
            }
        }

    }

}
