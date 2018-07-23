package com.latticeengines.objectapi.service.impl;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.stream.Collectors;

import javax.inject.Inject;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.collections4.MapUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.db.exposed.util.MultiTenantContext;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.datacloud.statistics.Bucket;
import com.latticeengines.domain.exposed.metadata.DataCollection;
import com.latticeengines.domain.exposed.metadata.InterfaceName;
import com.latticeengines.domain.exposed.metadata.statistics.AttributeRepository;
import com.latticeengines.domain.exposed.pls.RatingEngine;
import com.latticeengines.domain.exposed.query.AggregateLookup;
import com.latticeengines.domain.exposed.query.AttributeLookup;
import com.latticeengines.domain.exposed.query.BusinessEntity;
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
import com.latticeengines.objectapi.util.AccountQueryDecorator;
import com.latticeengines.objectapi.util.ContactQueryDecorator;
import com.latticeengines.objectapi.util.EntityQueryTranslator;
import com.latticeengines.objectapi.util.ProductQueryDecorator;
import com.latticeengines.objectapi.util.QueryDecorator;
import com.latticeengines.objectapi.util.QueryServiceUtils;
import com.latticeengines.query.exposed.evaluator.QueryEvaluatorService;
import com.latticeengines.query.exposed.exception.QueryEvaluationException;

import reactor.core.publisher.Flux;

@Service("entityQueryService")
public class EntityQueryServiceImpl implements EntityQueryService {

    private static final Logger log = LoggerFactory.getLogger(EntityQueryServiceImpl.class);

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
            Query query = queryTranslator.translateEntityQuery(frontEndQuery, decorator, timeTranslator, sqlUser);
            query.setLookups(Collections.singletonList(new EntityLookup(frontEndQuery.getMainEntity())));
            return queryEvaluatorService.getCount(attrRepo, query, sqlUser);
        } catch (Exception e) {
            throw new QueryEvaluationException("Failed to execute query " + JsonUtils.serialize(frontEndQuery), e);
        }
    }

    @Override
    public DataPage getData(FrontEndQuery frontEndQuery, DataCollection.Version version, String sqlUser,
            boolean enforceTranslation) {
        Flux<Map<String, Object>> flux = getDataFlux(frontEndQuery, version, sqlUser, enforceTranslation);
        List<Map<String, Object>> data = flux.toStream().collect(Collectors.toList());
        return new DataPage(data);
    }

    private Flux<Map<String, Object>> getDataFlux(FrontEndQuery frontEndQuery, DataCollection.Version version,
            String sqlUser, boolean enforceTranslation) {
        CustomerSpace customerSpace = MultiTenantContext.getCustomerSpace();
        AttributeRepository attrRepo = QueryServiceUtils.checkAndGetAttrRepo(customerSpace, version,
                queryEvaluatorService);
        try {
            EntityQueryTranslator queryTranslator = new EntityQueryTranslator(queryEvaluatorService.getQueryFactory(),
                    attrRepo);
            QueryDecorator decorator = getDecorator(frontEndQuery.getMainEntity(), true);
            TimeFilterTranslator timeTranslator = QueryServiceUtils.getTimeFilterTranslator(transactionService,
                    frontEndQuery);
            Query query = queryTranslator.translateEntityQuery(frontEndQuery, decorator, timeTranslator, sqlUser);
            if (query.getLookups() == null || query.getLookups().isEmpty()) {
                query.addLookup(new EntityLookup(frontEndQuery.getMainEntity()));
            }
            query = preProcess(frontEndQuery.getMainEntity(), query);

            Map<String, Map<Long, String>> translationMapping = new HashMap<>();

            if (enforceTranslation) {
                query.getLookups() //
                        .stream() //
                        .filter(lookup -> lookup instanceof AttributeLookup) //
                        .map(lookup -> (AttributeLookup) lookup) //
                        .map(attrLookup -> attrRepo.getColumnMetadata(attrLookup)) //
                        .filter(cm -> cm != null //
                                && cm.getStats() != null //
                                && cm.getStats().getBuckets() != null //
                                && CollectionUtils.isNotEmpty(cm.getStats().getBuckets().getBucketList()))
                        .forEach(cm -> {
                            Map<Long, String> enumMap = new HashMap<>();
                            List<Bucket> bucketList = cm.getStats().getBuckets().getBucketList();
                            bucketList.stream() //
                                    .forEach(bucket -> enumMap.put(bucket.getId(), bucket.getLabel()));
                            translationMapping.put(cm.getAttrName(), enumMap);
                        });
            }

            return queryEvaluatorService.getDataFlux(attrRepo, query, sqlUser) //
                    .map(row -> postProcess(frontEndQuery.getMainEntity(), row, enforceTranslation,
                            translationMapping));
        } catch (Exception e) {
            throw new QueryEvaluationException("Failed to execute query " + JsonUtils.serialize(frontEndQuery), e);
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
                throw new QueryEvaluationException("Failed to execute query " + JsonUtils.serialize(frontEndQuery), e);
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

    private Query preProcess(BusinessEntity entity, Query query) {
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
        return query;
    }

    private boolean notContactCompanyNameLookup(Lookup lookup) {
        return !isContactCompanyNameLookup(lookup);
    }

    private boolean isContactCompanyNameLookup(Lookup lookup) {
        if (lookup instanceof AttributeLookup) {
            AttributeLookup attrLookup = (AttributeLookup) lookup;
            String attributeName = attrLookup.getAttribute();
            if (attributeName.equals(InterfaceName.CompanyName.toString())
                    && BusinessEntity.Contact == attrLookup.getEntity()) {
                return true;
            }
        }
        return false;
    }

    private Map<String, Object> postProcess(BusinessEntity entity, Map<String, Object> result,
            boolean enforceTranslation, Map<String, Map<Long, String>> translationMapping) {
        Map<String, Object> processed = result;
        if (BusinessEntity.Account.equals(entity) || BusinessEntity.Contact.equals(entity)) {
            if (result.containsKey(InterfaceName.CompanyName.toString())
                    && result.containsKey(InterfaceName.LDC_Name.toString())) {
                processed = new HashMap<>();
                result.forEach(processed::put);
                String companyName = (String) processed.get(InterfaceName.CompanyName.toString());
                String ldcName = (String) processed.get(InterfaceName.LDC_Name.toString());
                String consolidatedName = (ldcName != null) ? ldcName : companyName;
                if (consolidatedName != null) {
                    processed.put(InterfaceName.CompanyName.toString(), consolidatedName);
                }
            }
        }

        if (enforceTranslation //
                && MapUtils.isNotEmpty(translationMapping) //
                && MapUtils.isNotEmpty(processed)) {
            final Map<String, Object> tempProcessed = processed;
            processed.keySet() //
                    .stream() //
                    .filter(key -> translationMapping.containsKey(key)) //
                    .forEach(key -> { //
                        Object val = tempProcessed.get(key);
                        if (val != null && val instanceof Long) {
                            Long enumNumeric = (Long) val;
                            tempProcessed.put(key, translationMapping.get(key).get(enumNumeric));
                        }
                    });
        }
        return processed;
    }

    private QueryDecorator getDecorator(BusinessEntity entity, boolean isDataQuery) {
        switch (entity) {
        case Account:
            return isDataQuery ? AccountQueryDecorator.DATA_QUERY : AccountQueryDecorator.COUNT_QUERY;
        case Contact:
            return isDataQuery ? ContactQueryDecorator.DATA_QUERY : ContactQueryDecorator.COUNT_QUERY;
        case Product:
            return isDataQuery ? ProductQueryDecorator.DATA_QUERY : ProductQueryDecorator.COUNT_QUERY;
        default:
            log.warn("Cannot find a decorator for entity " + entity);
            return null;
        }
    }

}
