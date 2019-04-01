package com.latticeengines.objectapi.service.impl;

import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import javax.annotation.Resource;
import javax.inject.Inject;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.collections4.MapUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jdbc.core.JdbcTemplate;

import com.latticeengines.db.exposed.util.MultiTenantContext;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.metadata.DataCollection;
import com.latticeengines.domain.exposed.metadata.statistics.AttributeRepository;
import com.latticeengines.domain.exposed.query.AggregateLookup;
import com.latticeengines.domain.exposed.query.AttributeLookup;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.domain.exposed.query.ComparisonType;
import com.latticeengines.domain.exposed.query.DataPage;
import com.latticeengines.domain.exposed.query.Lookup;
import com.latticeengines.domain.exposed.query.Query;
import com.latticeengines.domain.exposed.util.TimeFilterTranslator;
import com.latticeengines.objectapi.util.AccountQueryDecorator;
import com.latticeengines.objectapi.util.ContactQueryDecorator;
import com.latticeengines.objectapi.util.ProductQueryDecorator;
import com.latticeengines.objectapi.util.QueryDecorator;
import com.latticeengines.objectapi.util.QueryServiceUtils;
import com.latticeengines.query.exposed.evaluator.QueryEvaluatorService;
import com.latticeengines.query.exposed.translator.DayRangeTranslator;
import com.latticeengines.query.factory.RedshiftQueryProvider;

public abstract class BaseQueryServiceImpl {

    private static final Logger log = LoggerFactory.getLogger(BaseQueryServiceImpl.class);

    @Resource(name = "redshiftJdbcTemplate")
    private JdbcTemplate redshiftJdbcTemplate;

    @Inject
    private QueryEvaluatorService queryEvaluatorService;

    QueryDecorator getDecorator(BusinessEntity entity, boolean isDataQuery) {
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

    private Map<AttributeLookup, Object> getMaxDates(Set<AttributeLookup> lookups, AttributeRepository attrRepo) {
        String tenantId = MultiTenantContext.getTenant().getId();
        // Currently, only account and contact entity can have date attributes
        List<AttributeLookup> accountMaxLookups = new ArrayList<>();
        List<AttributeLookup> contactMaxLookups = new ArrayList<>();
        Map<AttributeLookup, Object> results = new HashMap<>();
        for (AttributeLookup lookup : lookups) {
            if (BusinessEntity.Account.equals(lookup.getEntity())) {
                accountMaxLookups.add(lookup);
            } else if (BusinessEntity.Contact.equals(lookup.getEntity())) {
                contactMaxLookups.add(lookup);
            } else {
                throw new UnsupportedOperationException(
                        String.format("Entity %s should not have Date Attribute.", lookup.getEntity().name()));
            }
        }
        if (CollectionUtils.isNotEmpty(accountMaxLookups)) {
            String accountTableName = queryEvaluatorService.getAndValidateServingStoreTableName(
                    BusinessEntity.Account, attrRepo);
            log.info(String.format("Get accountTableName %s for %s", accountTableName, tenantId));

            String selections = accountMaxLookups.stream() //
                    .map(lookup -> MessageFormat.format("max({0}) as {0}", lookup.getAttribute())) //
                    .collect(Collectors.joining(","));

            String query = MessageFormat.format("SELECT {0} FROM {1}", selections, accountTableName);
            log.info("query for accountTableName " + query);
            Map<String, Object> retMap = redshiftJdbcTemplate.queryForMap(query);
            retMap.forEach((k, v) -> {
                results.put(new AttributeLookup(BusinessEntity.Account, k), v);
            });
        }

        if (CollectionUtils.isNotEmpty(contactMaxLookups)) {
            String contactTableName = queryEvaluatorService.getAndValidateServingStoreTableName(
                    BusinessEntity.Contact, attrRepo);
            log.info(String.format("Get contactTableName %s for %s", contactTableName, tenantId));

            String selections = contactMaxLookups.stream() //
                    .map(lookup -> MessageFormat.format("max({0}) as {0}", lookup.getAttribute())) //
                    .collect(Collectors.joining(","));

            String query = MessageFormat.format("SELECT {0} FROM {1}", selections, contactTableName);
            log.info("query for contactTableName " + query);
            Map<String, Object> retMap = redshiftJdbcTemplate.queryForMap(query);
            retMap.forEach((k, v) -> {
                results.put(new AttributeLookup(BusinessEntity.Contact, k), v);
            });
        }
        return results;
    }

    private Map<AttributeLookup, Object> getMaxDatesViaFrontEndQuery(Set<AttributeLookup> lookups,
                                                             DataCollection.Version version) {
        CustomerSpace customerSpace = MultiTenantContext.getCustomerSpace();
        AttributeRepository attrRepo = QueryServiceUtils.checkAndGetAttrRepo(customerSpace, version,
                queryEvaluatorService);
        // System.out.println(JsonUtils.serialize(attrRepo));
        // Currently, only account and contact entity can have date attributes
        List<AggregateLookup> accountMaxLookups = new ArrayList<>();
        List<AggregateLookup> contactMaxLookups = new ArrayList<>();
        Map<AttributeLookup, Object> results = new HashMap<>();
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
            map.forEach((k, v) -> {
                results.put(new AttributeLookup(BusinessEntity.Account, k), v);
            });
        }
        if (CollectionUtils.isNotEmpty(contactMaxLookups)) {
            Query contactQuery = Query.builder() //
                    .select(contactMaxLookups.toArray(new Lookup[contactMaxLookups.size()])) //
                    .from(BusinessEntity.Contact) //
                    .build();
            DataPage dataPage = queryEvaluatorService.getData(attrRepo, contactQuery,
                    RedshiftQueryProvider.USER_SEGMENT);
            Map<String, Object> map = dataPage.getData().get(0);
            map.forEach((k, v) -> {
                results.put(new AttributeLookup(BusinessEntity.Contact, k), v);
            });
        }
        return results;
    }

    void preprocess(Map<ComparisonType, Set<AttributeLookup>> map, AttributeRepository attrRepo,
            TimeFilterTranslator timeTranslator) {
        if (MapUtils.isNotEmpty(map)) {
            for (ComparisonType type : map.keySet()) {
                switch (type) {
                case LATEST_DAY:
                    Map<AttributeLookup, Object> maxDates = getMaxDates(map.get(type), attrRepo);
                    updateTimeFilterTranslator(timeTranslator, type, maxDates);
                    break;
                default:
                    throw new UnsupportedOperationException(
                            String.format("ComparisonType %s is not supported for pre-processing.", type));
                }
            }
        }
    }

    private void updateTimeFilterTranslator(TimeFilterTranslator timeTranslator, ComparisonType type,
            Map<AttributeLookup, Object> maxDates) {
        Map<AttributeLookup, List<Object>> specifiedValues = timeTranslator.getSpecifiedValues().get(type);
        maxDates.forEach((k, v) -> {
            specifiedValues.put(k, Arrays.asList(DayRangeTranslator.getStartOfDayByTimestamp(v),
                    DayRangeTranslator.getEndOfDayByTimestamp(v)));
        });
    }

}
