package com.latticeengines.objectapi.service.impl;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.collections4.MapUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.latticeengines.common.exposed.util.DateTimeUtils;
import com.latticeengines.domain.exposed.metadata.statistics.AttributeRepository;
import com.latticeengines.domain.exposed.query.AggregateLookup;
import com.latticeengines.domain.exposed.query.AttributeLookup;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.domain.exposed.query.ComparisonType;
import com.latticeengines.domain.exposed.query.DataPage;
import com.latticeengines.domain.exposed.query.Lookup;
import com.latticeengines.domain.exposed.query.Query;
import com.latticeengines.domain.exposed.util.TimeFilterTranslator;
import com.latticeengines.query.exposed.evaluator.QueryEvaluatorService;
import com.latticeengines.query.factory.RedshiftQueryProvider;

public abstract class BaseQueryServiceImpl {

    private static final Logger log = LoggerFactory.getLogger(BaseQueryServiceImpl.class);

    protected QueryEvaluatorService queryEvaluatorService;

    BaseQueryServiceImpl(QueryEvaluatorService queryEvaluatorService) {
        this.queryEvaluatorService = queryEvaluatorService;
    }

    public QueryEvaluatorService getQueryEvaluatorService() {
        return queryEvaluatorService;
    }

    private Map<AttributeLookup, Object> getMaxDatesViaFrontEndQuery(Set<AttributeLookup> lookups,
            AttributeRepository attrRepo) {
        log.info("lookups are: " + lookups);
        // Currently, only account and contact entity can have date attributes
        List<AggregateLookup> accountMaxLookups = new ArrayList<>();
        List<AggregateLookup> contactMaxLookups = new ArrayList<>();
        Map<AttributeLookup, Object> results = new HashMap<>();
        for (AttributeLookup lookup : lookups) {
            if (BusinessEntity.Account.equals(lookup.getEntity())) {
                accountMaxLookups.add(AggregateLookup.max(lookup).as(lookup.getAttribute()));
            } else if (BusinessEntity.Contact.equals(lookup.getEntity())) {
                contactMaxLookups.add(AggregateLookup.max(lookup).as(lookup.getAttribute()));
            } else {
                throw new UnsupportedOperationException(
                        String.format("Entity %s should not have Date Attribute.", lookup.getEntity().name()));
            }
        }

        if (CollectionUtils.isNotEmpty(accountMaxLookups)) {
            Query accountQuery = Query.builder() //
                    .select(accountMaxLookups.toArray(new Lookup[0])) //
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
                    .select(contactMaxLookups.toArray(new Lookup[0])) //
                    .from(BusinessEntity.Contact) //
                    .build();
            DataPage dataPage = queryEvaluatorService.getData(attrRepo, contactQuery,
                    RedshiftQueryProvider.USER_SEGMENT);
            Map<String, Object> map = dataPage.getData().get(0);
            map.forEach((k, v) -> {
                results.put(new AttributeLookup(BusinessEntity.Contact, k), v);
            });
        }
        log.info("maxDate results are");
        results.forEach((k, v) -> log.info(k + ":" + v));
        return results;
    }

    void preprocess(Map<ComparisonType, Set<AttributeLookup>> map, AttributeRepository attrRepo,
            TimeFilterTranslator timeTranslator) {
        if (MapUtils.isNotEmpty(map)) {
            for (ComparisonType type : map.keySet()) {
                switch (type) {
                case LATEST_DAY:
                    Map<AttributeLookup, Object> maxDates = getMaxDatesViaFrontEndQuery(map.get(type), attrRepo);
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
            specifiedValues.put(k, Arrays.asList(DateTimeUtils.toDateOnlyFromMillis(v.toString()),
                    DateTimeUtils.toDateOnlyFromMillis(v.toString())));
        });
    }

}
