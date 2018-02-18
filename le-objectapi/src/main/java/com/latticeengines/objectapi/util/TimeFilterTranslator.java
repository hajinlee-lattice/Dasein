package com.latticeengines.objectapi.util;

import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.tuple.Pair;

import com.google.common.collect.ImmutableMap;
import com.latticeengines.common.exposed.period.PeriodBuilder;
import com.latticeengines.domain.exposed.cdl.PeriodBuilderFactory;
import com.latticeengines.domain.exposed.cdl.PeriodStrategy;
import com.latticeengines.domain.exposed.query.ComparisonType;
import com.latticeengines.domain.exposed.query.TimeFilter;

public class TimeFilterTranslator {

    private final ImmutableMap<String, PeriodBuilder> periodBuilders;
    private final ImmutableMap<String, Integer> maxPeriodIds;

    public TimeFilterTranslator(List<PeriodStrategy> strategyList, String maxTxnDate) {
        Map<String, Integer> maxPeriodMap = new HashMap<>();
        if (CollectionUtils.isNotEmpty(strategyList)) {
            Map<String, PeriodBuilder> builderMap = new HashMap<>();
            strategyList.forEach(strategy -> {
                PeriodBuilder builder = PeriodBuilderFactory.build(strategy);
                builderMap.put(strategy.getName(), builder);
                maxPeriodMap.put(strategy.getName(), builder.toPeriodId(maxTxnDate));
            });
            periodBuilders = ImmutableMap.copyOf(builderMap);
        } else {
            periodBuilders = ImmutableMap.copyOf(Collections.emptyMap());
        }
        this.maxPeriodIds = ImmutableMap.copyOf(maxPeriodMap);
    }

    public TimeFilter translate(TimeFilter timeFilter) {
        Pair<String, String> range = translateRange(timeFilter);
        List<Object> vals;
        if (range == null) {
            vals = null;
        } else {
            vals = Arrays.asList(range.getLeft(), range.getRight());
        }
        return new TimeFilter(timeFilter.getRelation(), TimeFilter.Period.Date.name(), vals);
    }

    public Pair<String, String> translateRange(TimeFilter timeFilter) {
        String period = timeFilter.getPeriod();
        if (TimeFilter.Period.Date.name().equals(period)) {
            return translateDateFilter(timeFilter);
        }
        ComparisonType operator = timeFilter.getRelation();
        switch (operator) {
            case EVER:
                return null;
            case WITHIN:
                return translateWithIn(timeFilter.getPeriod(), timeFilter.getValues());
            case IN_CURRENT_PERIOD:
                return translateInCurrent(timeFilter.getPeriod());
            default:
                throw new UnsupportedOperationException("TimeFilter Operator " + operator + " is not supported.");
        }
    }

    @SuppressWarnings("unchecked")
    private Pair<String, String> translateDateFilter(TimeFilter timeFilter) {
        ComparisonType operator = timeFilter.getRelation();
        switch (operator) {
            case BETWEEN:
                List<Object> vals = timeFilter.getValues();
                if (CollectionUtils.isEmpty(vals) || vals.size() != 2) {
                    throw new IllegalArgumentException(ComparisonType.BETWEEN + //
                            " operator is only compatible with 2 values, but " + vals + " was provided.");
                }
                List<String> dates = vals.stream().map(val -> {
                    try {
                        return LocalDate.parse((String) val).format(DateTimeFormatter.ISO_DATE);
                    } catch (Exception e) {
                        throw new RuntimeException("Failed to parse value " + val + " to an ISO date.", e);
                    }
                }).collect(Collectors.toList());
                return Pair.of(dates.get(0), dates.get(1));
            default:
                throw new UnsupportedOperationException("Operator " + operator + " is not supported for date queries.");
        }
    }

    private Pair<String, String> translateWithIn(String period, List<Object> vals) {
        if (CollectionUtils.isEmpty(vals) || vals.size() != 1) {
            throw new IllegalArgumentException(ComparisonType.WITHIN + //
                    " operator is only compatible with single value, but " + vals + " was provided.");
        }
        Object val = vals.get(0);
        Integer offset;
        if (val instanceof Integer) {
            offset = (Integer) val;
        } else if (val instanceof String) {
            offset = Integer.valueOf((String) val);
        } else {
            try {
                offset = Integer.valueOf(String.valueOf(val));
            } catch (Exception e) {
                throw new IllegalArgumentException("Cannot cast value " + val + " to an Integer.", e);
            }
        }

        if (!periodBuilders.containsKey(period)) {
            throw new RuntimeException("Cannot find a period builder for period " + period);
        }

        if (!maxPeriodIds.containsKey(period)) {
            throw new RuntimeException("Cannot determine max period id for period " + period);
        }

        int maxPeriodId = maxPeriodIds.get(period);
        int targetPeriod = maxPeriodId - offset;

        PeriodBuilder builder = periodBuilders.get(period);
        Pair<LocalDate, LocalDate> dateRange = builder.toDateRange(targetPeriod, maxPeriodId - 1);
        String start = dateRange.getLeft().format(DateTimeFormatter.ISO_DATE);
        String end = dateRange.getRight().format(DateTimeFormatter.ISO_DATE);
        return Pair.of(start, end);
    }

    private Pair<String, String> translateInCurrent(String period) {
        if (!periodBuilders.containsKey(period)) {
            throw new RuntimeException("Cannot find a period builder for period " + period);
        }

        if (!maxPeriodIds.containsKey(period)) {
            throw new RuntimeException("Cannot determine max period id for period " + period);
        }

        int maxPeriodId = maxPeriodIds.get(period);

        PeriodBuilder builder = periodBuilders.get(period);
        Pair<LocalDate, LocalDate> dateRange = builder.toDateRange(maxPeriodId, maxPeriodId);
        String start = dateRange.getLeft().format(DateTimeFormatter.ISO_DATE);
        String end = dateRange.getRight().format(DateTimeFormatter.ISO_DATE);
        return Pair.of(start, end);
    }

}
