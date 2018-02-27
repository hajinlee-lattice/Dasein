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
            case PRIOR_ONLY:
                return translatePrior(timeFilter.getPeriod(), timeFilter.getValues());
            case BETWEEN:
                return translateBetween(timeFilter.getPeriod(), timeFilter.getValues());
            case IN_CURRENT_PERIOD:
                return translateInCurrent(timeFilter.getPeriod());
            default:
                throw new UnsupportedOperationException("TimeFilter Operator " + operator + " is not supported.");
        }
    }

    @SuppressWarnings("unchecked")
    private Pair<String, String> translateDateFilter(TimeFilter timeFilter) {
        ComparisonType operator = timeFilter.getRelation();
        List<Object> vals = timeFilter.getValues();
        switch (operator) {
            case BETWEEN:
                verifyDoubleVals(operator, vals);
                List<String> dates = vals.stream().map(this::castToDate).collect(Collectors.toList());
                return Pair.of(dates.get(0), dates.get(1));
            case BEFORE:
                verifySingleVal(operator, vals);
                return Pair.of(null, castToDate(vals.get(0)));
            case AFTER:
                verifySingleVal(operator, vals);
                return Pair.of(castToDate(vals.get(0)), null);
            default:
                throw new UnsupportedOperationException("Operator " + operator + " is not supported for date queries.");
        }
    }

    private Pair<String, String> translateWithIn(String period, List<Object> vals) {
        verifyPeriodIsValid(period);
        int offset = parseSingleInteger(ComparisonType.WITHIN, vals);

        int maxPeriodId = maxPeriodIds.get(period);
        int targetPeriod = maxPeriodId - offset;

        PeriodBuilder builder = periodBuilders.get(period);
        Pair<LocalDate, LocalDate> dateRange = builder.toDateRange(targetPeriod, maxPeriodId - 1);
        String start = dateRange.getLeft().format(DateTimeFormatter.ISO_DATE);
        String end = dateRange.getRight().format(DateTimeFormatter.ISO_DATE);
        return Pair.of(start, end);
    }

    private Pair<String, String> translatePrior(String period, List<Object> vals) {
        verifyPeriodIsValid(period);
        int offset = parseSingleInteger(ComparisonType.PRIOR_ONLY, vals);

        int maxPeriodId = maxPeriodIds.get(period);
        int targetPeriod = maxPeriodId - offset - 1;

        PeriodBuilder builder = periodBuilders.get(period);
        Pair<LocalDate, LocalDate> dateRange = builder.toDateRange(targetPeriod, targetPeriod);
        String end = dateRange.getRight().format(DateTimeFormatter.ISO_DATE);
        return Pair.of(null, end);
    }

    private Pair<String, String> translateBetween(String period, List<Object> vals) {
        verifyPeriodIsValid(period);
        List<Integer> offsets = parseDoubleIntegers(ComparisonType.BETWEEN, vals);
        int offset1 = Math.max(offsets.get(0), offsets.get(1));
        int offset2 = Math.min(offsets.get(0), offsets.get(1));

        int maxPeriodId = maxPeriodIds.get(period);
        int fromPeriod = maxPeriodId - offset1;
        int toPeriod = maxPeriodId - offset2;

        PeriodBuilder builder = periodBuilders.get(period);
        Pair<LocalDate, LocalDate> dateRange = builder.toDateRange(fromPeriod, toPeriod);
        String start = dateRange.getLeft().format(DateTimeFormatter.ISO_DATE);
        String end = dateRange.getRight().format(DateTimeFormatter.ISO_DATE);
        return Pair.of(start, end);
    }

    private void verifyPeriodIsValid(String period) {
        if (!periodBuilders.containsKey(period)) {
            throw new RuntimeException("Cannot find a period builder for period " + period);
        }

        if (!maxPeriodIds.containsKey(period)) {
            throw new RuntimeException("Cannot determine max period id for period " + period);
        }
    }

    private Integer parseSingleInteger(ComparisonType operator, List<Object> vals) {
        verifySingleVal(operator, vals);
        Object val = vals.get(0);
        return castToInteger(val);
    }

    private List<Integer> parseDoubleIntegers(ComparisonType operator, List<Object> vals) {
        verifyDoubleVals(operator, vals);
        Integer offset1 = castToInteger(vals.get(0));
        Integer offset2 = castToInteger(vals.get(1));
        return Arrays.asList(offset1, offset2);
    }

    private void verifySingleVal(ComparisonType operator, List<Object> vals) {
        if (CollectionUtils.isEmpty(vals) || vals.size() != 1) {
            throw new IllegalArgumentException(operator + //
                    " operator is only compatible with single value, but " + vals + " was provided.");
        }
    }

    private void verifyDoubleVals(ComparisonType operator, List<Object> vals) {
        if (CollectionUtils.isEmpty(vals) || vals.size() != 2) {
            throw new IllegalArgumentException(operator + //
                    " operator is only compatible with two values, but " + vals + " was provided.");
        }
    }

    private String castToDate(Object val) {
        try {
            return LocalDate.parse((String) val).format(DateTimeFormatter.ISO_DATE);
        } catch (Exception e) {
            throw new RuntimeException("Failed to parse value " + val + " to an ISO date.", e);
        }
    }

    private Integer castToInteger(Object val) {
        Integer integer;
        if (val instanceof Integer) {
            integer = (Integer) val;
        } else if (val instanceof String) {
            integer = Integer.valueOf((String) val);
        } else {
            try {
                integer = Integer.valueOf(String.valueOf(val));
            } catch (Exception e) {
                throw new IllegalArgumentException("Cannot cast value " + val + " to an Integer.", e);
            }
        }
        return integer;
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
