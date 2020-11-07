package com.latticeengines.domain.exposed.util;

import java.io.Serializable;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.tuple.Pair;

import com.google.common.collect.ImmutableMap;
import com.latticeengines.common.exposed.util.DateTimeUtils;
import com.latticeengines.domain.exposed.cdl.PeriodBuilderFactory;
import com.latticeengines.domain.exposed.cdl.PeriodStrategy;
import com.latticeengines.domain.exposed.period.PeriodBuilder;
import com.latticeengines.domain.exposed.query.AttributeLookup;
import com.latticeengines.domain.exposed.query.ComparisonType;
import com.latticeengines.domain.exposed.query.TimeFilter;

public class TimeFilterTranslator implements Serializable {

    private static final long serialVersionUID = 0L;

    private final ImmutableMap<String, PeriodBuilder> periodBuilders;
    private final ImmutableMap<String, Integer> currentPeriodIds;
    private final String evaluationDate;
    private ConcurrentHashMap<ComparisonType, Map<AttributeLookup, List<Object>>> specifiedValues;

    public TimeFilterTranslator(List<PeriodStrategy> strategyList, String evaluationDate) {
        Map<String, Integer> currentPeriodMap = new HashMap<>();
        if (CollectionUtils.isNotEmpty(strategyList)) {
            Map<String, PeriodBuilder> builderMap = new HashMap<>();
            strategyList.forEach(strategy -> {
                PeriodBuilder builder = PeriodBuilderFactory.build(strategy);
                builderMap.put(strategy.getName(), builder);
                currentPeriodMap.put(strategy.getName(), builder.toPeriodId(evaluationDate));
            });
            periodBuilders = ImmutableMap.copyOf(builderMap);
        } else {
            periodBuilders = ImmutableMap.copyOf(Collections.emptyMap());
        }
        this.currentPeriodIds = ImmutableMap.copyOf(currentPeriodMap);
        this.evaluationDate = evaluationDate;
        this.specifiedValues = new ConcurrentHashMap<>();
        for (ComparisonType type : ComparisonType.VAGUE_TYPES) {
            specifiedValues.put(type, new HashMap<>());
        }
    }

    public ConcurrentHashMap<ComparisonType, Map<AttributeLookup, List<Object>>> getSpecifiedValues() {
        return this.specifiedValues;
    }

    public TimeFilter translate(TimeFilter timeFilter, AttributeLookup lookup) {
        Pair<String, String> range = periodIdRangeToDateRange(timeFilter.getPeriod(), translateRange(timeFilter));
        List<Object> vals;
        if (range == null && ComparisonType.EVER.equals(timeFilter.getRelation())) {
            return new TimeFilter(ComparisonType.EVER, timeFilter.getPeriod(), null);
        } else if (range == null && ComparisonType.IS_EMPTY.equals(timeFilter.getRelation())) {
            return new TimeFilter(ComparisonType.IS_EMPTY, null, null);
        } else if (range == null && ComparisonType.LATEST_DAY.equals(timeFilter.getRelation())) {
            if (lookup == null) {
                throw new NullPointerException("lookup cannot be null for LASTEST_DAY comparator");
            }
            Map<AttributeLookup, List<Object>> map = specifiedValues.get(ComparisonType.LATEST_DAY);
            if (!map.containsKey(lookup)) {
                throw new NullPointerException("TimeFilterTranslator does not have the lookup: " + lookup);
            }
            return new TimeFilter(ComparisonType.BETWEEN, PeriodStrategy.Template.Date.name(), map.get(lookup));
        } else {
            vals = Arrays.asList(range.getLeft(), range.getRight());
            return new TimeFilter(ComparisonType.BETWEEN, PeriodStrategy.Template.Date.name(), vals);
        }
    }

    public TimeFilter translate(TimeFilter timeFilter) {
        return this.translate(timeFilter, null);
    }

    public Pair<String, String> periodIdRangeToDateRange(String period, Pair<Integer, Integer> periodIdRange) {
        if (periodIdRange == null) {
            return null;
        }
        if (PeriodStrategy.Template.Date.name().equals(period) || PeriodStrategy.Template.Day.name().equals(period)) {
            return Pair.of(DateTimeUtils.dayPeriodToDate(periodIdRange.getLeft()), DateTimeUtils.dayPeriodToDate(periodIdRange.getRight()));
        }

        PeriodBuilder builder = periodBuilders.get(period);
        Pair<LocalDate, LocalDate> dateRange;
        String start = null, end = null;
        if (periodIdRange.getLeft() == null) {
            dateRange = builder.toDateRange(periodIdRange.getRight(), periodIdRange.getRight());
            end = dateRange.getRight().format(DateTimeFormatter.ISO_DATE);
        } else if (periodIdRange.getRight() == null) {
            dateRange = builder.toDateRange(periodIdRange.getLeft(), periodIdRange.getRight());
            start = dateRange.getLeft().format(DateTimeFormatter.ISO_DATE);
        } else {
            dateRange = builder.toDateRange(periodIdRange.getLeft(), periodIdRange.getRight());
            start = dateRange.getLeft().format(DateTimeFormatter.ISO_DATE);
            end = dateRange.getRight().format(DateTimeFormatter.ISO_DATE);
        }
        return Pair.of(start, end);
    }

    public Pair<Integer, Integer> translateRange(TimeFilter timeFilter) {
        String period = timeFilter.getPeriod();
        if (PeriodStrategy.Template.Date.name().equals(period)) {
            return translateDateFilter(timeFilter);
        }
        ComparisonType operator = timeFilter.getRelation();
        switch (operator) {
        case EVER:
        case IS_EMPTY:
            return null;
        case WITHIN:
            return translateWithIn(timeFilter.getPeriod(), timeFilter.getValues());
        case WITHIN_INCLUDE:
            return translateWithInInclude(timeFilter.getPeriod(), timeFilter.getValues());
        case PRIOR:
            return translatePrior(timeFilter.getPeriod(), timeFilter.getValues());
        case LAST:
            return translateLast(timeFilter.getPeriod(), timeFilter.getValues());
        case LATEST_DAY:
            return null;
        case BETWEEN:
            return translateBetween(timeFilter.getPeriod(), timeFilter.getValues());
        case IN_CURRENT_PERIOD:
            return translateInCurrent(timeFilter.getPeriod());
        default:
            throw new UnsupportedOperationException("TimeFilter Operator " + operator + " is not supported.");
        }
    }

    public int dateToPeriod(String periodName, String date) {
        return periodBuilders.get(periodName).toPeriodId(date);
    }



    private Pair<Integer, Integer> translateDateFilter(TimeFilter timeFilter) {
        ComparisonType operator = timeFilter.getRelation();
        List<Object> vals = timeFilter.getValues();
        switch (operator) {
            case BETWEEN:
            case BETWEEN_DATE:
                verifyDoubleVals(operator, vals);
                List<String> dates = vals.stream().map(this::castToDate).collect(Collectors.toList());
                return Pair.of(DateTimeUtils.dateToDayPeriod(dates.get(0)), DateTimeUtils.dateToDayPeriod(dates.get(1)));
            case BEFORE:
                verifySingleVal(operator, vals);
                return Pair.of(null, DateTimeUtils.dateToDayPeriod(castToDate(vals.get(0))));
            case AFTER:
                verifySingleVal(operator, vals);
                return Pair.of(DateTimeUtils.dateToDayPeriod(castToDate(vals.get(0))), null);
            default:
                throw new UnsupportedOperationException("Operator " + operator + " is not supported for date queries.");
        }
    }

    private Pair<Integer, Integer> translateWithInInclude(String period, List<Object> vals) {
        verifyPeriodIsValid(period);
        int offset = parseSingleInteger(ComparisonType.WITHIN_INCLUDE, vals);

        int currentPeriodId = currentPeriodIds.get(period);
        int targetPeriod = currentPeriodId - offset;

        return Pair.of(targetPeriod, currentPeriodId);
    }

    private Pair<Integer, Integer> translateLast(String period, List<Object> vals) {
        if (!PeriodStrategy.Template.Day.name().equals(period)) {
            throw new UnsupportedOperationException(String.format("We do not support %s", period));
        }
        int lastDays = parseSingleInteger(ComparisonType.LAST, vals);
        if (lastDays < 1) {
            throw new IllegalArgumentException(
                    "Operand has to be larger than or equal to 1, but " + vals + " was provided.");
        }
        int currentPeriodId = PeriodStrategy.Template.Day.name().equals(period) ? DateTimeUtils.dateToDayPeriod(evaluationDate) : currentPeriodIds.get(period);
        int targetPeriodId = currentPeriodId;
        if (PeriodStrategy.Template.Day.name().equals(period)) {
            targetPeriodId = DateTimeUtils.subtractDays(currentPeriodId, lastDays - 1);
        }else{
            targetPeriodId = currentPeriodId - lastDays + 1;
        }

        return Pair.of(targetPeriodId, currentPeriodId);
    }

    private Pair<Integer, Integer> translateWithIn(String period, List<Object> vals) {
        verifyPeriodIsValid(period);
        int offset = parseSingleInteger(ComparisonType.WITHIN, vals);

        int currentPeriodId = currentPeriodIds.get(period);
        int targetPeriod = currentPeriodId - offset;

        return Pair.of(targetPeriod, currentPeriodId - 1);
    }

    private Pair<Integer, Integer> translatePrior(String period, List<Object> vals) {
        verifyPeriodIsValid(period);
        int offset = parseSingleInteger(ComparisonType.PRIOR_ONLY, vals);

        int currentPeriodId = currentPeriodIds.get(period);
        int targetPeriod = currentPeriodId - offset - 1;

        return Pair.of(null, targetPeriod);
    }

    private Pair<Integer, Integer> translateBetween(String period, List<Object> vals) {
        verifyPeriodIsValid(period);
        List<Integer> offsets = parseDoubleIntegers(ComparisonType.BETWEEN, vals);
        int offset1 = Math.max(offsets.get(0), offsets.get(1));
        int offset2 = Math.min(offsets.get(0), offsets.get(1));

        int currentPeriodId = currentPeriodIds.get(period);
        int fromPeriod = currentPeriodId - offset1;
        int toPeriod = currentPeriodId - offset2;

        return Pair.of(fromPeriod, toPeriod);
    }

    private void verifyPeriodIsValid(String period) {
        if (!periodBuilders.containsKey(period)) {
            throw new RuntimeException("Cannot find a period builder for period " + period);
        }

        if (!currentPeriodIds.containsKey(period)) {
            throw new RuntimeException("Cannot determine current period id for period " + period);
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

    private Pair<Integer, Integer> translateInCurrent(String period) {
        if (!periodBuilders.containsKey(period)) {
            throw new RuntimeException("Cannot find a period builder for period " + period);
        }

        if (!currentPeriodIds.containsKey(period)) {
            throw new RuntimeException("Cannot determine current period id for period " + period);
        }

        int currentPeriodId = currentPeriodIds.get(period);

        PeriodBuilder builder = periodBuilders.get(period);
        Pair<LocalDate, LocalDate> dateRange = builder.toDateRange(currentPeriodId, currentPeriodId);
        String start = dateRange.getLeft().format(DateTimeFormatter.ISO_DATE);
        String end = dateRange.getRight().format(DateTimeFormatter.ISO_DATE);
        return Pair.of(currentPeriodId, currentPeriodId);
    }

    private String getEvaluationDate() {
        return this.evaluationDate;
    }

}
