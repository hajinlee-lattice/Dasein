package com.latticeengines.query.exposed.translator;

import java.time.LocalDate;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.List;

import org.apache.commons.collections4.CollectionUtils;

import com.latticeengines.domain.exposed.cdl.PeriodStrategy;
import com.latticeengines.domain.exposed.query.AttributeLookup;
import com.latticeengines.domain.exposed.query.ComparisonType;
import com.latticeengines.domain.exposed.query.DateRestriction;
import com.latticeengines.domain.exposed.query.Restriction;
import com.latticeengines.domain.exposed.query.RestrictionBuilder;
import com.latticeengines.domain.exposed.query.TimeFilter;

public class DayRangeTranslator extends TranslatorCommon {
    public Restriction convert(DateRestriction dateRestriction) {
        TimeFilter timeFilter = dateRestriction.getTimeFilter();
        if (timeFilter == null) {
            throw new NullPointerException("TimeFilter cannot be null");
        }
        AttributeLookup attr = dateRestriction.getAttr();
        RestrictionBuilder builder = Restriction.builder();
        if (ComparisonType.EVER.equals(timeFilter.getRelation())) {
            builder = builder.let(attr).isNotNull();
        } else if (ComparisonType.IS_EMPTY.equals(timeFilter.getRelation())) {
            builder = builder.let(attr).isNull();
        } else {
            builder = convertTimeRange(attr, builder, timeFilter);
        }
        return builder.build();
    }

    private RestrictionBuilder convertTimeRange(AttributeLookup attr, RestrictionBuilder builder,
            TimeFilter timeFilter) {
        if (!PeriodStrategy.Template.Date.name().equals(timeFilter.getPeriod())) {
            throw new UnsupportedOperationException(
                    "Can only translate Date period, but " + timeFilter.getPeriod() + " was given.");
        }
        List<Object> vals = timeFilter.getValues();
        if (CollectionUtils.isNotEmpty(vals) && vals.size() == 2) {
            if (vals.get(0) == null && vals.get(1) != null) {
                builder = builder.let(attr).lte(getEndOfDayAsMillisecond(vals.get(1)));
            } else if (vals.get(0) != null && vals.get(1) == null) {
                builder = builder.let(attr).gte(getStartOfDayAsMillisecond(vals.get(0)));
            } else {
                builder = builder.and( //
                        Restriction.builder().let(attr).gte(getStartOfDayAsMillisecond(vals.get(0))).build(),
                        Restriction.builder().let(attr).lte(getEndOfDayAsMillisecond(vals.get(1))).build());
            }
        } else {
            throw new IllegalArgumentException("TimeFilter has to have two values");
        }
        return builder;
    }

    public Long getStartOfDayAsMillisecond(Object date) {
        LocalDate day = LocalDate.parse(date.toString(), DateTimeFormatter.ISO_DATE);
        ZoneId zoneId = ZoneId.of("UTC");
        return day.atStartOfDay(zoneId).toEpochSecond() * 1_000;
    }

    public Long getEndOfDayAsMillisecond(Object date) {
        LocalDate day = LocalDate.parse(date.toString(), DateTimeFormatter.ISO_DATE);
        ZoneId zoneId = ZoneId.of("UTC");
        return day.atStartOfDay(zoneId).plusHours(23).plusMinutes(59).plusSeconds(59).toEpochSecond() * 1_000;
    }
}
