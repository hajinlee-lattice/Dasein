package com.latticeengines.query.exposed.translator;

import java.util.Arrays;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import com.latticeengines.domain.exposed.cdl.PeriodStrategy;
import com.latticeengines.domain.exposed.query.AttributeLookup;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.domain.exposed.query.ComparisonType;
import com.latticeengines.domain.exposed.query.DateRestriction;
import com.latticeengines.domain.exposed.query.Restriction;
import com.latticeengines.domain.exposed.query.TimeFilter;

public class DayRangeTranslatorUnitTestNG {

    private static final Logger log = LoggerFactory.getLogger(DayRangeTranslatorUnitTestNG.class);

    @Test(groups = "unit", dataProvider = "dateRestrictionProvider")
    public void testConvert(DateRestriction dateRestriction) {
        Restriction restriction = DayRangeTranslator.convert(dateRestriction);
        Assert.assertNotNull(restriction);
        log.info(restriction.toString());
    }

    @DataProvider(name = "dateRestrictionProvider")
    public Object[] provideDateRestrictions() {
        List<Object> betweenVals = Arrays.asList("2018-02-11", "2018-02-17");
        TimeFilter between = new TimeFilter(ComparisonType.BETWEEN, PeriodStrategy.Template.Date.name(), betweenVals);
        List<Object> priorVals = Arrays.asList(null, "2018-02-17");
        TimeFilter prior = new TimeFilter(ComparisonType.BETWEEN, PeriodStrategy.Template.Date.name(), priorVals);
        List<Object> afterVals = Arrays.asList(null, "2018-02-17");
        TimeFilter after = new TimeFilter(ComparisonType.BETWEEN, PeriodStrategy.Template.Date.name(), afterVals);
        return new Object[] { //
                new DateRestriction(provideAttributeLookup(), TimeFilter.ever()), //
                new DateRestriction(provideAttributeLookup(), TimeFilter.isEmpty()), //
                new DateRestriction(provideAttributeLookup(), between), //
                new DateRestriction(provideAttributeLookup(), prior), //
                new DateRestriction(provideAttributeLookup(), after) //
        };
    }

    private AttributeLookup provideAttributeLookup() {
        AttributeLookup attr = new AttributeLookup();
        attr.setAttribute("A");
        attr.setEntity(BusinessEntity.Account);
        return attr;
    }

    @Test(groups = "unit")
    public void testToMilliSecond() {
        Long startOfDay = DayRangeTranslator.getStartOfDayByDate("2018-02-17");
        Long endOfDay = DayRangeTranslator.getEndOfDayByDate("2018-02-17");
        Assert.assertEquals(startOfDay.longValue(), 1518825600000L);
        Assert.assertEquals(endOfDay.longValue(), 1518911999000L);

        Long timestamp = 1518825700000L;
        startOfDay = DayRangeTranslator.getStartOfDayByTimestamp(timestamp);
        endOfDay = DayRangeTranslator.getEndOfDayByTimestamp(timestamp);
        Assert.assertEquals(startOfDay.longValue(), 1518825600000L);
        Assert.assertEquals(endOfDay.longValue(), 1518911999000L);
    }

}
