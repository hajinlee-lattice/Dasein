package com.latticeengines.datacloud.collection.service.impl;

import java.util.Calendar;
import java.util.Date;

import javax.inject.Inject;

import org.springframework.stereotype.Component;
import org.testng.Assert;

import com.latticeengines.datacloud.collection.entitymgr.ArchiveProgressEntityMgr;
import com.latticeengines.datacloud.collection.service.CollectedArchiveService;
import com.latticeengines.datacloud.core.source.CollectedSource;
import com.latticeengines.datacloud.core.source.impl.Feature;
import com.latticeengines.datacloud.core.util.DateRange;

@Component
public class FeatureArchiveServiceImplTestNG extends CollectionArchiveServiceImplTestNGBase {

    @Inject
    FeatureArchiveService collectedArchiveService;

    @Inject
    Feature source;

    @Inject
    ArchiveProgressEntityMgr progressEntityMgr;

    CollectedArchiveService getCollectedArchiveService() {
        return collectedArchiveService;
    }

    @Override
    ArchiveProgressEntityMgr getProgressEntityMgr() {
        return progressEntityMgr;
    }

    @Override
    Date[] getDates() {
        Date[] dates = new Date[3];

        calendar.set(2015, Calendar.NOVEMBER, 1);
        dates[0] = calendar.getTime();

        calendar.set(2015, Calendar.DECEMBER, 1);
        dates[1] = calendar.getTime();

        calendar.set(2016, Calendar.JANUARY, 1);
        dates[2] = calendar.getTime();

        return dates;
    }

    @Override
    CollectedSource getSource() {
        return source;
    }

    // not every kind of progress need this, we only need to test this on one
    // kind
    @Override
    protected void testAutoDetermineDateRange() {
        DateRange range = collectedArchiveService.determineNewJobDateRange();
        System.out.println(range);

        Date cutDate = dates[1];
        Assert.assertTrue(range.getStartDate().before(cutDate),
                "the auto determined range should start before " + cutDate + ". But it is " + range.getStartDate());
        Assert.assertTrue(range.getEndDate().after(cutDate),
                "the auto determined range should end after " + cutDate + ". But it is " + range.getStartDate());
    }

}
