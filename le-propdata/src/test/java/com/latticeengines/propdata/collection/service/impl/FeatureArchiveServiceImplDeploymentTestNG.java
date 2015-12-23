package com.latticeengines.propdata.collection.service.impl;

import java.util.Calendar;
import java.util.Date;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Component;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.latticeengines.domain.exposed.propdata.collection.ArchiveProgress;
import com.latticeengines.propdata.collection.entitymanager.ArchiveProgressEntityMgr;
import com.latticeengines.propdata.collection.service.ArchiveService;
import com.latticeengines.propdata.collection.source.impl.CollectionSource;
import com.latticeengines.propdata.collection.util.DateRange;

@Component
public class FeatureArchiveServiceImplDeploymentTestNG extends ArchiveServiceImplDeploymentTestNGBase {

    @Autowired
    @Qualifier(value = "featureArchiveService")
    ArchiveService archiveService;

    @Autowired
    ArchiveProgressEntityMgr progressEntityMgr;

    @Override
    ArchiveService getArchiveService() {
        return archiveService;
    }

    @Override
    ArchiveProgressEntityMgr getProgressEntityMgr() { return progressEntityMgr; }

    @Override
    Date[] getDates() {
        Date[] dates = new Date[3];

        calendar.set(2013, Calendar.OCTOBER, 22);
        dates[0] = calendar.getTime();

        calendar.set(2013, Calendar.NOVEMBER, 2);
        dates[1] = calendar.getTime();

        calendar.set(2014, Calendar.JANUARY, 1);
        dates[2] = calendar.getTime();

        return dates;
    }

    @Test(groups = "deployment", dependsOnMethods = "testWholeProgress", enabled = false)
    public void testEmptyInput() {
        Date[] dates = getEmptyDataDates();

        ArchiveProgress progress = createNewProgress(dates[0], dates[1]);
        progress = importFromDB(progress);
        progress = transformRawData(progress);
        exportToDB(progress);

        cleanupProgressTables();
    }

    @Override
    CollectionSource getSource() { return CollectionSource.FEATURE; }

    @Override
    String[] uniqueColumns() { return new String[]{"URL", "Feature"}; }

    // not every kind of progress need this, we only need to test this on one kind
    @Override
    protected void testAutoDetermineDateRange() {
        DateRange range = archiveService.determineNewJobDateRange();
        System.out.println(range);

        Date cutDate = dates[1];
        Assert.assertTrue(range.getStartDate().before(cutDate),
                "the auto determined range should start before " + cutDate
                        + ". But it is " + range.getStartDate());
        Assert.assertTrue(range.getEndDate().after(cutDate),
                "the auto determined range should end after " + cutDate
                        + ". But it is " + range.getStartDate());
    }

    private Date[] getEmptyDataDates() {
        Date[] dates = new Date[2];

        calendar.set(2015, Calendar.OCTOBER, 22);
        dates[0] = calendar.getTime();

        calendar.set(2015, Calendar.NOVEMBER, 2);
        dates[1] = calendar.getTime();

        return dates;
    }



}
