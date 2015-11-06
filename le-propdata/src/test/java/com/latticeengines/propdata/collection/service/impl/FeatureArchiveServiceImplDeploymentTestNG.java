package com.latticeengines.propdata.collection.service.impl;

import java.util.Calendar;
import java.util.Date;

import org.springframework.beans.factory.annotation.Autowired;

import com.latticeengines.domain.exposed.propdata.collection.FeatureArchiveProgress;
import com.latticeengines.propdata.collection.entitymanager.ArchiveProgressEntityMgr;
import com.latticeengines.propdata.collection.entitymanager.FeatureArchiveProgressEntityMgr;
import com.latticeengines.propdata.collection.service.FeatureArchiveService;

public class FeatureArchiveServiceImplDeploymentTestNG extends ArchiveServiceImplDeploymentTestNGBase {

    @Autowired
    FeatureArchiveService archiveService;

    @Autowired
    FeatureArchiveProgressEntityMgr progressEntityMgr;

    @Override
    FeatureArchiveService getArchiveService() {
        return archiveService;
    }

    @Override
    ArchiveProgressEntityMgr<FeatureArchiveProgress> getProgressEntityMgr() { return progressEntityMgr; }

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

    @Override
    Date[] getEmptyDataDates() {
        Date[] dates = new Date[2];

        calendar.set(2015, Calendar.OCTOBER, 22);
        dates[0] = calendar.getTime();

        calendar.set(2015, Calendar.NOVEMBER, 2);
        dates[1] = calendar.getTime();

        return dates;
    }

    @Override
    String destTableName() { return "Feature_MostRecent"; }

    @Override
    String sourceName() { return "Feature"; }

    @Override
    String[] uniqueColumns() { return new String[]{"URL", "Feature"}; }

}
