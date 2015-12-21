package com.latticeengines.propdata.collection.service.impl;

import java.util.Calendar;
import java.util.Date;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Component;

import com.latticeengines.propdata.collection.entitymanager.ArchiveProgressEntityMgr;
import com.latticeengines.propdata.collection.service.ArchiveService;
import com.latticeengines.propdata.collection.source.CollectionSource;

@Component
public class BuiltWithArchiveServiceImplDeploymentTestNG extends ArchiveServiceImplDeploymentTestNGBase {

    @Autowired
    @Qualifier(value = "builtWithArchiveService")
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

        calendar.set(2013, Calendar.DECEMBER, 1);
        dates[0] = calendar.getTime();

        calendar.set(2015, Calendar.JANUARY, 1);
        dates[1] = calendar.getTime();

        calendar.set(2016, Calendar.FEBRUARY, 1);
        dates[2] = calendar.getTime();

        return dates;
    }

    @Override
    CollectionSource getSource() { return CollectionSource.BUILT_WITH; }

    @Override
    String[] uniqueColumns() { return new String[]{"Domain", "Technology_Name"}; }


}
