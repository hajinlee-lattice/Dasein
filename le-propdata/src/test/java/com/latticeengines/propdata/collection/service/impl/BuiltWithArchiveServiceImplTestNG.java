package com.latticeengines.propdata.collection.service.impl;

import java.util.Calendar;
import java.util.Date;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.latticeengines.propdata.collection.entitymanager.ArchiveProgressEntityMgr;
import com.latticeengines.propdata.collection.service.CollectedArchiveService;
import com.latticeengines.propdata.core.source.CollectedSource;
import com.latticeengines.propdata.core.source.impl.BuiltWith;

@Component
public class BuiltWithArchiveServiceImplTestNG extends CollectionArchiveServiceImplTestNGBase {

    @Autowired
    BuiltWithArchiveService collectedArchiveService;

    @Autowired
    BuiltWith source;

    @Autowired
    ArchiveProgressEntityMgr progressEntityMgr;

    CollectedArchiveService getCollectedArchiveService() {
        return collectedArchiveService;
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
    CollectedSource getSource() { return source; }

}
