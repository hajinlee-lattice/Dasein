package com.latticeengines.propdata.collection.service.impl;

import java.util.Calendar;
import java.util.Date;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.latticeengines.propdata.collection.entitymgr.ArchiveProgressEntityMgr;
import com.latticeengines.propdata.collection.service.CollectedArchiveService;
import com.latticeengines.propdata.core.source.CollectedSource;
import com.latticeengines.propdata.core.source.impl.Alexa;

@Component
public class AlexaArchiveServiceImplTestNG extends CollectionArchiveServiceImplTestNGBase {

    @Autowired
    AlexaArchiveService collectedArchiveService;

    @Autowired
    Alexa source;

    @Autowired
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

        calendar.set(2015, Calendar.SEPTEMBER, 1);
        dates[0] = calendar.getTime();

        calendar.set(2015, Calendar.OCTOBER, 1);
        dates[1] = calendar.getTime();

        calendar.set(2015, Calendar.NOVEMBER, 1);
        dates[2] = calendar.getTime();

        return dates;
    }

    @Override
    CollectedSource getSource() {
        return source;
    }

}
