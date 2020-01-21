package com.latticeengines.datacloud.collection.service.impl;

import java.util.Calendar;
import java.util.Date;

import javax.inject.Inject;

import org.springframework.stereotype.Component;

import com.latticeengines.datacloud.collection.entitymgr.ArchiveProgressEntityMgr;
import com.latticeengines.datacloud.collection.service.CollectedArchiveService;
import com.latticeengines.datacloud.core.source.CollectedSource;
import com.latticeengines.datacloud.core.source.impl.OrbIntelligence;

@Component
public class OrbIntelligenceArchiveServiceImplTestNG extends CollectionArchiveServiceImplTestNGBase {

    @Inject
    OrbIntelligenceArchiveService collectedArchiveService;

    @Inject
    OrbIntelligence source;

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

        calendar.set(2016, Calendar.MARCH, 1);
        dates[0] = calendar.getTime();

        calendar.set(2016, Calendar.APRIL, 1);
        dates[1] = calendar.getTime();

        calendar.set(2016, Calendar.MAY, 1);
        dates[2] = calendar.getTime();

        return dates;
    }

    @Override
    CollectedSource getSource() {
        return source;
    }

}
