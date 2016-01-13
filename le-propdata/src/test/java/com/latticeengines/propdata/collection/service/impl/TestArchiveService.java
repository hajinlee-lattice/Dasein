package com.latticeengines.propdata.collection.service.impl;

import java.util.Date;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.latticeengines.propdata.collection.entitymanager.ArchiveProgressEntityMgr;
import com.latticeengines.propdata.collection.service.CollectedArchiveService;
import com.latticeengines.propdata.core.source.CollectedSource;
import com.latticeengines.propdata.core.source.impl.TestCollectedSource;

@Component("testArchiveService")
public class TestArchiveService extends AbstractCollectionArchiveService implements CollectedArchiveService {

    Log log = LogFactory.getLog(this.getClass());

    @Autowired
    ArchiveProgressEntityMgr progressEntityMgr;

    @Autowired
    TestCollectedSource source;

    @Override
    public CollectedSource getSource() { return source; }

    @Override
    ArchiveProgressEntityMgr getProgressEntityMgr() { return progressEntityMgr; }

    @Override
    Log getLogger() { return log; }

    @Override
    protected Date getLatestTimestampArchived() {
        return new Date();
    }

}
