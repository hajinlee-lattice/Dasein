package com.latticeengines.propdata.collection.service.impl;

import java.util.Date;
import java.util.concurrent.TimeUnit;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.dao.EmptyResultDataAccessException;
import org.springframework.stereotype.Component;

import com.latticeengines.propdata.collection.entitymanager.ArchiveProgressEntityMgr;
import com.latticeengines.propdata.collection.service.CollectedArchiveService;
import com.latticeengines.propdata.collection.source.CollectedSource;
import com.latticeengines.propdata.collection.source.impl.BuiltWith;

@Component("builtWithArchiveService")
public class BuiltWithArchiveService extends AbstractCollectionArchiveService implements CollectedArchiveService {

    Log log = LogFactory.getLog(this.getClass());

    @Autowired
    ArchiveProgressEntityMgr progressEntityMgr;

    @Autowired
    BuiltWith source;

    @Override
    public CollectedSource getSource() { return source; }

    @Override
    ArchiveProgressEntityMgr getProgressEntityMgr() { return progressEntityMgr; }

    @Override
    Log getLogger() { return log; }

    @Override
    protected Date getLatestTimestampArchived() {
        Date latestInDest;
        try {
            latestInDest = jdbcTemplateCollectionDB.queryForObject(
                    "SELECT MAX([LE_Last_Upload_Dat]) FROM [Feature_MostRecent]", Date.class);
        } catch (EmptyResultDataAccessException e) {
            latestInDest = new Date(System.currentTimeMillis() - TimeUnit.DAYS.toMillis(3650));
        }
        return latestInDest;
    }
}
