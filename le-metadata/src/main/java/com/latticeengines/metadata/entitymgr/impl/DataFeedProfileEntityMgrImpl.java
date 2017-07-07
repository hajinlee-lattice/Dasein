package com.latticeengines.metadata.entitymgr.impl;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.latticeengines.db.exposed.dao.BaseDao;
import com.latticeengines.db.exposed.entitymgr.impl.BaseEntityMgrImpl;
import com.latticeengines.domain.exposed.metadata.datafeed.DataFeedProfile;
import com.latticeengines.metadata.dao.DataFeedProfileDao;
import com.latticeengines.metadata.entitymgr.DataFeedProfileEntityMgr;

@Component("datafeedProfileEntityMgr")
public class DataFeedProfileEntityMgrImpl extends BaseEntityMgrImpl<DataFeedProfile>
        implements DataFeedProfileEntityMgr {

    @Autowired
    private DataFeedProfileDao datafeedProfileDao;

    @Override
    public BaseDao<DataFeedProfile> getDao() {
        return datafeedProfileDao;
    }

    @Override
    public DataFeedProfile findByProfileId(Long profileId) {
        if (profileId == null) {
            return null;
        }
        DataFeedProfile profile = findByField("pid", profileId);
        return profile;
    }

}
