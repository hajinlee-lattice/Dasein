package com.latticeengines.metadata.dao.impl;

import org.springframework.stereotype.Component;

import com.latticeengines.db.exposed.dao.impl.BaseDaoImpl;
import com.latticeengines.domain.exposed.metadata.datafeed.DataFeedProfile;
import com.latticeengines.metadata.dao.DataFeedProfileDao;

@Component("datafeedProfileDao")
public class DataFeedProfileDaoImpl extends BaseDaoImpl<DataFeedProfile> implements DataFeedProfileDao {

    @Override
    protected Class<DataFeedProfile> getEntityClass() {
        return DataFeedProfile.class;
    }

}
