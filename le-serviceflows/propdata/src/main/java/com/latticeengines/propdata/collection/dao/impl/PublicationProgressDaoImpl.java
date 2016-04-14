package com.latticeengines.propdata.collection.dao.impl;

import com.latticeengines.db.exposed.dao.impl.BaseDaoWithAssignedSessionFactoryImpl;
import com.latticeengines.domain.exposed.propdata.manage.PublicationProgress;
import com.latticeengines.propdata.collection.dao.PublicationProgressDao;

public class PublicationProgressDaoImpl extends BaseDaoWithAssignedSessionFactoryImpl<PublicationProgress>
        implements PublicationProgressDao {

    @Override
    protected Class<PublicationProgress> getEntityClass() {
        return PublicationProgress.class;
    }

}
