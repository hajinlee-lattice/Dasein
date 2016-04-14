package com.latticeengines.propdata.collection.dao.impl;

import com.latticeengines.db.exposed.dao.impl.BaseDaoWithAssignedSessionFactoryImpl;
import com.latticeengines.domain.exposed.propdata.manage.Publication;
import com.latticeengines.propdata.collection.dao.PublicationDao;

public class PublicationDaoImpl extends BaseDaoWithAssignedSessionFactoryImpl<Publication>
        implements PublicationDao {

    @Override
    protected Class<Publication> getEntityClass() {
        return Publication.class;
    }

}
