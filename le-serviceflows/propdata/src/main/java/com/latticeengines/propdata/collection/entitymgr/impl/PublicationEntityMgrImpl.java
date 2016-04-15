package com.latticeengines.propdata.collection.entitymgr.impl;

import java.util.List;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Isolation;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import com.latticeengines.domain.exposed.propdata.manage.Publication;
import com.latticeengines.propdata.collection.dao.PublicationDao;
import com.latticeengines.propdata.collection.entitymgr.PublicationEntityMgr;

@Component("publicationEntityMgr")
public class PublicationEntityMgrImpl implements PublicationEntityMgr {

    @Autowired
    private PublicationDao dao;

    @Override
    @Transactional(value = "propDataManage", readOnly = true, isolation = Isolation.REPEATABLE_READ)
    public Publication findByPublicationName(String publicationName) {
        return dao.findByField("PublicationName", publicationName);
    }

    @Override
    @Transactional(value = "propDataManage")
    public Publication addPublication(Publication publication) {
        dao.create(publication);
        return dao.findByField("PublicationName", publication.getPublicationName());
    }

    @Override
    @Transactional(value = "propDataManage", propagation = Propagation.REQUIRED)
    public void removePublication(String publicationName) {
        Publication publication = dao.findByField("PublicationName", publicationName);
        if (publication != null) {
            dao.delete(publication);
        }
    }

    @Override
    @Transactional(value = "propDataManage", readOnly = true, isolation = Isolation.READ_UNCOMMITTED)
    public List<Publication> findAll() {
        return dao.findAll();
    }

}
