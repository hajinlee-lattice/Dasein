package com.latticeengines.datacloud.etl.publication.entitymgr.impl;

import java.util.List;

import javax.inject.Inject;

import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Isolation;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import com.google.common.base.Preconditions;
import com.latticeengines.common.exposed.validator.annotation.NotNull;
import com.latticeengines.datacloud.etl.publication.dao.PublicationDao;
import com.latticeengines.datacloud.etl.publication.entitymgr.PublicationEntityMgr;
import com.latticeengines.domain.exposed.datacloud.manage.Publication;

@Component("publicationEntityMgr")
public class PublicationEntityMgrImpl implements PublicationEntityMgr {

    @Inject
    private PublicationDao dao;

    @Override
    @Transactional(value = "propDataManage", readOnly = true, isolation = Isolation.REPEATABLE_READ)
    public Publication findByPublicationName(@NotNull String publicationName) {
        Preconditions.checkNotNull(publicationName);
        return dao.findByField("PublicationName", publicationName);
    }

    @Override
    @Transactional(value = "propDataManage")
    public Publication addPublication(@NotNull Publication publication) {
        Preconditions.checkNotNull(publication);
        dao.create(publication);
        return dao.findByField("PublicationName", publication.getPublicationName());
    }

    @Override
    @Transactional(value = "propDataManage", propagation = Propagation.REQUIRED)
    public void removePublication(@NotNull String publicationName) {
        Preconditions.checkNotNull(publicationName);
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
