package com.latticeengines.metadata.dao.impl;

import org.springframework.stereotype.Component;

import com.latticeengines.db.exposed.dao.impl.BaseDaoImpl;
import com.latticeengines.domain.exposed.metadata.DependencyLink;
import com.latticeengines.metadata.dao.DependencyLinkDao;

@Component("dependencyLinkDao")
public class DependencyLinkDaoImpl extends BaseDaoImpl<DependencyLink> implements DependencyLinkDao {
    @Override
    protected Class<DependencyLink> getEntityClass() {
        return DependencyLink.class;
    }
}
