package com.latticeengines.apps.cdl.dao.impl;

import org.springframework.stereotype.Component;

import com.latticeengines.apps.cdl.dao.CatalogDao;
import com.latticeengines.db.exposed.dao.impl.BaseDaoImpl;
import com.latticeengines.domain.exposed.cdl.activity.Catalog;

@Component("catalogDao")
public class CatalogDaoImpl extends BaseDaoImpl<Catalog> implements CatalogDao {

    @Override
    protected Class<Catalog> getEntityClass() {
        return Catalog.class;
    }
}
