package com.latticeengines.apps.dcp.dao.impl;

import org.springframework.stereotype.Component;

import com.latticeengines.apps.dcp.dao.EnrichmentLayoutDao;
import com.latticeengines.db.exposed.dao.impl.BaseDaoImpl;
import com.latticeengines.domain.exposed.dcp.EnrichmentLayout;

@Component("enrichmentLayoutDao")
public class EnrichmentLayoutDaoImpl extends BaseDaoImpl<EnrichmentLayout> implements EnrichmentLayoutDao {

    @Override
    protected Class<EnrichmentLayout> getEntityClass() {
        return EnrichmentLayout.class;
    }
}
