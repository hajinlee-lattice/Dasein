package com.latticeengines.pls.dao.impl;

import com.latticeengines.db.exposed.dao.impl.BaseDaoImpl;
import com.latticeengines.domain.exposed.pls.EnrichmentAttribute;
import com.latticeengines.pls.dao.EnrichmentAttrDao;
import org.springframework.stereotype.Component;

@Component("enrichmentAttrDao")
public class EnrichmentAttrDaoImpl extends BaseDaoImpl<EnrichmentAttribute> implements EnrichmentAttrDao {

    @Override
    protected Class<EnrichmentAttribute> getEntityClass() {
        return EnrichmentAttribute.class;
    }
}
