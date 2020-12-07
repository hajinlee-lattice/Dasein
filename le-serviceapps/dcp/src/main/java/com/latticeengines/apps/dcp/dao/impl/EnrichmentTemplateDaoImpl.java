package com.latticeengines.apps.dcp.dao.impl;

import org.springframework.stereotype.Component;

import com.latticeengines.apps.dcp.dao.EnrichmentTemplateDao;
import com.latticeengines.db.exposed.dao.impl.BaseDaoImpl;
import com.latticeengines.domain.exposed.dcp.EnrichmentTemplate;

@Component("enrichmentTemplateDao")
public class EnrichmentTemplateDaoImpl extends BaseDaoImpl<EnrichmentTemplate> implements EnrichmentTemplateDao {

    @Override
    protected Class<EnrichmentTemplate> getEntityClass() {
        return EnrichmentTemplate.class;
    }
}
