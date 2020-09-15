package com.latticeengines.apps.dcp.service.impl;

import java.util.List;

import javax.inject.Inject;

import org.springframework.stereotype.Service;

import com.latticeengines.apps.dcp.entitymgr.EnrichmentLayoutEntityMgr;
import com.latticeengines.apps.dcp.service.EnrichmentLayoutService;
import com.latticeengines.domain.exposed.dcp.EnrichmentLayout;

@Service("EnrichmentLayoutService")
public class EnrichmentLayoutServiceImpl implements EnrichmentLayoutService {

    @Inject
    private EnrichmentLayoutEntityMgr enrichmentLayoutEntityMgr;

    @Override
    public void create(EnrichmentLayout enrichmentLayout) {
        if (validate(enrichmentLayout)) {
            enrichmentLayoutEntityMgr.create(enrichmentLayout);
        }
    }

    @Override
    public List<EnrichmentLayout> getAll() {
        return enrichmentLayoutEntityMgr.findAll();
    }

    @Override
    public boolean update(EnrichmentLayout enrichmentLayout) {
        if (validate(enrichmentLayout)) {
            enrichmentLayoutEntityMgr.update(enrichmentLayout);
            return true;
        }
        else {
            return false;
        }
    }

    @Override
    public EnrichmentLayout findByLayoutId(String layoutId) {
        return enrichmentLayoutEntityMgr.findByField("layoutId", layoutId);
    }

    @Override
    public EnrichmentLayout findBySourceId(String sourceId) {
        return enrichmentLayoutEntityMgr.findByField("sourceId", sourceId);
    }

    @Override
    public void delete(EnrichmentLayout enrichmentLayout) {
        enrichmentLayoutEntityMgr.delete(enrichmentLayout);
    }

    @Override
    public void delete(String layoutId) {
        EnrichmentLayout enrichmentLayout = findByLayoutId(layoutId);
        if (null != enrichmentLayout) {
            delete(enrichmentLayout);
        }
    }

    @Override
    public boolean validate(EnrichmentLayout enrichmentLayout) {
        /*
        validation fails if subscriber is not entitled to given elements for given domain and record type.
        Error should include specific elements that are cause validation to fail
         */
        return true;
    }
}
