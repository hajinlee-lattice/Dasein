package com.latticeengines.apps.dcp.entitymgr.impl;

import javax.annotation.Resource;
import javax.inject.Inject;

import org.springframework.stereotype.Component;

import com.latticeengines.apps.dcp.dao.EnrichmentLayoutDao;
import com.latticeengines.apps.dcp.entitymgr.EnrichmentLayoutEntityMgr;
import com.latticeengines.apps.dcp.repository.EnrichmentLayoutRepository;
import com.latticeengines.db.exposed.dao.BaseDao;
import com.latticeengines.db.exposed.entitymgr.impl.BaseReadWriteRepoEntityMgrImpl;
import com.latticeengines.domain.exposed.dcp.EnrichmentLayout;

@Component("enrichmentLayoutEntityMgr")
public class EnrichmentLayoutEntityMgrImpl
        extends BaseReadWriteRepoEntityMgrImpl<EnrichmentLayoutRepository, EnrichmentLayout, Long>
        implements EnrichmentLayoutEntityMgr {

    @Inject
    private EnrichmentLayoutEntityMgrImpl _self;

    @Inject
    private EnrichmentLayoutDao enrichmentLayoutDao;

    @Resource(name = "enrichmentLayoutReaderRepository")
    private EnrichmentLayoutRepository enrichmentLayoutReaderRepository;

    @Resource(name = "enrichmentLayoutWriterRepository")
    private EnrichmentLayoutRepository enrichmentLayoutWriterRepository;

    @Override
    protected BaseReadWriteRepoEntityMgrImpl<EnrichmentLayoutRepository, EnrichmentLayout, Long> getSelf() {
        return _self;
    }

    @Override
    protected EnrichmentLayoutRepository getReaderRepo() {
        return enrichmentLayoutReaderRepository;
    }

    @Override
    protected EnrichmentLayoutRepository getWriterRepo() {
        return enrichmentLayoutWriterRepository;
    }

    @Override
    public BaseDao<EnrichmentLayout> getDao() {
        return enrichmentLayoutDao;
    }

}
