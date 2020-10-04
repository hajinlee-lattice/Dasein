package com.latticeengines.apps.dcp.entitymgr.impl;

import java.util.List;

import javax.annotation.Resource;
import javax.inject.Inject;

import org.springframework.data.domain.Pageable;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

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

    @Override
    @Transactional(propagation = Propagation.REQUIRES_NEW, readOnly = true)
    public List<EnrichmentLayout> findAll(Pageable pageable) {
        return getReadOrWriteRepository().findAllEnrichmentLayouts(pageable);
    }

    @Override
    @Transactional(propagation = Propagation.REQUIRES_NEW, readOnly = true)
    public EnrichmentLayout findByLayoutId(String layoutId) {
        return getReadOrWriteRepository().findByLayoutId(layoutId);
    }

    @Override
    @Transactional(propagation = Propagation.REQUIRES_NEW, readOnly = true)
    public EnrichmentLayout findBySourceId(String sourceId) {
        return getReadOrWriteRepository().findBySourceId(sourceId);
    }

}
