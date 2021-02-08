package com.latticeengines.apps.dcp.entitymgr.impl;

import java.util.List;

import javax.annotation.Resource;
import javax.inject.Inject;

import org.springframework.data.domain.Pageable;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import com.latticeengines.apps.dcp.dao.EnrichmentTemplateDao;
import com.latticeengines.apps.dcp.entitymgr.EnrichmentTemplateEntityMgr;
import com.latticeengines.apps.dcp.repository.EnrichmentTemplateRepository;
import com.latticeengines.db.exposed.dao.BaseDao;
import com.latticeengines.db.exposed.entitymgr.impl.BaseReadWriteRepoEntityMgrImpl;
import com.latticeengines.domain.exposed.dcp.EnrichmentTemplate;

@Component("enrichmentTemplateEntityMgr")
public class EnrichmentTemplateEntityMgrImpl
        extends BaseReadWriteRepoEntityMgrImpl<EnrichmentTemplateRepository, EnrichmentTemplate, Long>
        implements EnrichmentTemplateEntityMgr {

    @Inject
    private EnrichmentTemplateEntityMgrImpl _self;

    @Inject
    private EnrichmentTemplateDao enrichmentTemplateDao;

    @Resource(name = "enrichmentTemplateReaderRepository")
    private EnrichmentTemplateRepository enrichmentTemplateReaderRepository;

    @Resource(name = "enrichmentTemplateWriterRepository")
    private EnrichmentTemplateRepository enrichmentTemplateWriterRepository;

    @Override
    protected BaseReadWriteRepoEntityMgrImpl<EnrichmentTemplateRepository, EnrichmentTemplate, Long> getSelf() {
        return _self;
    }

    @Override
    protected EnrichmentTemplateRepository getReaderRepo() {
        return enrichmentTemplateReaderRepository;
    }

    @Override
    protected EnrichmentTemplateRepository getWriterRepo() {
        return enrichmentTemplateWriterRepository;
    }

    @Override
    public BaseDao<EnrichmentTemplate> getDao() {
        return enrichmentTemplateDao;
    }

    @Override
    @Transactional(propagation = Propagation.REQUIRES_NEW, readOnly = true)
    public List<EnrichmentTemplate> findAll(Pageable pageable) {
        return getReadOrWriteRepository().findAllEnrichmentTemplates(pageable);
    }

    @Override
    @Transactional(propagation = Propagation.REQUIRES_NEW, readOnly = true)
    public EnrichmentTemplate find(String templateId) {
        return getReadOrWriteRepository().findByTemplateId(templateId);
    }
}
