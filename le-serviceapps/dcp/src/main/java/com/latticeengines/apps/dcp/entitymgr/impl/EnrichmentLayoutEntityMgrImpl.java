package com.latticeengines.apps.dcp.entitymgr.impl;

import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

import javax.annotation.Resource;
import javax.inject.Inject;

import org.apache.commons.collections4.CollectionUtils;
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
import com.latticeengines.domain.exposed.dcp.EnrichmentLayoutDetail;

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
    public List<EnrichmentLayoutDetail> findAllEnrichmentLayoutDetail(Pageable pageable) {
        List<EnrichmentLayout> result = getReaderRepo().findAllEnrichmentLayouts(pageable);

        if (CollectionUtils.isEmpty(result)) {
            return Collections.emptyList();
        } else {
            return result.stream().map(EnrichmentLayoutDetail::new).collect(Collectors.toList());
        }
    }

    @Override
    @Transactional(propagation = Propagation.REQUIRES_NEW, readOnly = true)
    public EnrichmentLayoutDetail findEnrichmentLayoutDetailByLayoutId(String layoutId) {
        EnrichmentLayout enrichmentLayout = findByField("layoutId", layoutId);
        return (null != enrichmentLayout) ? new EnrichmentLayoutDetail(enrichmentLayout) : null;

    }

    @Override
    @Transactional(propagation = Propagation.REQUIRES_NEW, readOnly = true)
    public EnrichmentLayoutDetail findEnrichmentLayoutDetailBySourceId(String sourceId) {
        EnrichmentLayout enrichmentLayout = findByField("sourceId", sourceId);
        return (null != enrichmentLayout) ? new EnrichmentLayoutDetail(enrichmentLayout) : null;
    }
}
