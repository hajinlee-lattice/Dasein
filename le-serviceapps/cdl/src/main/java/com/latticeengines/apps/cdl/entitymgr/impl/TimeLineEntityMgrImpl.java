package com.latticeengines.apps.cdl.entitymgr.impl;

import java.util.List;

import javax.inject.Inject;

import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import com.latticeengines.apps.cdl.entitymgr.TimeLineEntityMgr;
import com.latticeengines.apps.cdl.repository.jpa.writer.TimeLineWriterRepository;
import com.latticeengines.apps.cdl.repository.reader.TimeLineReaderRepository;
import com.latticeengines.db.exposed.entitymgr.impl.JpaEntityMgrRepositoryImpl;
import com.latticeengines.db.exposed.repository.BaseJpaRepository;
import com.latticeengines.db.exposed.util.MultiTenantContext;
import com.latticeengines.domain.exposed.cdl.activity.TimeLine;
import com.latticeengines.domain.exposed.security.Tenant;

@Component("timeLineEntityMgr")
public class TimeLineEntityMgrImpl extends JpaEntityMgrRepositoryImpl<TimeLine, Long> implements TimeLineEntityMgr {

    @Inject
    private TimeLineReaderRepository readerRepository;

    @Inject
    private TimeLineWriterRepository writerRepository;

    @Override
    @Transactional(transactionManager = "transactionManager", propagation = Propagation.REQUIRES_NEW, readOnly = true)
    public TimeLine findByPid(Long pid) {
        return readerRepository.findByPid(pid);
    }

    @Override
    @Transactional(transactionManager = "transactionManager", propagation = Propagation.REQUIRES_NEW, readOnly = true)
    public TimeLine findByTimeLineId(String timelineId) {
        Tenant tenant = MultiTenantContext.getTenant();
        return readerRepository.findByTenantAndTimelineId(tenant, timelineId);
    }

    @Override
    @Transactional(transactionManager = "transactionManager", propagation = Propagation.REQUIRES_NEW, readOnly = true)
    public TimeLine findByEntity(String entity) {
        Tenant tenant = MultiTenantContext.getTenant();
        return readerRepository.findByTenantAndEntity(tenant, entity);
    }

    @Override
    @Transactional(transactionManager = "transactionManager", propagation = Propagation.REQUIRES_NEW, readOnly = true)
    public List<TimeLine> findByTenant(Tenant tenant) {
        return readerRepository.findByTenant(tenant);
    }

    @Override
    public BaseJpaRepository<TimeLine, Long> getRepository() {
        return writerRepository;
    }
}
