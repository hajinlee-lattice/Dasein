package com.latticeengines.apps.cdl.entitymgr.impl;

import java.util.List;

import javax.inject.Inject;

import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import com.google.common.base.Preconditions;
import com.latticeengines.apps.cdl.entitymgr.SLATermEntityMgr;
import com.latticeengines.apps.cdl.repository.jpa.writer.SLATermWriterRepository;
import com.latticeengines.apps.cdl.repository.reader.SLATermReaderRepository;
import com.latticeengines.db.exposed.entitymgr.impl.JpaEntityMgrRepositoryImpl;
import com.latticeengines.db.exposed.repository.BaseJpaRepository;
import com.latticeengines.domain.exposed.cdl.sla.SLATerm;
import com.latticeengines.domain.exposed.security.Tenant;

@Component("SLATermEntityMgr")
public class SLATermEntityMgrImpl extends JpaEntityMgrRepositoryImpl<SLATerm, Long> implements SLATermEntityMgr {

    @Inject
    private SLATermReaderRepository readerRepository;
    @Inject
    private SLATermWriterRepository writerRepository;

    @Override
    @Transactional(transactionManager = "transactionManager", propagation = Propagation.REQUIRES_NEW, readOnly = true)
    public SLATerm findByPid(Long pid) {
        Preconditions.checkNotNull(pid, "Pid should not be null");
        return readerRepository.findByPid(pid);
    }

    @Override
    @Transactional(transactionManager = "transactionManager", propagation = Propagation.REQUIRES_NEW, readOnly = true)
    public SLATerm findByTermNameAndTenant(Tenant tenant, String termName) {
        Preconditions.checkNotNull(tenant, "Tenant should not be null");
        Preconditions.checkNotNull(termName, "termName should not be null");
        return readerRepository.findByTermNameAndTenant(tenant, termName);
    }

    @Override
    @Transactional(transactionManager = "transactionManager", propagation = Propagation.REQUIRES_NEW, readOnly = true)
    public List<SLATerm> findByTenant(Tenant tenant) {
        Preconditions.checkNotNull(tenant, "Tenant should not be null");
        return readerRepository.findByTenant(tenant);
    }

    @Override
    public BaseJpaRepository<SLATerm, Long> getRepository() {
        return writerRepository;
    }
}
