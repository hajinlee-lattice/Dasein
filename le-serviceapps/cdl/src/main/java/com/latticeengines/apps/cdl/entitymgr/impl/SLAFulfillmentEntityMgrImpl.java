package com.latticeengines.apps.cdl.entitymgr.impl;

import java.util.List;

import javax.inject.Inject;

import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import com.google.common.base.Preconditions;
import com.latticeengines.apps.cdl.entitymgr.SLAFulfillmentEntityMgr;
import com.latticeengines.apps.cdl.repository.jpa.writer.SLAFulfillmentWriterRepository;
import com.latticeengines.apps.cdl.repository.reader.SLAFulfillmentReaderRepository;
import com.latticeengines.db.exposed.entitymgr.impl.JpaEntityMgrRepositoryImpl;
import com.latticeengines.db.exposed.repository.BaseJpaRepository;
import com.latticeengines.domain.exposed.cdl.sla.SLAFulfillment;
import com.latticeengines.domain.exposed.cdl.sla.SLATerm;
import com.latticeengines.domain.exposed.pls.Action;
import com.latticeengines.domain.exposed.security.Tenant;

@Component("SLAFulfillmentEntityMgr")
public class SLAFulfillmentEntityMgrImpl extends JpaEntityMgrRepositoryImpl<SLAFulfillment, Long> implements SLAFulfillmentEntityMgr {

    @Inject
    private SLAFulfillmentReaderRepository readerRepository;
    @Inject
    private SLAFulfillmentWriterRepository writerRepository;

    @Override
    @Transactional(transactionManager = "transactionManager", propagation = Propagation.REQUIRES_NEW, readOnly = true)
    public SLAFulfillment findByPid(Long pid) {
        return readerRepository.findByPid(pid);
    }

    @Override
    @Transactional(transactionManager = "transactionManager", propagation = Propagation.REQUIRES_NEW, readOnly = true)
    public SLAFulfillment findByActionAndTerm(Action action, SLATerm term) {
        Preconditions.checkNotNull(action, "Action should not be null");
        Preconditions.checkNotNull(term, "Term should not be null");
        return readerRepository.findByActionAndTerm(action, term);
    }

    @Override
    @Transactional(transactionManager = "transactionManager", propagation = Propagation.REQUIRES_NEW, readOnly = true)
    public List<SLAFulfillment> findByAction(Action action) {
        Preconditions.checkNotNull(action, "Action should not be null");
        return readerRepository.findByAction(action);
    }

    @Override
    @Transactional(transactionManager = "transactionManager", propagation = Propagation.REQUIRES_NEW, readOnly = true)
    public List<SLAFulfillment> findByTerm(SLATerm term) {
        Preconditions.checkNotNull(term, "Term should not be null");
        return readerRepository.findByTerm(term);
    }

    @Override
    @Transactional(transactionManager = "transactionManager", propagation = Propagation.REQUIRES_NEW, readOnly = true)
    public List<SLAFulfillment> findByTenant(Tenant tenant) {
        Preconditions.checkNotNull(tenant, "tenant should not be null");
        return readerRepository.findByTenant(tenant);
    }

    @Override
    @Transactional(transactionManager = "transactionManager", propagation = Propagation.REQUIRES_NEW, readOnly = true)
    public BaseJpaRepository<SLAFulfillment, Long> getRepository() {
        return writerRepository;
    }
}
