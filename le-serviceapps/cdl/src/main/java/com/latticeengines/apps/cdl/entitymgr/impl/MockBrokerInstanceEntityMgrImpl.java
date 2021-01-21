package com.latticeengines.apps.cdl.entitymgr.impl;

import java.util.Date;
import java.util.List;

import javax.inject.Inject;

import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import com.latticeengines.apps.cdl.entitymgr.MockBrokerInstanceEntityMgr;
import com.latticeengines.apps.cdl.repository.jpa.writer.MockBrokerInstanceWriterRepository;
import com.latticeengines.apps.cdl.repository.reader.MockBrokerInstanceReaderRepository;
import com.latticeengines.db.exposed.entitymgr.impl.JpaEntityMgrRepositoryImpl;
import com.latticeengines.db.exposed.repository.BaseJpaRepository;
import com.latticeengines.db.exposed.util.MultiTenantContext;
import com.latticeengines.domain.exposed.cdl.MockBrokerInstance;
import com.latticeengines.domain.exposed.security.Tenant;

@Component("mockBrokerInstanceEntityMgr")
public class MockBrokerInstanceEntityMgrImpl extends JpaEntityMgrRepositoryImpl<MockBrokerInstance, Long> implements MockBrokerInstanceEntityMgr {

    @Inject
    private MockBrokerInstanceReaderRepository readerRepository;

    @Inject
    private MockBrokerInstanceWriterRepository writerRepository;

    @Override
    public BaseJpaRepository<MockBrokerInstance, Long> getRepository() {
        return writerRepository;
    }

    @Override
    @Transactional(propagation = Propagation.REQUIRES_NEW, readOnly = true)
    public List<MockBrokerInstance> getAllInstance(int maxRow) {
        return readerRepository.findAllWithLimit(maxRow);
    }

    @Override
    public List<MockBrokerInstance> getAllValidInstance(Date nextScheduledTime) {
        return readerRepository.findByNextScheduledTime(nextScheduledTime);
    }

    @Override
    @Transactional(propagation = Propagation.REQUIRES_NEW, readOnly = true)
    public MockBrokerInstance findBySourceId(String sourceId) {
        Tenant tenant = MultiTenantContext.getTenant();
        return readerRepository.findByTenantAndSourceId(tenant, sourceId);
    }

}
