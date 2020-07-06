package com.latticeengines.apps.cdl.entitymgr.impl;

import java.util.List;

import javax.inject.Inject;

import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import com.latticeengines.apps.cdl.entitymgr.JourneyStageEntityMgr;
import com.latticeengines.apps.cdl.repository.jpa.writer.JourneyStageWriterRepository;
import com.latticeengines.apps.cdl.repository.reader.JourneyStageReaderRepository;
import com.latticeengines.db.exposed.entitymgr.impl.JpaEntityMgrRepositoryImpl;
import com.latticeengines.db.exposed.repository.BaseJpaRepository;
import com.latticeengines.domain.exposed.cdl.activity.JourneyStage;
import com.latticeengines.domain.exposed.security.Tenant;

@Component("JourneyStageEntityMgr")
public class JourneyStageEntityMgrImpl extends JpaEntityMgrRepositoryImpl<JourneyStage, Long> implements JourneyStageEntityMgr {

    @Inject
    private JourneyStageReaderRepository readerRepository;

    @Inject
    private JourneyStageWriterRepository writerRepository;

    @Override
    @Transactional(transactionManager = "transactionManager", propagation = Propagation.REQUIRES_NEW, readOnly = true)
    public JourneyStage findByPid(Long pid) {
        return readerRepository.findByPid(pid);
    }

    @Override
    @Transactional(transactionManager = "transactionManager", propagation = Propagation.REQUIRES_NEW, readOnly = true)
    public List<JourneyStage> findByTenant(Tenant tenant) {
        return readerRepository.findByTenant(tenant);
    }

    @Override
    public BaseJpaRepository<JourneyStage, Long> getRepository() {
        return writerRepository;
    }
}
