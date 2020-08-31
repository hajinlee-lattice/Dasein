package com.latticeengines.apps.cdl.entitymgr.impl;

import java.util.List;

import javax.inject.Inject;

import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import com.latticeengines.apps.cdl.entitymgr.ActivityAlertsConfigEntityMgr;
import com.latticeengines.apps.cdl.repository.jpa.writer.ActivityAlertsConfigWriterRepository;
import com.latticeengines.apps.cdl.repository.reader.ActivityAlertsConfigReaderRepository;
import com.latticeengines.db.exposed.entitymgr.impl.JpaEntityMgrRepositoryImpl;
import com.latticeengines.db.exposed.repository.BaseJpaRepository;
import com.latticeengines.domain.exposed.cdl.activity.ActivityAlertsConfig;
import com.latticeengines.domain.exposed.security.Tenant;

@Component("activityAlertsConfigEntityMgr")
public class ActivityAlertsConfigEntityMgrImpl extends JpaEntityMgrRepositoryImpl<ActivityAlertsConfig, Long>
        implements ActivityAlertsConfigEntityMgr {

    @Inject
    private ActivityAlertsConfigReaderRepository readerRepository;

    @Inject
    private ActivityAlertsConfigWriterRepository writerRepository;

    @Override
    @Transactional(transactionManager = "transactionManager", propagation = Propagation.REQUIRES_NEW, readOnly = true)
    public List<ActivityAlertsConfig> findAllByTenant(Tenant tenant) {
        return readerRepository.findAllByTenant(tenant);
    }

    @Override
    @Transactional(transactionManager = "transactionManager", propagation = Propagation.REQUIRES_NEW, readOnly = true)
    public ActivityAlertsConfig findByPid(Long pid) {
        return readerRepository.findById(pid).orElse(null);
    }

    @Override
    public BaseJpaRepository<ActivityAlertsConfig, Long> getRepository() {
        return writerRepository;
    }
}
