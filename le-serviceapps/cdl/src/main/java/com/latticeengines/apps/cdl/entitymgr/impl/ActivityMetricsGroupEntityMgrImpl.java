package com.latticeengines.apps.cdl.entitymgr.impl;

import java.util.List;

import javax.inject.Inject;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import com.latticeengines.apps.cdl.entitymgr.ActivityMetricsGroupEntityMgr;
import com.latticeengines.apps.cdl.repository.jpa.writer.ActivityMetricsGroupWriterRepository;
import com.latticeengines.apps.cdl.repository.reader.ActivityMetricsGroupReaderRepository;
import com.latticeengines.db.exposed.entitymgr.impl.JpaEntityMgrRepositoryImpl;
import com.latticeengines.db.exposed.repository.BaseJpaRepository;
import com.latticeengines.db.exposed.util.MultiTenantContext;
import com.latticeengines.domain.exposed.cdl.activity.ActivityMetricsGroup;
import com.latticeengines.domain.exposed.security.Tenant;

@Component("activityMetricsGroupEntityMgr")
public class ActivityMetricsGroupEntityMgrImpl extends JpaEntityMgrRepositoryImpl<ActivityMetricsGroup, Long>
        implements ActivityMetricsGroupEntityMgr {
    @Inject
    private ActivityMetricsGroupReaderRepository readerRepository;

    @Inject
    private ActivityMetricsGroupWriterRepository writerRepository;

    @Override
    public BaseJpaRepository<ActivityMetricsGroup, Long> getRepository() {
        return writerRepository;
    }

    @Override
    @Transactional(transactionManager = "transactionManager", propagation = Propagation.REQUIRES_NEW, readOnly = true)
    public ActivityMetricsGroup findByPid(Long pid) {
        return readerRepository.findByPid(pid);
    }

    @Override
    @Transactional(transactionManager = "transactionManager", propagation = Propagation.REQUIRES_NEW, readOnly = true)
    public String getNextAvailableGroupId(String base) {
        Tenant tenant = MultiTenantContext.getTenant();
        if (tenant == null) {
            throw new IllegalStateException("Tenant not set for multitenant context.");
        }
        List<String> groupIds = readerRepository.findGroupIdsByBase(tenant, base);
        String idx = CollectionUtils.isEmpty(groupIds) ? "" : getNextIndex(base, groupIds.get(0)).toString();
        return String.format("%s%s", base, idx);
    }

    @Override
    @Transactional(transactionManager = "transactionManager", propagation = Propagation.REQUIRES_NEW, readOnly = true)
    public List<ActivityMetricsGroup> findByTenant(Tenant tenant) {
        return readerRepository.findByTenant(tenant);
    }

    private Long getNextIndex(String base, String groupIds) {
        String curIdxStr = groupIds.substring(base.length());
        return StringUtils.isBlank(curIdxStr) ? 1L : Long.parseLong(curIdxStr) + 1;
    }
}
