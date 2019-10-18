package com.latticeengines.apps.cdl.entitymgr.impl;

import java.util.List;

import javax.inject.Inject;

import org.apache.commons.collections4.CollectionUtils;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import com.google.common.base.Preconditions;
import com.latticeengines.apps.cdl.entitymgr.AtlasStreamEntityMgr;
import com.latticeengines.apps.cdl.repository.jpa.writer.AtlasStreamWriterRepository;
import com.latticeengines.apps.cdl.repository.reader.AtlasStreamReaderRepository;
import com.latticeengines.common.exposed.util.HibernateUtils;
import com.latticeengines.common.exposed.validator.annotation.NotNull;
import com.latticeengines.db.exposed.entitymgr.impl.JpaEntityMgrRepositoryImpl;
import com.latticeengines.db.exposed.repository.BaseJpaRepository;
import com.latticeengines.domain.exposed.cdl.activity.AtlasStream;
import com.latticeengines.domain.exposed.security.Tenant;

@Component("streamEntityMgr")
public class AtlasStreamEntityMgrImpl extends JpaEntityMgrRepositoryImpl<AtlasStream, Long> implements AtlasStreamEntityMgr {
    @Inject
    private AtlasStreamReaderRepository readerRepository;

    @Inject
    private AtlasStreamWriterRepository writerRepository;

    @Override
    @Transactional(transactionManager = "transactionManager", propagation = Propagation.REQUIRES_NEW, readOnly = true)
    public AtlasStream findByNameAndTenant(@NotNull String name, @NotNull Tenant tenant) {
        return findByNameAndTenant(name, tenant, false);
    }

    @Override
    @Transactional(transactionManager = "transactionManager", propagation = Propagation.REQUIRES_NEW, readOnly = true)
    public AtlasStream findByNameAndTenant(String name, Tenant tenant, boolean inflateDimensions) {
        Preconditions.checkNotNull(name, "Name should not be null");
        Preconditions.checkNotNull(tenant, "Tenant should not be null");
        List<AtlasStream> streams = readerRepository.findByNameAndTenant(name, tenant);
        if (CollectionUtils.isEmpty(streams)) {
            return null;
        }
        Preconditions.checkArgument(streams.size() == 1, String.format(
                "Stream %s should be unique for tenant %s, got %d instead", name, tenant.getId(), streams.size()));
        AtlasStream stream = streams.get(0);
        if (inflateDimensions) {
            HibernateUtils.inflateDetails(stream.getDimensions());
        }
        return stream;
    }

    @Override
    @Transactional(transactionManager = "transactionManager", propagation = Propagation.REQUIRES_NEW, readOnly = true)
    public List<AtlasStream> findByTenant(@NotNull Tenant tenant) {
        Preconditions.checkNotNull(tenant, "Tenant should not be null");
        return readerRepository.findByTenant(tenant);
    }

    @Override
    @Transactional(transactionManager = "transactionManager", propagation = Propagation.REQUIRES_NEW, readOnly = true)
    public List<AtlasStream> findByTenant(@NotNull Tenant tenant, boolean inflateDimensions) {
        Preconditions.checkNotNull(tenant, "Tenant should not be null");

        List<AtlasStream> streams = readerRepository.findByTenant(tenant);
        if (CollectionUtils.isNotEmpty(streams) && inflateDimensions) {
            streams.forEach(stream -> HibernateUtils.inflateDetails(stream.getDimensions()));
        }
        return streams;
    }

    @Override
    public BaseJpaRepository<AtlasStream, Long> getRepository() {
        return writerRepository;
    }
}
