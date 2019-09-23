package com.latticeengines.apps.cdl.entitymgr.impl;

import java.util.List;

import javax.inject.Inject;

import org.apache.commons.collections4.CollectionUtils;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import com.google.common.base.Preconditions;
import com.latticeengines.apps.cdl.entitymgr.StreamEntityMgr;
import com.latticeengines.apps.cdl.repository.jpa.writer.StreamWriterRepository;
import com.latticeengines.apps.cdl.repository.reader.StreamReaderRepository;
import com.latticeengines.common.exposed.validator.annotation.NotNull;
import com.latticeengines.db.exposed.entitymgr.impl.JpaEntityMgrRepositoryImpl;
import com.latticeengines.db.exposed.repository.BaseJpaRepository;
import com.latticeengines.domain.exposed.cdl.activity.Stream;
import com.latticeengines.domain.exposed.security.Tenant;

@Component("streamEntityMgr")
public class StreamEntityMgrImpl extends JpaEntityMgrRepositoryImpl<Stream, Long> implements StreamEntityMgr {
    @Inject
    private StreamReaderRepository readerRepository;

    @Inject
    private StreamWriterRepository writerRepository;

    @Override
    @Transactional(transactionManager = "transactionManager", propagation = Propagation.REQUIRES_NEW, readOnly = true)
    public Stream findByNameAndTenant(@NotNull String name, @NotNull Tenant tenant) {
        Preconditions.checkNotNull(name, "Name should not be null");
        Preconditions.checkNotNull(tenant, "Tenant should not be null");
        List<Stream> streams = readerRepository.findByNameAndTenant(name, tenant);
        if (CollectionUtils.isEmpty(streams)) {
            return null;
        }
        Preconditions.checkArgument(streams.size() == 1, String.format(
                "Stream %s should be unique for tenant %s, got %d instead", name, tenant.getId(), streams.size()));
        return streams.get(0);
    }

    @Override
    @Transactional(transactionManager = "transactionManager", propagation = Propagation.REQUIRES_NEW, readOnly = true)
    public List<Stream> findByTenant(@NotNull Tenant tenant) {
        Preconditions.checkNotNull(tenant, "Tenant should not be null");
        return readerRepository.findByTenant(tenant);
    }

    @Override
    public BaseJpaRepository<Stream, Long> getRepository() {
        return writerRepository;
    }
}
