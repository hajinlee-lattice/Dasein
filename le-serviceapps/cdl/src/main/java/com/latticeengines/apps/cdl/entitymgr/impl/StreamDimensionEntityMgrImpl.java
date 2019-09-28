package com.latticeengines.apps.cdl.entitymgr.impl;

import java.util.List;

import javax.inject.Inject;

import org.apache.commons.collections4.CollectionUtils;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import com.google.common.base.Preconditions;
import com.latticeengines.apps.cdl.entitymgr.StreamDimensionEntityMgr;
import com.latticeengines.apps.cdl.repository.jpa.writer.StreamDimensionWriterRepository;
import com.latticeengines.apps.cdl.repository.reader.StreamDimensionReaderRepository;
import com.latticeengines.common.exposed.validator.annotation.NotNull;
import com.latticeengines.db.exposed.entitymgr.impl.JpaEntityMgrRepositoryImpl;
import com.latticeengines.db.exposed.repository.BaseJpaRepository;
import com.latticeengines.domain.exposed.cdl.activity.AtlasStream;
import com.latticeengines.domain.exposed.cdl.activity.StreamDimension;
import com.latticeengines.domain.exposed.security.Tenant;

@Component("streamDimensionEntityMgr")
public class StreamDimensionEntityMgrImpl extends JpaEntityMgrRepositoryImpl<StreamDimension, Long> implements StreamDimensionEntityMgr {

    @Inject
    private StreamDimensionReaderRepository readerRepository;

    @Inject
    private StreamDimensionWriterRepository writerRepository;

    @Override
    @Transactional(transactionManager = "transactionManager", propagation = Propagation.REQUIRES_NEW, readOnly = true)
    public StreamDimension findByNameAndTenantAndStream(@NotNull String name, @NotNull Tenant tenant,
            @NotNull AtlasStream stream) {
        Preconditions.checkNotNull(name, "Name should not be null");
        Preconditions.checkNotNull(tenant, "Tenant should not be null");
        Preconditions.checkNotNull(stream, "Stream should not be null");
        List<StreamDimension> dimensions = readerRepository.findByNameAndTenantAndStream(name, tenant, stream);
        if (CollectionUtils.isEmpty(dimensions)) {
            return null;
        }
        Preconditions.checkArgument(dimensions.size() == 1,
                String.format("Dimension %s should be unique for tenant %s, got %d instead", name, tenant.getId(),
                        dimensions.size()));
        return dimensions.get(0);
    }

    @Override
    @Transactional(transactionManager = "transactionManager", propagation = Propagation.REQUIRES_NEW, readOnly = true)
    public List<StreamDimension> findByTenant(@NotNull Tenant tenant) {
        Preconditions.checkNotNull(tenant, "Tenant should not be null");
        return readerRepository.findByTenant(tenant);
    }

    @Override
    public BaseJpaRepository<StreamDimension, Long> getRepository() {
        return writerRepository;
    }
}
