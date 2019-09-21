package com.latticeengines.apps.cdl.entitymgr.impl;

import javax.inject.Inject;

import org.springframework.stereotype.Component;

import com.latticeengines.apps.cdl.entitymgr.DimensionEntityMgr;
import com.latticeengines.apps.cdl.repository.jpa.writer.DimensionWriterRepository;
import com.latticeengines.apps.cdl.repository.reader.DimensionReaderRepository;
import com.latticeengines.db.exposed.entitymgr.impl.JpaEntityMgrRepositoryImpl;
import com.latticeengines.db.exposed.repository.BaseJpaRepository;
import com.latticeengines.domain.exposed.cdl.activity.Dimension;

@Component("dimensionEntityMgr")
public class DimensionEntityMgrImpl extends JpaEntityMgrRepositoryImpl<Dimension, Long> implements DimensionEntityMgr {

    @Inject
    private DimensionReaderRepository readerRepository;

    @Inject
    private DimensionWriterRepository writerRepository;

    @Override
    public BaseJpaRepository<Dimension, Long> getRepository() {
        return writerRepository;
    }
}
