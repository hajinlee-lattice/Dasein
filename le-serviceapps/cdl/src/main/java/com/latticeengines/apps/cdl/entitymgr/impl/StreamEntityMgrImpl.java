package com.latticeengines.apps.cdl.entitymgr.impl;

import javax.inject.Inject;

import org.springframework.stereotype.Component;

import com.latticeengines.apps.cdl.entitymgr.StreamEntityMgr;
import com.latticeengines.apps.cdl.repository.reader.StreamReaderRepository;
import com.latticeengines.apps.cdl.repository.writer.StreamWriterRepository;
import com.latticeengines.db.exposed.entitymgr.impl.JpaEntityMgrRepositoryImpl;
import com.latticeengines.db.exposed.repository.BaseJpaRepository;
import com.latticeengines.domain.exposed.cdl.activity.Stream;

@Component("streamEntityMgr")
public class StreamEntityMgrImpl extends JpaEntityMgrRepositoryImpl<Stream, Long> implements StreamEntityMgr {
    @Inject
    private StreamReaderRepository readerRepository;

    @Inject
    private StreamWriterRepository writerRepository;

    @Override
    public BaseJpaRepository<Stream, Long> getRepository() {
        return writerRepository;
    }
}
