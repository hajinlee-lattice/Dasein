package com.latticeengines.ldc_collectiondb.entitymgr.impl;

import java.util.List;

import javax.inject.Inject;

import org.springframework.stereotype.Component;

import com.latticeengines.db.exposed.entitymgr.impl.JpaEntityMgrRepositoryImpl;
import com.latticeengines.db.exposed.repository.BaseJpaRepository;
import com.latticeengines.ldc_collectiondb.entity.RawCollectionRequest;
import com.latticeengines.ldc_collectiondb.entitymgr.RawCollectionRequestMgr;
import com.latticeengines.ldc_collectiondb.repository.reader.RawCollectionRequestReaderRepository;
import com.latticeengines.ldc_collectiondb.repository.writer.RawCollectionRequestWriterRepository;

@Component("rawCollectionRequestMgr")
public class RawCollectionRequestMgrImpl extends JpaEntityMgrRepositoryImpl<RawCollectionRequest, Long> //
        implements RawCollectionRequestMgr {

    @Inject
    private RawCollectionRequestWriterRepository repository;

    @Inject
    private RawCollectionRequestReaderRepository readerRepository;

    @Override
    public BaseJpaRepository<RawCollectionRequest, Long> getRepository() {
        return repository;
    }

    @Override
    public List<RawCollectionRequest> getNonTransferred() {
        return readerRepository.findByTransferred(false);
    }

    @Override
    public void saveRequests(Iterable<RawCollectionRequest> reqs) {
        repository.saveAll(reqs);
    }

}
