package com.latticeengines.datacloud.collection.entitymgr.impl;

import java.util.List;

import javax.inject.Inject;

import org.springframework.stereotype.Component;

import com.latticeengines.datacloud.collection.entitymgr.RawCollectionRequestMgr;
import com.latticeengines.datacloud.collection.repository.RawCollectionRequestRepository;
import com.latticeengines.datacloud.collection.repository.reader.RawCollectionRequestReaderRepository;
import com.latticeengines.db.exposed.entitymgr.impl.JpaEntityMgrRepositoryImpl;
import com.latticeengines.db.exposed.repository.BaseJpaRepository;
import com.latticeengines.ldc_collectiondb.entity.RawCollectionRequest;

@Component("rawCollectionRequestMgr")
public class RawCollectionRequestMgrImpl extends JpaEntityMgrRepositoryImpl<RawCollectionRequest, Long> //
        implements RawCollectionRequestMgr {

    @Inject
    private RawCollectionRequestRepository rawCollectionRequestRepository;

    @Inject
    private RawCollectionRequestReaderRepository rawCollectionRequestReaderRepository;

    @Override
    public BaseJpaRepository<RawCollectionRequest, Long> getRepository() {
        return rawCollectionRequestRepository;
    }

    @Override
    public List<RawCollectionRequest> getNonTransferred() {
        return rawCollectionRequestReaderRepository.findByTransferred(false);
    }

    @Override
    public void saveRequests(Iterable<RawCollectionRequest> reqs) {
        rawCollectionRequestRepository.saveAll(reqs);
    }

}
