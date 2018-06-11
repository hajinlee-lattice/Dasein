package com.latticeengines.datacloud.collection.entitymgr.impl;

import java.util.List;

import javax.inject.Inject;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import com.latticeengines.datacloud.collection.entitymgr.RawCollectionRequestMgr;
import com.latticeengines.datacloud.collection.repository.RawCollectionRequestRepository;
import com.latticeengines.datacloud.collection.repository.reader.RawCollectionRequestReaderRepository;
import com.latticeengines.db.exposed.entitymgr.impl.JpaEntityMgrRepositoryImpl;
import com.latticeengines.db.exposed.repository.BaseJpaRepository;
import com.latticeengines.ldc_collectiondb.entity.RawCollectionRequest;

@Component
public class RawCollectionRequestMgrImpl extends JpaEntityMgrRepositoryImpl<RawCollectionRequest, Long> implements RawCollectionRequestMgr {
    private static final Logger log = LoggerFactory.getLogger(RawCollectionRequestMgrImpl.class);

    @Inject
    private RawCollectionRequestRepository rawCollectionRequestRepository;
    @Inject
    private RawCollectionRequestReaderRepository rawCollectionRequestReaderRepository;

    @Override
    public BaseJpaRepository<RawCollectionRequest, Long> getRepository() {
        return rawCollectionRequestRepository;
    }

    public List<RawCollectionRequest> getNonTransferred() {
        List<RawCollectionRequest> resultList = rawCollectionRequestReaderRepository.findByTransferred(false);

        return resultList;
    }
}
