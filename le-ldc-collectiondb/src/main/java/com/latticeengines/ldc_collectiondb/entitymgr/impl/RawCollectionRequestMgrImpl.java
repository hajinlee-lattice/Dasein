package com.latticeengines.ldc_collectiondb.entitymgr.impl;

import java.sql.Timestamp;
import java.util.List;

import javax.inject.Inject;

import org.springframework.data.domain.PageRequest;
import org.springframework.stereotype.Component;

import com.latticeengines.db.exposed.entitymgr.impl.JpaEntityMgrRepositoryImpl;
import com.latticeengines.db.exposed.repository.BaseJpaRepository;
import com.latticeengines.ldc_collectiondb.entity.RawCollectionRequest;
import com.latticeengines.ldc_collectiondb.entity.VendorConfig;
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
    public List<RawCollectionRequest> getNonTransferred(int limit) {

        return readerRepository.findByTransferred(false, PageRequest.of(0, limit));

    }

    @Override
    public void saveRequests(Iterable<RawCollectionRequest> reqs) {

        repository.saveAll(reqs);

    }

    @Override
    public void saveDomains(Iterable<String> domains, String reqId) {

        for (String vendor: VendorConfig.EFFECTIVE_VENDOR_SET) {

            Timestamp ts = new Timestamp(System.currentTimeMillis());

            for (String domain : domains) {

                repository.save(RawCollectionRequest.generate(vendor, domain, ts, reqId));

            }
        }

    }

    @Override
    public void cleanupRequestsBetween(Timestamp start, Timestamp end) {
        repository.removeByRequestedTimeBetween(start, end);
    }


    @Override
    public void cleanupTransferred() {

        repository.removeByTransferred(true);

    }
}
