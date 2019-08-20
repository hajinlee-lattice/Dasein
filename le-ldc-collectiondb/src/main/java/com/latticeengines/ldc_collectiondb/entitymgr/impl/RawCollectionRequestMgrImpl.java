package com.latticeengines.ldc_collectiondb.entitymgr.impl;

import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.List;

import javax.inject.Inject;

import org.springframework.data.domain.PageRequest;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

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

    private void saveRequestsInternal(List<RawCollectionRequest> reqBuf, int bufCap, Iterable<String> domains, String vendor, String reqId) {

        Timestamp ts = new Timestamp(System.currentTimeMillis());

        for (String domain : domains) {

            //add to buf
            reqBuf.add(RawCollectionRequest.generate(vendor, domain, ts, reqId));

            //save
            if (reqBuf.size() == bufCap) {

                repository.saveAll(reqBuf);
                reqBuf.clear();
                ts = new Timestamp(System.currentTimeMillis());
            }

        }

        //save
        if (reqBuf.size() > 0) {

            repository.saveAll(reqBuf);
            reqBuf.clear();

        }

    }

    @Override
    @Transactional
    public void saveRequests(Iterable<String> domains, String reqId) {

        int batch = 512;
        List<RawCollectionRequest> reqBuf = new ArrayList<>(batch);
        for (String vendor: VendorConfig.EFFECTIVE_VENDOR_SET) {

            saveRequestsInternal(reqBuf, batch, domains, vendor, reqId);

        }

    }

    @Override
    @Transactional
    public void saveRequests(Iterable<String> domains, String vendor, String reqId) {

        int batch = 512;
        List<RawCollectionRequest> reqBuf = new ArrayList<>(batch);
        saveRequestsInternal(reqBuf, batch, domains, vendor, reqId);

    }

    @Override
    @Transactional
    public void deleteFiltered(Iterable<RawCollectionRequest> added, BitSet filter) {

        //temp buf
        int batch = 512;
        List<RawCollectionRequest> delBuf = new ArrayList<>(batch);

        int i = 0;
        for (RawCollectionRequest req : added) {

            //add to temp buf
            if (filter.get(i)) {

                delBuf.add(req);

            }
            ++i;

            //delete in batch
            if (delBuf.size() == batch) {

                repository.deleteAll(delBuf);
                delBuf.clear();

            }
        }

        //delete if buf not empty
        if (delBuf.size() > 0) {

            repository.deleteAll(delBuf);
            delBuf.clear();

        }

    }

    @Override
    @Transactional
    public void updateTransferred(Iterable<RawCollectionRequest> added, BitSet filter) {

        //temp buf
        int batch = 512;
        List<RawCollectionRequest> updBuf = new ArrayList<>(batch);

        int i = 0;
        for (RawCollectionRequest req : added) {

            //add to temp buf
            if (!filter.get(i)) {

                updBuf.add(req);

            }
            ++i;

            //update in batch
            if (updBuf.size() == batch) {

                repository.saveAll(updBuf);
                updBuf.clear();

            }

        }

        //update if buf not empty
        if (updBuf.size() > 0) {

            repository.saveAll(updBuf);
            updBuf.clear();

        }

    }

    @Override
    public void cleanupRequestsBetween(Timestamp start, Timestamp end) {

        repository.removeByRequestedTimeBetween(start, end);

    }

    @Override
    public void cleanupTransferred(int batch) {

        long minPid = readerRepository.getMinPid();
        repository.removeByTransferredAndPidLessThan(true, minPid + batch);

    }
}
