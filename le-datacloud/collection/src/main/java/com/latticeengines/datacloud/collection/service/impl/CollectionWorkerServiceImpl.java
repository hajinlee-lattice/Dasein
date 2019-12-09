package com.latticeengines.datacloud.collection.service.impl;

import java.sql.Timestamp;
import java.util.List;

import javax.inject.Inject;

import org.apache.commons.collections4.ListUtils;
import org.springframework.stereotype.Component;

import com.latticeengines.datacloud.collection.service.CollectionWorkerService;
import com.latticeengines.ldc_collectiondb.entity.CollectionWorker;
import com.latticeengines.ldc_collectiondb.entity.VendorConfig;
import com.latticeengines.ldc_collectiondb.entitymgr.CollectionWorkerMgr;

@Component
public class CollectionWorkerServiceImpl implements CollectionWorkerService {

    @Inject
    CollectionWorkerMgr collectionWorkerMgr;

    @Override
    public CollectionWorkerMgr getEntityMgr() {

        return collectionWorkerMgr;

    }

    @Override
    public List<CollectionWorker> getActiveWorker(String vendor) {
        vendor = vendor.toUpperCase();
        if (!VendorConfig.EFFECTIVE_VENDOR_SET.contains(vendor)) {

            return ListUtils.emptyIfNull(null);

        }

        return collectionWorkerMgr.getActiveWorker(vendor);

    }

    @Override
    public List<CollectionWorker> getWorkerByStatus(List<String> status) {

        return collectionWorkerMgr.getWorkerByStatus(status);

    }

    @Override
    public List<CollectionWorker> getWorkerStopped(String vendor, Timestamp after) {

        vendor = vendor.toUpperCase();
        if (!VendorConfig.EFFECTIVE_VENDOR_SET.contains(vendor)) {

            return null;

        }

        return collectionWorkerMgr.getWorkerStopped(vendor, after);

    }

    @Override
    public void cleanupWorkerBetween(Timestamp start, Timestamp end) {
        collectionWorkerMgr.cleanupWorkerBetween(start, end);

    }

    @Override
    public List<CollectionWorker> getWorkerBySpawnTimeBetween(Timestamp start, Timestamp end) {
        return collectionWorkerMgr.getWorkerBySpawnTimeBetween(start, end);
    }
}
