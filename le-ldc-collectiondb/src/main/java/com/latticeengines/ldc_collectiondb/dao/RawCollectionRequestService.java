package com.latticeengines.ldc_collectiondb.dao;

import com.latticeengines.db.exposed.repository.BaseJpaRepository;
import com.latticeengines.ldc_collectiondb.entity.RawCollectionRequest;

import java.util.BitSet;
import java.util.List;

public interface RawCollectionRequestService {
    public abstract RawCollectionRequest getById(long id);
    public abstract void save(RawCollectionRequest rawReq);
    public abstract void update(RawCollectionRequest rawReq);
    public abstract void delete(long id);
    public abstract List<RawCollectionRequest> getAll();
    public abstract List<RawCollectionRequest> getNonTransferred();
    public abstract void updateTransferredStatus(List<RawCollectionRequest> added, BitSet filter, boolean deleteFiltered);
}
