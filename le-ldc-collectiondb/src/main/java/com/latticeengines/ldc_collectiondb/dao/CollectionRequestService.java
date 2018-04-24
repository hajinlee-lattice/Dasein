package com.latticeengines.ldc_collectiondb.dao;

import com.latticeengines.ldc_collectiondb.entity.CollectionRequest;
import com.latticeengines.ldc_collectiondb.entity.RawCollectionRequest;

import java.util.BitSet;
import java.util.List;

public interface CollectionRequestService {
    public abstract CollectionRequest getById(long id);
    public abstract void save(CollectionRequest rawReq);
    public abstract void update(CollectionRequest rawReq);
    public abstract void delete(long id);
    public abstract List<CollectionRequest> getAll();
    public abstract BitSet add(List<RawCollectionRequest> toAdd);
}
