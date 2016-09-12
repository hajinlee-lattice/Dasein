package com.latticeengines.datacloud.match.entitymgr;

import java.util.List;

public interface MetadataColumnEntityMgr<E> {

    List<E> findByTag(String tag);

    List<E> findAll();

    E findById(String columnId);

}
