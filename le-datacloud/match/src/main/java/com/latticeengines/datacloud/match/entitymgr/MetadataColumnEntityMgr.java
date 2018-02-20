package com.latticeengines.datacloud.match.entitymgr;

import java.util.List;

import com.latticeengines.db.exposed.entitymgr.BaseEntityMgrRepository;

public interface MetadataColumnEntityMgr<E> extends BaseEntityMgrRepository<E, Long> {

    void create(E metadataColumn);

    List<E> findByTag(String tag, String dataCloudVersion);

    List<E> findAll(String dataCloudVersion);

    List<E> findByPage(String dataCloudVersion, int page, int pageSize);

    Long count(String dataCloudVersion);

    E findById(String columnId, String dataCloudVersion);

}
