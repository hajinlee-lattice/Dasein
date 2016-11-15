package com.latticeengines.datacloud.match.entitymgr;

import java.util.List;

public interface MetadataColumnEntityMgr<E> {

    void create(E metadataColumn);

    void deleteByColumnIdAndDataCloudVersion(String columnId, String dataCloudVersion);

    List<E> findByTag(String tag, String dataCloudVersion);

    List<E> findAll(String dataCloudVersion);

    E findById(String columnId, String dataCloudVersion);

    void updateMetadataColumns(String dataCloudVersion, List<E> metadataColumns);

}
