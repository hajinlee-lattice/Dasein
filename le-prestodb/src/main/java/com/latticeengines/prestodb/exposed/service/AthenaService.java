package com.latticeengines.prestodb.exposed.service;

import java.util.List;
import java.util.Map;

import org.apache.commons.lang3.tuple.Pair;

import com.latticeengines.domain.exposed.metadata.datastore.AthenaDataUnit;
import com.latticeengines.domain.exposed.metadata.datastore.DataUnit;
import com.latticeengines.domain.exposed.metadata.datastore.S3DataUnit;

import reactor.core.publisher.Flux;

public interface AthenaService {

    boolean tableExists(String tableName);

    void deleteTableIfExists(String tableName);

    default void createTableIfNotExists(String tableName, String s3Bucket, String s3Prefix, DataUnit.DataFormat format) {
        createTableIfNotExists(tableName, s3Bucket, s3Prefix, format, null);
    }

    void createTableIfNotExists(String tableName, String s3Bucket, String s3Prefix, DataUnit.DataFormat format, //
                                List<Pair<String, Class<?>>> partitionKeys);

    AthenaDataUnit saveDataUnit(S3DataUnit s3DataUnit);

    Flux<Map<String, Object>> queryFlux(String sql);
    List<Map<String, Object>> query(String sql);
    <T> T queryObject(String sql, Class<T> clz);
    <T> List<T> queryForList(String sql, Class<T> clz);

}
