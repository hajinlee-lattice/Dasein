package com.latticeengines.datafabric.entitymanager;

import java.util.List;

import org.apache.avro.generic.GenericRecord;

import com.latticeengines.domain.exposed.datafabric.FabricStoreEnum;
import com.latticeengines.domain.exposed.datafabric.generic.GenericFabricStatus;
import com.latticeengines.domain.exposed.datafabric.generic.GenericRecordRequest;
import com.latticeengines.domain.exposed.dataplatform.HasId;

public interface GenericFabricEntityManager<T extends HasId<String>> {

    GenericFabricStatus getBatchStatus(String batchId);

    String createUniqueBatchId(Long totalCount);

    String createOrGetNamedBatchId(String batchId, Long totalCount, boolean createNew);

    void updateBatchCount(String batchId, long delta, boolean isFinished);

    boolean cleanup(String batchId);

    void publishRecord(GenericRecordRequest request, GenericRecord genericRecord);

    void publishEntity(GenericRecordRequest request, T entity, Class<T> clazz);

    void publishEntityBatch(String batchId, String recordType, FabricStoreEnum store, String repository,
            List<GenericRecordRequest> requests, List<T> entities, Class<T> clazz);

    void publishRecordBatch(String batchId, String recordType, FabricStoreEnum store, String repository,
            List<GenericRecordRequest> requests, List<GenericRecord> genericRecords);

}
