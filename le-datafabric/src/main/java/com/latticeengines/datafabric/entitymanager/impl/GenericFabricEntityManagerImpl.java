package com.latticeengines.datafabric.entitymanager.impl;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import javax.annotation.PostConstruct;

import org.apache.avro.generic.GenericRecord;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Lazy;
import org.springframework.stereotype.Component;

import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.datafabric.entitymanager.GenericFabricEntityManager;
import com.latticeengines.domain.exposed.datafabric.FabricStoreEnum;
import com.latticeengines.domain.exposed.datafabric.TopicScope;
import com.latticeengines.domain.exposed.datafabric.generic.GenericFabricNode;
import com.latticeengines.domain.exposed.datafabric.generic.GenericFabricStatus;
import com.latticeengines.domain.exposed.datafabric.generic.GenericFabricStatusEnum;
import com.latticeengines.domain.exposed.datafabric.generic.GenericRecordRequest;
import com.latticeengines.domain.exposed.dataplatform.HasId;

@Component("genericFabricEntityManager")
@Lazy
public class GenericFabricEntityManagerImpl<T extends HasId<String>> extends BaseFabricEntityMgrImpl<T> implements
        GenericFabricEntityManager<T> {

    private static final int TEN_MINUTES = 600_000;

    private static final Logger log = LoggerFactory.getLogger(GenericFabricEntityManagerImpl.class);

    public static final String FABRIC_GENERIC_CONNECTOR = "FabricGenericConnector";
    public static final String FABRIC_GENERIC_RECORD = "FabricGenericRecord";
    private static final TopicScope ENVIRONMENT_PRIVATE = TopicScope.ENVIRONMENT_PRIVATE;

    private volatile boolean disabled;
    private volatile long lastCheckTime;

    @Value("${datafabric.generic.entity.kafka.replication}")
    private int kafkaRep;

    public GenericFabricEntityManagerImpl() {
        super(new BaseFabricEntityMgrImpl.Builder().recordType(FABRIC_GENERIC_RECORD).topic(FABRIC_GENERIC_CONNECTOR)
                .scope(ENVIRONMENT_PRIVATE).store(FABRIC_GENERIC_CONNECTOR).repository(FABRIC_GENERIC_CONNECTOR));
    }

    public GenericFabricEntityManagerImpl(Builder builder) {
        super(builder);
    }

    @PostConstruct
    public void initGeneric() {
        boolean result = messageService.createTopic(FABRIC_GENERIC_CONNECTOR, ENVIRONMENT_PRIVATE, 100, kafkaRep);
        if (result) {
            log.info("Topic was created! topic=" + FABRIC_GENERIC_CONNECTOR);
        } else {
            log.warn("Could not create/find topic=" + FABRIC_GENERIC_CONNECTOR
                    + " disabling the generic fabric entity manager!.");
            setDisable();
        }
    }

    @Override
    public String createUniqueBatchId(Long totalCount) {
        if (disabled && isInCheckPeriod()) {
            log.warn("Generic Fabric entity manager is disabled!");
            return null;
        }
        try {
            String uuid = UUID.randomUUID().toString();
            GenericFabricNode node = new GenericFabricNode();
            node.setName(uuid);
            if (totalCount != null) {
                node.setTotalCount(totalCount);
            }
            String data = JsonUtils.serialize(node);
            messageService.createZNode(uuid, data, true);
            setEnable();
            return uuid;
        } catch (Exception ex) {
            log.warn("Can not create batch Id. Disable the entity manager!", ex);
            setDisable();
            return null;
        }
    }

    @Override
    public String createOrGetNamedBatchId(String batchName, Long totalCount, boolean createNew) {
        if (disabled && isInCheckPeriod()) {
            log.warn("Generic Fabric entity manager is disabled!");
            return batchName;
        }
        try {
            GenericFabricNode node = new GenericFabricNode();
            node.setName(batchName);
            if (totalCount != null) {
                node.setTotalCount(totalCount);
            }
            String data = JsonUtils.serialize(node);
            messageService.createZNode(batchName, data, createNew);
            setEnable();
        } catch (Exception ex) {
            log.warn("Can not create batch Id=" + batchName + " disable the entity manager!", ex);
            setDisable();
        }
        return batchName;
    }

    @Override
    public GenericFabricStatus getBatchStatus(String batchId) {
        GenericFabricStatus status = new GenericFabricStatus();
        status.setStatus(GenericFabricStatusEnum.UNKNOWN);
        if (disabled && isInCheckPeriod()) {
            log.warn("Generic Fabric entity manager is disabled!");
            return status;
        }
        try {
            String data = messageService.readData(batchId);
            if (StringUtils.isBlank(data)) {
                return status;
            }

            GenericFabricNode fabricNode = JsonUtils.deserialize(data, GenericFabricNode.class);
            status.setStatus(getStatusEnum(batchId, fabricNode));
            setStatusProgress(batchId, status, fabricNode);
            status.setMessage(fabricNode.toString());
            setEnable();
        } catch (Exception ex) {
            log.warn("Can not get status for batch Id=" + batchId + " disable the entity manager!", ex);
            setDisable();
        }
        return status;
    }

    @Override
    public void publishEntity(GenericRecordRequest request, T entity, Class<T> clazz) {
        if (disabled && isInCheckPeriod()) {
            log.warn("Generic Fabric entity manager is disabled!");
            return;
        }
        boolean isValid = validateRequest(request);
        if (!isValid) {
            return;
        }
        Pair<GenericRecord, Map<String, Object>> pair = entityToPair(entity, clazz);
        GenericRecord genericRecord = (pair == null) ? null : pair.getLeft();
        publishInternal(request, genericRecord);
    }

    @Override
    public void publishEntityBatch(String batchId, String recordType, FabricStoreEnum store, String repository,
            List<GenericRecordRequest> requests, List<T> entities, Class<T> clazz) {
        boolean isValid = validateRequest(batchId, recordType, store, repository);
        if (!isValid) {
            return;
        }
        if (requests.size() != entities.size()) {
            log.error("Sizes does not match for requests and entities!");
            return;
        }
        for (int i = 0; i < requests.size(); i++) {
            if (disabled && isInCheckPeriod()) {
                log.warn("Generic Fabric entity manager is disabled!");
                return;
            }
            T entity = entities.get(i);
            GenericRecordRequest request = requests.get(i);
            request.setStores(Arrays.asList(store));
            request.setBatchId(batchId);
            request.setRecordType(recordType);
            request.setRepositories(Arrays.asList(repository));
            Pair<GenericRecord, Map<String, Object>> pair = entityToPair(entity, clazz);
            GenericRecord genericRecord = (pair == null) ? null : pair.getLeft();
            publishInternal(request, genericRecord);
        }
    }

    @Override
    public void publishRecord(GenericRecordRequest request, GenericRecord genericRecord) {
        if (disabled && isInCheckPeriod()) {
            log.warn("Generic Fabric entity manager is disabled!");
            return;
        }
        boolean isValid = validateRequest(request);
        if (!isValid) {
            return;
        }
        publishInternal(request, genericRecord);
    }

    @Override
    public void publishRecordBatch(String batchId, String recordType, FabricStoreEnum store, String repository,
            List<GenericRecordRequest> requests, List<GenericRecord> genericRecords) {
        boolean isValid = validateRequest(batchId, recordType, store, repository);
        if (!isValid) {
            return;
        }
        if (requests.size() != genericRecords.size()) {
            log.error("Sizes does not match for requests and records!");
            return;
        }
        for (int i = 0; i < requests.size(); i++) {
            if (disabled && isInCheckPeriod()) {
                log.warn("Generic Fabric entity manager is disabled!");
                return;
            }
            GenericRecordRequest request = requests.get(i);
            GenericRecord genericRecord = genericRecords.get(i);
            request.setBatchId(batchId);
            request.setRecordType(recordType);
            request.setStores(Arrays.asList(store));
            request.setRepositories(Arrays.asList(repository));
            publishInternal(request, genericRecord);
        }
    }

    public void publishInternal(GenericRecordRequest recordRequest, GenericRecord record) {
        try {
            Future<RecordMetadata> future = publish(recordRequest, record);
            if (!isInCheckPeriod()) {
                synchronized (this) {
                    if (!isInCheckPeriod()) {
                        try {
                            future.get(10, TimeUnit.SECONDS);
                            setEnable();
                        } catch (Exception ex) {
                            setDisable();
                            future.cancel(true);
                            log.error("Publish timeout! Disable Generic Entity Manager", ex);
                        }
                    }
                }
            }
        } catch (Throwable t) {
            setDisable();
            log.error("Publish failed! Disable Generic Entity Manager", t);
        }
    }

    private boolean validateRequest(String batchId, String recordType, FabricStoreEnum store, String repository) {
        if (store == null || StringUtils.isBlank(repository)) {
            log.error("Store or repository is not specified!");
            return false;
        }
        if (StringUtils.isBlank(batchId) || StringUtils.isBlank(recordType)) {
            log.error("batchId or recordType is not specified!");
            return false;
        }
        return true;
    }

    private boolean validateRequest(GenericRecordRequest request) {
        if (CollectionUtils.isEmpty(request.getStores())) {
            log.error("No stores specified!");
            return false;
        }
        if (CollectionUtils.isEmpty(request.getRepositories())) {
            log.error("No repository specified!");
            return false;
        }
        return true;
    }

    private void setStatusProgress(String batchId, GenericFabricStatus status, GenericFabricNode fabricNode) {
        if (fabricNode.getTotalCount() < 0) {
            status.setProgress(0.01f);
            return;
        }
        if (fabricNode.getTotalCount() == 0) {
            status.setProgress(1.0f);
            return;
        }
        float result = fabricNode.getFinishedCount() == fabricNode.getTotalCount() ? 1.0f : 1.0f
                * fabricNode.getFinishedCount() / fabricNode.getTotalCount();
        status.setProgress(result);
    }

    private GenericFabricStatusEnum getStatusEnum(String batchId, GenericFabricNode fabricNode) {

        if (fabricNode.getTotalCount() <= -1) {
            return GenericFabricStatusEnum.PROCESSING;
        }
        if (fabricNode.getFailedCount() > 0) {
            log.error("Batch failed for batch Id=" + batchId + " error=" + fabricNode.toString());
            return GenericFabricStatusEnum.ERROR;
        }
        if (fabricNode.getFinishedCount() >= fabricNode.getTotalCount()) {
            return GenericFabricStatusEnum.FINISHED;
        }
        if (fabricNode.getFinishedCount() < fabricNode.getTotalCount()) {
            return GenericFabricStatusEnum.PROCESSING;
        }
        return GenericFabricStatusEnum.UNKNOWN;
    }

    @Override
    public void updateBatchCount(String batchId, long delta, boolean isFinished) {
        if (disabled && isInCheckPeriod()) {
            log.warn("Generic Fabric entity manager is disabled!");
            return;
        }

        messageService.incrementalUpdate(
                batchId,
                (origData) -> {
                    try {
                        if (StringUtils.isBlank(origData)) {
                            return resetCount(delta, isFinished);
                        }
                        if (delta <= 0) {
                            return origData;
                        }
                        GenericFabricNode node = addCount(delta, isFinished, origData);
                        setEnable();
                        return JsonUtils.serialize(node);
                    } catch (Exception ex) {
                        log.error("Failed to update, batchIdName=" + batchId + " delta=" + delta
                                + " disable the entity manager!");
                        setDisable();
                        return origData;
                    }
                });
    }

    private String resetCount(long delta, boolean isFinished) {
        GenericFabricNode node = new GenericFabricNode();
        if (isFinished) {
            node.setFinishedCount(delta);
        } else {
            node.setFailedCount(delta);
        }
        return JsonUtils.serialize(node);
    }

    private GenericFabricNode addCount(long delta, boolean isFinished, String origData) {
        GenericFabricNode node = JsonUtils.deserialize(origData, GenericFabricNode.class);
        if (isFinished) {
            long newCount = node.getFinishedCount() + delta;
            if (newCount < Long.MAX_VALUE) {
                node.setFinishedCount(newCount);
            }
        } else {
            long newCount = node.getFailedCount() + delta;
            if (newCount < Long.MAX_VALUE) {
                node.setFailedCount(newCount);
            }
        }
        return node;
    }

    @Override
    public boolean cleanup(String batchId) {
        if (disabled && isInCheckPeriod()) {
            log.warn("Generic Fabric entity manager is disabled!");
            return false;
        }
        return messageService.cleanup(batchId);
    }

    public void setDisable() {
        if (!disabled) {
            synchronized (this) {
                if (!disabled) {
                    disabled = true;
                    lastCheckTime = System.currentTimeMillis();
                    log.error("Set GenericFabricEntityManagerImpl to be Disabled!");
                }
            }
        } else {
            lastCheckTime = System.currentTimeMillis();
        }
    }

    public void setEnable() {
        if (disabled) {
            synchronized (this) {
                if (disabled) {
                    disabled = false;
                    lastCheckTime = System.currentTimeMillis();
                    log.warn("Set GenericFabricEntityManagerImpl to be Enabled!");
                }
            }
        }
    }

    private boolean isInCheckPeriod() {
        return System.currentTimeMillis() - lastCheckTime <= TEN_MINUTES;
    }

}
