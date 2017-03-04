package com.latticeengines.datafabric.entitymanager.impl;

import java.util.Map;
import java.util.UUID;

import javax.annotation.PostConstruct;

import org.apache.avro.generic.GenericRecord;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.context.annotation.Lazy;
import org.springframework.stereotype.Component;

import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.datafabric.entitymanager.GenericFabricEntityManager;
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

    private static final Log log = LogFactory.getLog(GenericFabricEntityManagerImpl.class);

    public static final String FABRIC_GENERIC_CONNECTOR = "FabricGenericConnector";
    public static final String FABRIC_GENERIC_RECORD = "FabricGenericRecord";
    private static final TopicScope ENVIRONMENT_PRIVATE = TopicScope.ENVIRONMENT_PRIVATE;

    public GenericFabricEntityManagerImpl() {
        super(new BaseFabricEntityMgrImpl.Builder().recordType(FABRIC_GENERIC_RECORD).topic(FABRIC_GENERIC_CONNECTOR)
                .scope(ENVIRONMENT_PRIVATE).store(FABRIC_GENERIC_CONNECTOR).repository(FABRIC_GENERIC_CONNECTOR));
    }

    public GenericFabricEntityManagerImpl(Builder builder) {
        super(builder);
    }

    @PostConstruct
    public void initGeneric() {
        boolean result = messageService.createTopic(FABRIC_GENERIC_CONNECTOR, ENVIRONMENT_PRIVATE, 100, 2);
        if (result) {
            log.info("Topic was created! topic=" + FABRIC_GENERIC_CONNECTOR);
        }
    }

    @Override
    public String createUniqueBatchId(Long totalCount) {
        String uuid = UUID.randomUUID().toString();
        GenericFabricNode node = new GenericFabricNode();
        node.setName(uuid);
        if (totalCount != null) {
            node.setTotalCount(totalCount);
        }
        String data = JsonUtils.serialize(node);
        messageService.createZNode(uuid, data, true);
        return uuid;
    }

    @Override
    public String createOrGetNamedBatchId(String batchName, Long totalCount, boolean createNew) {
        GenericFabricNode node = new GenericFabricNode();
        node.setName(batchName);
        if (totalCount != null) {
            node.setTotalCount(totalCount);
        }
        String data = JsonUtils.serialize(node);
        messageService.createZNode(batchName, data, createNew);
        return batchName;
    }

    @Override
    public GenericFabricStatus getBatchStatus(String batchId) {
        GenericFabricStatus status = new GenericFabricStatus();
        String data = messageService.readData(batchId);
        if (StringUtils.isBlank(data)) {
            status.setStatus(GenericFabricStatusEnum.UNKNOWN);
            return status;
        }

        GenericFabricNode fabricNode = JsonUtils.deserialize(data, GenericFabricNode.class);
        status.setStatus(getStatusEnum(batchId, fabricNode));
        setStatusProgress(batchId, status, fabricNode);
        status.setMessage(fabricNode.getMessage());
        return status;
    }

    @Override
    public void publishEntity(GenericRecordRequest request, T entity, Class<T> clazz) {
        try {
            Pair<GenericRecord, Map<String, Object>> pair = entityToPair(entity, clazz);
            GenericRecord genericRecord = (pair == null) ? null : pair.getLeft();
            publish(request, genericRecord);

        } catch (Exception e) {
            log.warn("Publish entity failed! entity Id=" + entity.getId(), e);
            throw new RuntimeException("Publish entity failed! entity Id=" + entity.getId(), e);
        }
    }

    @Override
    public void publishRecord(GenericRecordRequest request, GenericRecord genericRecord) {
        publish(request, genericRecord);
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
        float result = fabricNode.getCount() == fabricNode.getTotalCount() ? 1.0f : 1.0f * fabricNode.getCount()
                / fabricNode.getTotalCount();
        status.setProgress(result);
    }

    private GenericFabricStatusEnum getStatusEnum(String batchId, GenericFabricNode fabricNode) {

        if (StringUtils.isNotBlank(fabricNode.getMessage())) {
            log.error("Batch failed for batch Id=" + batchId + " error=" + fabricNode.getMessage());
            return GenericFabricStatusEnum.ERROR;
        }
        if (fabricNode.getTotalCount() <= -1) {
            return GenericFabricStatusEnum.PROCESSING;
        }
        if (fabricNode.getCount() >= fabricNode.getTotalCount()) {
            return GenericFabricStatusEnum.FINISHED;
        }
        if (fabricNode.getCount() < fabricNode.getTotalCount()) {
            return GenericFabricStatusEnum.PROCESSING;
        }
        return GenericFabricStatusEnum.UNKNOWN;
    }

    @Override
    public void updateBatchCount(String batchId, long delta) {
        messageService.incrementalUpdate(batchId, (origData) -> {
            try {
                if (StringUtils.isBlank(origData)) {
                    GenericFabricNode node = new GenericFabricNode();
                    node.setCount(delta);
                    return JsonUtils.serialize(node);
                }
                if (delta <= 0) {
                    return origData;
                }
                GenericFabricNode node = JsonUtils.deserialize(origData, GenericFabricNode.class);
                if (node.getCount() + delta < Long.MAX_VALUE) {
                    node.setCount(node.getCount() + delta);
                }
                return JsonUtils.serialize(node);
            } catch (Exception ex) {
                log.warn("Failed to update, batchIdName=" + batchId + " delta=" + delta);
                return origData;
            }
        });
    }

    @Override
    public boolean cleanup(String batchId) {
        return messageService.cleanup(batchId);
    }

}
