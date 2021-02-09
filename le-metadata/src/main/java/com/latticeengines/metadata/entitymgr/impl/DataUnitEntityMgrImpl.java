package com.latticeengines.metadata.entitymgr.impl;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.stream.Collectors;

import javax.inject.Inject;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.domain.PageRequest;
import org.springframework.stereotype.Service;

import com.latticeengines.common.exposed.util.NamingUtils;
import com.latticeengines.common.exposed.util.ThreadPoolUtils;
import com.latticeengines.db.exposed.repository.BaseJpaRepository;
import com.latticeengines.documentdb.entity.DataUnitEntity;
import com.latticeengines.documentdb.entitymgr.impl.BaseDocumentEntityMgrImpl;
import com.latticeengines.domain.exposed.metadata.datastore.DataUnit;
import com.latticeengines.domain.exposed.metadata.datastore.DynamoDataUnit;
import com.latticeengines.domain.exposed.metadata.datastore.RedshiftDataUnit;
import com.latticeengines.domain.exposed.metadata.datastore.S3DataUnit;
import com.latticeengines.domain.exposed.metadata.retention.RetentionPolicyTimeUnit;
import com.latticeengines.domain.exposed.util.RetentionPolicyUtil;
import com.latticeengines.metadata.entitymgr.DataUnitEntityMgr;
import com.latticeengines.metadata.repository.document.reader.DataUnitCrossTenantReaderRepository;
import com.latticeengines.metadata.repository.document.reader.DataUnitReaderRepository;
import com.latticeengines.metadata.repository.document.writer.DataUnitWriterRepository;
import com.latticeengines.redshiftdb.exposed.service.RedshiftPartitionService;

@Service("dataUnitEntityMgr")
public class DataUnitEntityMgrImpl extends BaseDocumentEntityMgrImpl<DataUnitEntity> implements DataUnitEntityMgr {

    private static final Logger log = LoggerFactory.getLogger(DataUnitEntityMgrImpl.class);

    @Inject
    private DataUnitWriterRepository repository;

    @Inject
    private DataUnitReaderRepository readerRepository;

    @Inject
    private DataUnitCrossTenantReaderRepository dataUnitCrossTenantReaderRepository;

    @Inject
    private RedshiftPartitionService redshiftPartitionService;

    @Override
    public BaseJpaRepository<DataUnitEntity, String> getRepository() {
        return repository;
    }

    private ExecutorService service = ThreadPoolUtils.getFixedSizeThreadPool("dataunit-mgr", ThreadPoolUtils.NUM_CORES * 2);

    private int purgeSize = 3;

    @Override
    public DataUnit createOrUpdateByNameAndStorageType(String tenantId, DataUnit dataUnit) {
        String name = dataUnit.getName();
        dataUnit.setTenant(tenantId);
        DataUnit.StorageType storageType = dataUnit.getStorageType();
        if (StringUtils.isBlank(name)) {
            name = NamingUtils.timestamp(dataUnit.getStorageType().name());
            dataUnit.setName(name);
        }
        DataUnitEntity existing = repository.findByTenantIdAndNameAndStorageType(tenantId, name, storageType);
        if (existing == null) {
            dataUnit = createNewDataUnit(tenantId, dataUnit);
            return dataUnit;
        } else {
            return updateExistingDataUnit(dataUnit, existing);
        }
    }

    private void purgeSnapShot(DataUnitEntity dataUnitEntity) {
        DataUnit dataUnit = dataUnitEntity.getDocument();
        List<DataUnit.Role> roles = dataUnit.getRoles();
        if (CollectionUtils.isNotEmpty(roles) && roles.contains(DataUnit.Role.Master) && StringUtils.isNotEmpty(dataUnit.getDataTemplateId())) {
            List<DataUnitEntity> dataUnitEntities = readerRepository.findByTenantIdAndDataTemplateId(dataUnit.getTenant(), dataUnit.getDataTemplateId());
            Runnable runnable = () -> {
                if (CollectionUtils.isNotEmpty(dataUnitEntities)) {
                    Map<String, DataUnitEntity> entitiesToSave = new HashMap<>();
                    List<DataUnitEntity> snapEntities = new ArrayList<>();
                    for (DataUnitEntity entity : dataUnitEntities) {
                        List<DataUnit.Role> dataUnitRoles = entity.getDocument().getRoles();
                        if (CollectionUtils.isNotEmpty(dataUnitRoles)) {
                            if (dataUnitRoles.contains(DataUnit.Role.Master) && !entity.getUuid().equals(dataUnitEntity.getUuid())
                                    && entity.getCreatedDate().getTime() <= dataUnitEntity.getCreatedDate().getTime()) {
                                dataUnitRoles.remove(DataUnit.Role.Master);
                                entitiesToSave.putIfAbsent(entity.getUuid(), entity);
                            }
                            if (dataUnitRoles.contains(DataUnit.Role.Snapshot)
                                    && RetentionPolicyUtil.getExpireTimeByRetentionPolicyStr(entity.getDocument().getRetentionPolicy()) == -1) {
                                snapEntities.add(entity);
                            }
                        }
                    }
                    if (snapEntities.size() > purgeSize) {
                        snapEntities.sort(Comparator.comparing(DataUnitEntity::getCreatedDate));
                        List<DataUnitEntity> entitiesToPurge = snapEntities.subList(0, snapEntities.size() - purgeSize);
                        for (DataUnitEntity entity : entitiesToPurge) {
                            entity.getDocument().setRetentionPolicy(RetentionPolicyUtil.toRetentionPolicyStr(7, RetentionPolicyTimeUnit.DAY));
                            entitiesToSave.putIfAbsent(entity.getUuid(), entity);
                        }
                    }
                    repository.saveAll(entitiesToSave.values());
                }
            };
            service.submit(runnable);
        }
    }

    @Override
    public void updateSignature(String tenantId, DataUnit dataUnit, String signature) {
        if (DataUnit.StorageType.Dynamo.equals(dataUnit.getStorageType())) {
            DataUnitEntity existing = repository.findByTenantIdAndNameAndStorageType(tenantId, dataUnit.getName(), dataUnit.getStorageType());
            if (existing != null) {
                DataUnit existingDataUnit = existing.getDocument();
                if (existingDataUnit != null) {
                    ((DynamoDataUnit) existingDataUnit).setSignature(signature);
                    repository.save(existing);
                }
            }
        }
    }

    @Override
    public List<DataUnit> findAll(String tenantId) {
        List<DataUnitEntity> entities = readerRepository.findByTenantId(tenantId);
        return convertList(entities, false);
    }

    @Override
    public List<DataUnit> findByStorageType(DataUnit.StorageType storageType) {
        List<DataUnitEntity> entities = dataUnitCrossTenantReaderRepository.findByStorageTypeOrderByTenantId(storageType);
        return convertList(entities, false);
    }

    private boolean reformatS3DataUnit(DataUnit dataUnit) {
        if (DataUnit.StorageType.S3.equals(dataUnit.getStorageType())) {
            S3DataUnit s3DataUnit = (S3DataUnit) dataUnit;
            if (StringUtils.isNotEmpty(s3DataUnit.getLinkedDir())) {
                s3DataUnit.fixBucketAndPrefix();
                if (StringUtils.isNotEmpty(s3DataUnit.getBucket())) {
                    s3DataUnit.setLinkedDir(null);
                    log.info(String.format("S3 data unit will be saved with bucket name %s and prefix %s.",
                            s3DataUnit.getBucket(), s3DataUnit.getPrefix()));
                    return true;
                }
            }
        }
        return false;
    }

    private void asyncReformatS3DataUnit(List<DataUnitEntity> dataUnitEntities) {
        List<DataUnitEntity> formattedS3DataUnitEntities = new ArrayList<>();
        Runnable runnable = () -> {
            dataUnitEntities.forEach(dataUnitEntity -> {
                boolean reformat = false;
                if (dataUnitEntity.getDocument() != null) {
                    reformat = reformatS3DataUnit(dataUnitEntity.getDocument());
                }
                if (reformat) {
                    formattedS3DataUnitEntities.add(dataUnitEntity);
                }

            });
            if (CollectionUtils.isNotEmpty(formattedS3DataUnitEntities)) {
                repository.saveAll(formattedS3DataUnitEntities);
            }
        };
        service.submit(runnable);
    }

    private DataUnit createNewDataUnit(String tenantId, DataUnit dataUnit) {
        DataUnitEntity newEntity = new DataUnitEntity();
        newEntity.setTenantId(tenantId);
        newEntity.setUuid(UUID.randomUUID().toString());
        reformatS3DataUnit(dataUnit);
        newEntity.setDocument(dataUnit);
        DataUnitEntity saved = repository.save(newEntity);
        purgeSnapShot(saved);
        return convertEntity(saved);
    }

    private DataUnit updateExistingDataUnit(DataUnit toUpdate, DataUnitEntity existing) {
        reformatS3DataUnit(toUpdate);
        existing.setDocument(toUpdate);
        DataUnitEntity saved = repository.save(existing);
        return convertEntity(saved);
    }

    @Override
    public List<DataUnit> findByNameFromReader(String tenantId, String name) {
        List<DataUnitEntity> entities = readerRepository.findByTenantIdAndName(tenantId, name);
        return convertList(entities, true);
    }

    @Override
    public List<DataUnit> findAllByTypeFromReader(String tenantId, DataUnit.StorageType storageType) {
        List<DataUnitEntity> entities = readerRepository.findByTenantIdAndStorageType(tenantId, storageType);
        return convertList(entities, true);
    }

    @Override
    public List<DataUnit> findAllByDataTemplateIdAndRoleFromReader(String tenantId, String dataTemplateId, DataUnit.Role role) {
        List<DataUnitEntity> entities = readerRepository.findByTenantIdAndDataTemplateId(tenantId, dataTemplateId);
        List<DataUnitEntity> entitiesWithRole = entities.stream().filter(entity -> entity.getDocument().getRoles().contains(role))
                .collect(Collectors.toList());
        return convertList(entitiesWithRole, false);
    }

    @Override
    public DataUnit findByDataTemplateIdAndRoleFromReader(String tenantId, String dataTemplateId, DataUnit.Role role) {
        List<DataUnitEntity> entities = readerRepository.findByTenantIdAndDataTemplateId(tenantId, dataTemplateId);
        List<DataUnitEntity> entitiesWithRole = entities.stream().filter(entity -> entity.getDocument().getRoles().contains(role))
                .collect(Collectors.toList());
        entitiesWithRole.sort(Comparator.comparing(DataUnitEntity::getCreatedDate).reversed());
        if (CollectionUtils.isNotEmpty(entitiesWithRole)) {
            return convertEntity(entitiesWithRole.get(0));
        } else {
            return null;
        }
    }

    @Override
    public List<DataUnit> findAllDataUnitEntitiesWithExpiredRetentionPolicy(int pageIndex, int pageSize) {
        List<DataUnitEntity> entities = readerRepository.findAllDataUnitEntitiesWithExpiredRetentionPolicy(
                RetentionPolicyUtil.NEVER_EXPIRE_POLICY, PageRequest.of(pageIndex, pageSize));
        return convertList(entities, false);
    }

    @Override
    public DataUnit findByNameTypeFromReader(String tenantId, String name, DataUnit.StorageType storageType) {
        DataUnitEntity entity = readerRepository.findByTenantIdAndNameAndStorageType(tenantId, name, storageType);
        if (entity != null) {
            asyncReformatS3DataUnit(Collections.singletonList(entity));
            return convertEntity(entity);
        } else {
            return null;
        }
    }

    @Override
    public void deleteByName(String tenantId, String name, DataUnit.StorageType storageType) {
        DataUnitEntity existing = repository.findByTenantIdAndNameAndStorageType(tenantId, name, storageType);
        if (existing != null) {
            repository.delete(existing);
        }
    }

    @Override
    public void cleanupTenant(String tenantId) {
        List<DataUnitEntity> entities = repository.removeByTenantId(tenantId);
        log.info("Deleted " + entities.size() + " documents from " + tenantId);
    }

    @Override
    public List<DataUnit> deleteAllByName(String tenantId, String name) {
        List<DataUnitEntity> entities = repository.findByTenantIdAndName(tenantId, name);
        List<DataUnit> units = convertList(entities, false);
        repository.deleteAll(entities);
        return units;
    }

    @Override
    public DataUnit renameTableName(String tenantId, DataUnit dataUnit, String tableName) {
        DataUnitEntity existing = repository.findByTenantIdAndNameAndStorageType(tenantId, dataUnit.getName(),
                dataUnit.getStorageType());
        if (existing != null) {
            dataUnit.setName(tableName);
            reformatS3DataUnit(dataUnit);
            existing.setDocument(dataUnit);
            DataUnitEntity saved = repository.save(existing);
            return convertEntity(saved);
        }
        return null;
    }

    private DataUnit convertEntity(DataUnitEntity entity) {
        DataUnit dataUnit = entity.getDocument();
        if (dataUnit instanceof RedshiftDataUnit) {
            setRedshiftPartition((RedshiftDataUnit) dataUnit);
        }
        dataUnit.setUpdated(entity.getLastModifiedDate());
        return dataUnit;
    }

    private List<DataUnit> convertList(List<DataUnitEntity> entities, boolean reformat) {
        List<DataUnit> units = new ArrayList<>();
        if (CollectionUtils.isNotEmpty(entities)) {
            if (reformat) {
                asyncReformatS3DataUnit(entities);
            }
            entities.forEach(dataUnitEntity -> units.add(convertEntity(dataUnitEntity)));
        }
        return units;
    }

    private void setRedshiftPartition(RedshiftDataUnit dataUnit) {
        if (StringUtils.isBlank(dataUnit.getClusterPartition())) {
            log.warn("Redshift data unit {} in {} is still in the legacy partition.",
                    dataUnit.getName(), dataUnit.getTenant());
            dataUnit.setClusterPartition(redshiftPartitionService.getLegacyPartition());
        }
    }

}
