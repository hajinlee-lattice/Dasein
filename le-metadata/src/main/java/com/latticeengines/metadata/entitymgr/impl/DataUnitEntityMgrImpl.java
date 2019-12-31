package com.latticeengines.metadata.entitymgr.impl;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ExecutorService;

import javax.inject.Inject;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import com.latticeengines.common.exposed.util.NamingUtils;
import com.latticeengines.common.exposed.util.ThreadPoolUtils;
import com.latticeengines.db.exposed.repository.BaseJpaRepository;
import com.latticeengines.documentdb.entity.DataUnitEntity;
import com.latticeengines.documentdb.entitymgr.impl.BaseDocumentEntityMgrImpl;
import com.latticeengines.domain.exposed.metadata.datastore.DataUnit;
import com.latticeengines.domain.exposed.metadata.datastore.S3DataUnit;
import com.latticeengines.metadata.entitymgr.DataUnitEntityMgr;
import com.latticeengines.metadata.repository.document.reader.DataUnitReaderRepository;
import com.latticeengines.metadata.repository.document.writer.DataUnitWriterRepository;

@Service("dataUnitEntityMgr")
public class DataUnitEntityMgrImpl extends BaseDocumentEntityMgrImpl<DataUnitEntity> implements DataUnitEntityMgr {

    private static final Logger log = LoggerFactory.getLogger(DataUnitEntityMgrImpl.class);

    @Inject
    private DataUnitWriterRepository repository;

    @Inject
    private DataUnitReaderRepository readerRepository;

    @Override
    public BaseJpaRepository<DataUnitEntity, String> getRepository() {
        return repository;
    }

    private static final int NUM_THREADS = 8;

    private ExecutorService service = ThreadPoolUtils.getFixedSizeThreadPool("dataunit-mgr", NUM_THREADS);

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
            return createNewDataUnit(tenantId, dataUnit);
        } else {
            return updateExistingDataUnit(dataUnit, existing);
        }
    }

    @Override
    public List<DataUnit> findAll(String tenantId) {
        List<DataUnitEntity> entities = readerRepository.findByTenantId(tenantId);
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

    private void asycReformatS3DataUnit(List<DataUnitEntity> dataUnitEntities) {
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
        return saved.getDocument();
    }

    private DataUnit updateExistingDataUnit(DataUnit toUpdate, DataUnitEntity existing) {
        reformatS3DataUnit(toUpdate);
        existing.setDocument(toUpdate);
        DataUnitEntity saved = repository.save(existing);
        return saved.getDocument();
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
    public DataUnit findByNameTypeFromReader(String tenantId, String name, DataUnit.StorageType storageType) {
        DataUnitEntity entity = readerRepository.findByTenantIdAndNameAndStorageType(tenantId, name, storageType);
        if (entity != null) {
            asycReformatS3DataUnit(Collections.singletonList(entity));
            return entity.getDocument();
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
            return saved.getDocument();
        }
        return null;
    }

    private List<DataUnit> convertList(List<DataUnitEntity> entities, boolean reformat) {
        List<DataUnit> units = new ArrayList<>();
        if (CollectionUtils.isNotEmpty(entities)) {
            if (reformat) {
                asycReformatS3DataUnit(entities);
            }
            entities.forEach(dataUnitEntity -> units.add(dataUnitEntity.getDocument()));
        }
        return units;
    }

}
