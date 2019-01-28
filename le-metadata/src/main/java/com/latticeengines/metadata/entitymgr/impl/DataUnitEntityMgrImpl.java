package com.latticeengines.metadata.entitymgr.impl;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import javax.inject.Inject;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import com.latticeengines.common.exposed.util.NamingUtils;
import com.latticeengines.db.exposed.repository.BaseJpaRepository;
import com.latticeengines.documentdb.entity.DataUnitEntity;
import com.latticeengines.documentdb.entitymgr.impl.BaseDocumentEntityMgrImpl;
import com.latticeengines.domain.exposed.metadata.datastore.DataUnit;
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
        return convertList(entities);
    }

    private DataUnit createNewDataUnit(String tenantId, DataUnit dataUnit) {
        DataUnitEntity newEntity = new DataUnitEntity();
        newEntity.setTenantId(tenantId);
        newEntity.setUuid(UUID.randomUUID().toString());
        newEntity.setDocument(dataUnit);
        DataUnitEntity saved = repository.save(newEntity);
        return saved.getDocument();
    }

    private DataUnit updateExistingDataUnit(DataUnit toUpdate, DataUnitEntity existing) {
        existing.setDocument(toUpdate);
        DataUnitEntity saved = repository.save(existing);
        return saved.getDocument();
    }

    @Override
    public List<DataUnit> findByNameFromReader(String tenantId, String name) {
        List<DataUnitEntity> entities = readerRepository.findByTenantIdAndName(tenantId, name);
        return convertList(entities);
    }

    @Override
    public List<DataUnit> findAllByTypeFromReader(String tenantId, DataUnit.StorageType storageType) {
        List<DataUnitEntity> entities = readerRepository.findByTenantIdAndStorageType(tenantId, storageType);
        return convertList(entities);
    }

    @Override
    public DataUnit findByNameTypeFromReader(String tenantId, String name, DataUnit.StorageType storageType) {
        DataUnitEntity entity = readerRepository.findByTenantIdAndNameAndStorageType(tenantId, name, storageType);
        if (entity != null) {
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
    public List<DataUnit> deleteAllByName(String name) {
        List<DataUnitEntity> entities = repository.findByName(name);
        List<DataUnit> units = convertList(entities);
        repository.deleteAll(entities);
        return units;
    }

    @Override
    public DataUnit renameTableName(String tenantId, DataUnit dataUnit, String tableName) {
        DataUnitEntity existing = repository.findByTenantIdAndNameAndStorageType(tenantId, dataUnit.getName(),
                dataUnit.getStorageType());
        if (existing != null) {
            dataUnit.setName(tableName);
            existing.setDocument(dataUnit);
            DataUnitEntity saved = repository.save(existing);
            return saved.getDocument();
        }
        return null;
    }

    private List<DataUnit> convertList(List<DataUnitEntity> entities) {
        List<DataUnit> units = new ArrayList<>();
        if (CollectionUtils.isNotEmpty(entities)) {
            entities.forEach(dataUnitEntity -> units.add(dataUnitEntity.getDocument()));
        }
        return units;
    }

}
