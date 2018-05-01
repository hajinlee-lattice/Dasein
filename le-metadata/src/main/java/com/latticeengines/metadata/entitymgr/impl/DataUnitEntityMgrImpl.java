package com.latticeengines.metadata.entitymgr.impl;

import java.util.List;
import java.util.UUID;

import javax.inject.Inject;

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
    public DataUnit createOrUpdateByName(String tenantId, DataUnit dataUnit) {
        String name = dataUnit.getName();
        if (StringUtils.isBlank(name)) {
            name = NamingUtils.timestamp(dataUnit.getStorageType().name());
            dataUnit.setName(name);
        }
        DataUnitEntity existing = repository.findByTenantIdAndName(tenantId, name);
        if (existing == null) {
            return createNewDataUnit(tenantId, dataUnit);
        } else {
            return updateExistingDataUnit(dataUnit, existing);
        }
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
    public DataUnit findByNameFromReader(String tenantId, String name) {
        DataUnitEntity entity = readerRepository.findByTenantIdAndName(tenantId, name);
        if (entity == null) {
            return null;
        } else {
            return entity.getDocument();
        }
    }

    @Override
    public void deleteByName(String tenantId, String name) {
        DataUnitEntity existing = repository.findByTenantIdAndName(tenantId, name);
        if (existing != null) {
            repository.delete(existing);
        }
    }

    @Override
    public void cleanupTenant(String tenantId) {
        List<DataUnitEntity> entities = repository.removeByTenantId(tenantId);
        log.info("Deleted " + entities.size() + " documents from " + tenantId);
    }

}
