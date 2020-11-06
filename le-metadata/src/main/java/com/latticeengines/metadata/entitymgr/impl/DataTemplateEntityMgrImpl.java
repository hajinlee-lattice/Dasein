package com.latticeengines.metadata.entitymgr.impl;

import java.util.UUID;

import javax.inject.Inject;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import com.latticeengines.db.exposed.repository.BaseJpaRepository;
import com.latticeengines.documentdb.entity.DataTemplateEntity;
import com.latticeengines.documentdb.entitymgr.impl.BaseDocumentEntityMgrImpl;
import com.latticeengines.domain.exposed.metadata.datastore.DataTemplate;
import com.latticeengines.metadata.entitymgr.DataTemplateEntityMgr;
import com.latticeengines.metadata.repository.document.reader.DataTemplateReaderRepository;
import com.latticeengines.metadata.repository.document.writer.DataTemplateWriterRepository;

@Service("dataTemplateEntityMgr")
public class DataTemplateEntityMgrImpl extends BaseDocumentEntityMgrImpl<DataTemplateEntity> implements DataTemplateEntityMgr {

    private static final Logger log = LoggerFactory.getLogger(DataTemplateEntityMgrImpl.class);

    @Inject
    private DataTemplateWriterRepository repository;

    @Inject
    private DataTemplateReaderRepository readerRepository;

    @Override
    public BaseJpaRepository<DataTemplateEntity, String> getRepository() {
        return repository;
    }

    @Override
    public String create(String tenantId, DataTemplate dataTemplate) {
        DataTemplateEntity newEntity = new DataTemplateEntity();
        newEntity.setTenantId(tenantId);
        newEntity.setUuid(UUID.randomUUID().toString());
        newEntity.setDocument(dataTemplate);
        DataTemplateEntity saved = repository.save(newEntity);
        return saved.getUuid();
    }

    @Override
    public DataTemplate findByUuid(String tenantId, String uuid) {
        DataTemplateEntity dataTemplateEntity = readerRepository.findByTenantIdAndUuid(tenantId, uuid);
        return dataTemplateEntity.getDocument();
    }

    @Override
    public void updateByUuid(String tenantId, String uuid, DataTemplate dataTemplate) {
        DataTemplateEntity existing = repository.findByTenantIdAndUuid(tenantId, uuid);
        if (existing != null) {
            existing.setDocument(dataTemplate);
            repository.save(existing);
        }
    }

    @Override
    public void deleteByUuid(String tenantId, String uuid) {
        DataTemplateEntity existing = repository.findByTenantIdAndUuid(tenantId, uuid);
        if (existing != null) {
            repository.delete(existing);
        }
    }
}
